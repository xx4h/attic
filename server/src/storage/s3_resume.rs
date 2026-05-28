//! Resume support for interrupted S3 download streams.
//!
//! Once atticd has started streaming a NAR body to a nix client, the
//! HTTP response status and Content-Length have already been written
//! — there is no way to surface a mid-stream failure as a proper
//! error. The client just sees a truncated body and (silently) caches
//! a broken NAR.
//!
//! [`ResumableS3Read`] wraps the S3 body stream in an [`AsyncRead`]
//! that, on upstream interruption, transparently re-issues the
//! `GetObject` with a `Range` header to continue from where it left
//! off. The downstream connection stays open the whole time.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::Client;
use tokio::io::{AsyncRead, ReadBuf};

use super::s3::StreamResumeConfig;

type IoResult<T> = io::Result<T>;
type BoxedRead = Box<dyn AsyncRead + Unpin + Send>;
type ReconnectFuture = Pin<Box<dyn Future<Output = IoResult<BoxedRead>> + Send>>;

/// Strategy for reopening a download from a given byte offset.
///
/// The production implementation issues an S3 `GetObject` with a
/// `Range` header (and an `If-Match` ETag guard). Tests inject a
/// fake implementation that yields canned byte streams or errors.
type Reconnect = Box<dyn Fn(u64) -> ReconnectFuture + Send>;

pub(super) struct ResumableS3Read {
    reconnect: Reconnect,
    /// Stored only for log/trace labels.
    key: String,
    /// Total size from the first response. `None` means we can't
    /// detect premature EOF (resume still works on hard errors).
    total_size: Option<u64>,

    inner: BoxedRead,
    bytes_read: u64,
    retries_remaining: u8,
    next_backoff: Duration,
    max_backoff: Duration,

    state: State,
}

enum State {
    Reading,
    Reconnecting(ReconnectFuture),
    Dead { kind: io::ErrorKind, msg: String },
}

impl ResumableS3Read {
    pub(super) fn from_first_response(
        client: Client,
        bucket: String,
        key: String,
        first: GetObjectOutput,
        config: StreamResumeConfig,
    ) -> Self {
        let etag = first.e_tag().map(|s| s.to_string());
        let total_size = first.content_length().and_then(|n| u64::try_from(n).ok());

        if etag.is_none() {
            tracing::warn!(
                bucket = %bucket,
                key = %key,
                "S3 response has no ETag; resumed Range requests will not be guarded against object replacement"
            );
        }

        let inner: BoxedRead = Box::new(first.body.into_async_read());

        let reconnect = make_s3_reconnect(client, bucket, key.clone(), etag);

        Self::from_parts(reconnect, inner, key, total_size, config)
    }

    fn from_parts(
        reconnect: Reconnect,
        inner: BoxedRead,
        key: String,
        total_size: Option<u64>,
        config: StreamResumeConfig,
    ) -> Self {
        Self {
            reconnect,
            key,
            total_size,
            inner,
            bytes_read: 0,
            retries_remaining: config.max_retries,
            next_backoff: Duration::from_millis(config.initial_backoff_ms),
            max_backoff: Duration::from_millis(config.max_backoff_ms),
            state: State::Reading,
        }
    }

    fn start_reconnect(&mut self) {
        self.retries_remaining = self.retries_remaining.saturating_sub(1);

        let delay = self.next_backoff;
        self.next_backoff = (self.next_backoff * 2).min(self.max_backoff);

        let key = self.key.clone();
        let bytes_read = self.bytes_read;
        let reconnect_fut = (self.reconnect)(self.bytes_read);

        let fut: ReconnectFuture = Box::pin(async move {
            tokio::time::sleep(delay).await;
            tracing::info!(
                %key,
                bytes_read,
                "Resuming S3 download with Range request"
            );
            reconnect_fut.await
        });

        self.state = State::Reconnecting(fut);
    }

    fn enter_dead(&mut self, e: &io::Error) {
        self.state = State::Dead {
            kind: e.kind(),
            msg: e.to_string(),
        };
    }
}

fn make_s3_reconnect(
    client: Client,
    bucket: String,
    key: String,
    etag: Option<String>,
) -> Reconnect {
    Box::new(move |range_start: u64| {
        let client = client.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let etag = etag.clone();
        Box::pin(async move {
            let range = format!("bytes={range_start}-");
            let mut req = client.get_object().bucket(&bucket).key(&key).range(&range);
            if let Some(t) = &etag {
                req = req.if_match(t);
            }
            let output = req.send().await.map_err(sdk_err_to_io)?;
            let new_inner: BoxedRead = Box::new(output.body.into_async_read());
            Ok(new_inner)
        })
    })
}

impl AsyncRead for ResumableS3Read {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        loop {
            // Drive an in-flight reconnect to completion before reading.
            if let State::Reconnecting(fut) = &mut self.state {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(new_inner)) => {
                        self.inner = new_inner;
                        self.state = State::Reading;
                    }
                    Poll::Ready(Err(e)) => {
                        if self.retries_remaining > 0 && is_retryable(&e) {
                            tracing::warn!(
                                key = %self.key,
                                bytes_read = self.bytes_read,
                                error = %e,
                                "S3 resume attempt failed; retrying"
                            );
                            self.start_reconnect();
                            continue;
                        }
                        self.enter_dead(&e);
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            match &self.state {
                State::Dead { kind, msg } => {
                    return Poll::Ready(Err(io::Error::new(*kind, msg.clone())));
                }
                State::Reading => {}
                State::Reconnecting(_) => unreachable!("handled above"),
            }

            let before = buf.filled().len();
            match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len() - before;
                    self.bytes_read += read as u64;

                    let premature_eof =
                        read == 0 && self.total_size.is_some_and(|total| self.bytes_read < total);

                    if premature_eof && self.retries_remaining > 0 {
                        tracing::warn!(
                            key = %self.key,
                            bytes_read = self.bytes_read,
                            total = ?self.total_size,
                            "S3 stream ended prematurely; resuming"
                        );
                        self.start_reconnect();
                        continue;
                    }

                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => {
                    if self.retries_remaining > 0 && is_retryable(&e) {
                        tracing::warn!(
                            key = %self.key,
                            bytes_read = self.bytes_read,
                            error = %e,
                            "S3 stream interrupted; resuming with Range"
                        );
                        self.start_reconnect();
                        continue;
                    }
                    self.enter_dead(&e);
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn is_retryable(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    match e.kind() {
        ConnectionReset | ConnectionAborted | BrokenPipe | TimedOut | UnexpectedEof
        | Interrupted => true,

        // FIXME: This retries ALL Other-kind io::Errors, including cases
        // we probably shouldn't retry (e.g. malformed body, decoder
        // errors). The AWS Rust SDK flattens body-stream errors into
        // io::Error::new(Other, ...) without exposing typed variants,
        // so precise classification would require walking the source
        // chain and string-matching hyper/h2 messages — brittle across
        // SDK upgrades. Revisit when the SDK exposes typed body errors
        // or telemetry shows what we're actually retrying.
        Other => true,

        _ => false,
    }
}

/// Convert an SDK error from the reconnect `GetObject` into an
/// [`io::Error`] whose kind drives the retry decision via
/// [`is_retryable`].
///
/// 412 PreconditionFailed (our `If-Match` ETag guard) becomes
/// `InvalidData` — non-retryable, since the object has been replaced
/// in S3 and resuming would splice bytes from two different objects.
/// Other 4xx (except 408/429) become `PermissionDenied` — also
/// non-retryable. Everything else becomes `Other` and is retried.
fn sdk_err_to_io(err: SdkError<GetObjectError>) -> io::Error {
    use io::ErrorKind;

    if let SdkError::ServiceError(svc) = &err {
        let status = svc.raw().status().as_u16();
        if status == 412 {
            return io::Error::new(
                ErrorKind::InvalidData,
                format!("S3 If-Match precondition failed (object changed): {err}"),
            );
        }
        if (400..500).contains(&status) && status != 408 && status != 429 {
            return io::Error::new(
                ErrorKind::PermissionDenied,
                format!("S3 fatal error during resume: {err}"),
            );
        }
    }

    io::Error::other(format!("S3 transient error during resume: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncReadExt;

    /// Per-poll behavior for the fake inner stream.
    enum MockChunk {
        Data(Vec<u8>),
        Error(io::ErrorKind),
        // Absence of further chunks = clean EOF.
    }

    struct MockReader {
        chunks: VecDeque<MockChunk>,
    }

    impl AsyncRead for MockReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.chunks.pop_front() {
                Some(MockChunk::Data(d)) => {
                    let n = d.len().min(buf.remaining());
                    buf.put_slice(&d[..n]);
                    // If the caller's buffer was smaller, push the
                    // unread tail back so the next poll picks it up.
                    if n < d.len() {
                        self.chunks.push_front(MockChunk::Data(d[n..].to_vec()));
                    }
                    Poll::Ready(Ok(()))
                }
                Some(MockChunk::Error(kind)) => {
                    Poll::Ready(Err(io::Error::new(kind, "mock stream error")))
                }
                None => Poll::Ready(Ok(())), // clean EOF
            }
        }
    }

    /// Per-reconnect-call response: either a fresh canned stream or
    /// a failure of the `GetObject` itself.
    enum ReconnectResponse {
        Success(Vec<MockChunk>),
        Failure(io::ErrorKind),
    }

    fn fake_reconnect(responses: Vec<ReconnectResponse>) -> (Reconnect, Arc<Mutex<Vec<u64>>>) {
        let queue = Arc::new(Mutex::new(VecDeque::from(responses)));
        let calls = Arc::new(Mutex::new(Vec::new()));

        let queue_for_closure = queue.clone();
        let calls_for_closure = calls.clone();

        let reconnect: Reconnect = Box::new(move |range_start: u64| {
            calls_for_closure.lock().unwrap().push(range_start);
            let next = queue_for_closure.lock().unwrap().pop_front();
            Box::pin(async move {
                match next {
                    Some(ReconnectResponse::Success(chunks)) => {
                        let reader = MockReader {
                            chunks: chunks.into_iter().collect(),
                        };
                        Ok(Box::new(reader) as BoxedRead)
                    }
                    Some(ReconnectResponse::Failure(kind)) => {
                        Err(io::Error::new(kind, "mock reconnect failure"))
                    }
                    None => Err(io::Error::other("no more mock responses queued")),
                }
            })
        });

        (reconnect, calls)
    }

    fn cfg(max_retries: u8) -> StreamResumeConfig {
        StreamResumeConfig {
            max_retries,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        }
    }

    fn boxed_mock(chunks: Vec<MockChunk>) -> BoxedRead {
        Box::new(MockReader {
            chunks: chunks.into_iter().collect(),
        })
    }

    #[tokio::test]
    async fn happy_path_no_resume() {
        let (reconnect, calls) = fake_reconnect(vec![]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Data(b"world".to_vec()),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        wrapper.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
        assert!(calls.lock().unwrap().is_empty(), "no reconnects expected");
    }

    #[tokio::test]
    async fn mid_stream_error_triggers_resume() {
        let (reconnect, calls) =
            fake_reconnect(vec![ReconnectResponse::Success(vec![MockChunk::Data(
                b"world".to_vec(),
            )])]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Error(io::ErrorKind::ConnectionReset),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        wrapper.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
        assert_eq!(
            *calls.lock().unwrap(),
            vec![6],
            "reconnect should resume from byte 6"
        );
    }

    #[tokio::test]
    async fn premature_eof_triggers_resume() {
        // Inner returns 6 bytes then clean EOF; total_size says 11.
        let (reconnect, calls) =
            fake_reconnect(vec![ReconnectResponse::Success(vec![MockChunk::Data(
                b"world".to_vec(),
            )])]);
        let inner = boxed_mock(vec![MockChunk::Data(b"hello ".to_vec())]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        wrapper.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
        assert_eq!(*calls.lock().unwrap(), vec![6]);
    }

    #[tokio::test]
    async fn other_kind_error_is_retried() {
        // Whatever the SDK shoves into `Other` should be retried.
        let (reconnect, calls) =
            fake_reconnect(vec![ReconnectResponse::Success(vec![MockChunk::Data(
                b"world".to_vec(),
            )])]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Error(io::ErrorKind::Other),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        wrapper.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
        assert_eq!(*calls.lock().unwrap(), vec![6]);
    }

    #[tokio::test]
    async fn max_retries_zero_disables_resume() {
        let (reconnect, calls) = fake_reconnect(vec![]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Error(io::ErrorKind::ConnectionReset),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(0));

        let mut out = Vec::new();
        let err = wrapper.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(out, b"hello ");
        assert!(
            calls.lock().unwrap().is_empty(),
            "no reconnects when disabled"
        );
    }

    #[tokio::test]
    async fn non_retryable_error_propagates() {
        let (reconnect, calls) = fake_reconnect(vec![]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Error(io::ErrorKind::PermissionDenied),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        let err = wrapper.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            calls.lock().unwrap().is_empty(),
            "PermissionDenied should not trigger reconnect"
        );
    }

    #[tokio::test]
    async fn retry_budget_exhausted_propagates_last_error() {
        // Inner errors immediately. Two reconnect attempts both yield
        // failing inner streams. After max_retries=2, the third error
        // surfaces.
        let (reconnect, calls) = fake_reconnect(vec![
            ReconnectResponse::Success(vec![MockChunk::Error(io::ErrorKind::ConnectionReset)]),
            ReconnectResponse::Success(vec![MockChunk::Error(io::ErrorKind::ConnectionReset)]),
        ]);
        let inner = boxed_mock(vec![MockChunk::Error(io::ErrorKind::ConnectionReset)]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(100), cfg(2));

        let mut buf = [0u8; 16];
        let err = wrapper.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "should exhaust both retries"
        );
    }

    #[tokio::test]
    async fn reconnect_failure_then_success() {
        // First reconnect call fails with retryable error; second
        // succeeds. Wrapper should keep going.
        let (reconnect, calls) = fake_reconnect(vec![
            ReconnectResponse::Failure(io::ErrorKind::TimedOut),
            ReconnectResponse::Success(vec![MockChunk::Data(b"world".to_vec())]),
        ]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Error(io::ErrorKind::ConnectionReset),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        wrapper.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
        // Both calls resume from byte 6.
        assert_eq!(*calls.lock().unwrap(), vec![6, 6]);
    }

    #[tokio::test]
    async fn reconnect_non_retryable_failure_propagates() {
        // Mimics a 412 If-Match failure that sdk_err_to_io would map
        // to InvalidData — wrapper must NOT keep retrying.
        let (reconnect, calls) =
            fake_reconnect(vec![ReconnectResponse::Failure(io::ErrorKind::InvalidData)]);
        let inner = boxed_mock(vec![
            MockChunk::Data(b"hello ".to_vec()),
            MockChunk::Error(io::ErrorKind::ConnectionReset),
        ]);
        let mut wrapper =
            ResumableS3Read::from_parts(reconnect, inner, "k".into(), Some(11), cfg(3));

        let mut out = Vec::new();
        let err = wrapper.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "should not retry past InvalidData"
        );
    }

    #[tokio::test]
    async fn unknown_total_size_skips_premature_eof_detection() {
        // No total_size: a clean short read is treated as real EOF,
        // not as a premature truncation to resume from.
        let (reconnect, calls) = fake_reconnect(vec![]);
        let inner = boxed_mock(vec![MockChunk::Data(b"hello".to_vec())]);
        let mut wrapper = ResumableS3Read::from_parts(reconnect, inner, "k".into(), None, cfg(3));

        let mut out = Vec::new();
        wrapper.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello");
        assert!(calls.lock().unwrap().is_empty());
    }

    #[test]
    fn is_retryable_classification() {
        use io::ErrorKind::*;
        for k in [
            ConnectionReset,
            ConnectionAborted,
            BrokenPipe,
            TimedOut,
            UnexpectedEof,
            Interrupted,
            Other,
        ] {
            assert!(is_retryable(&io::Error::new(k, "x")), "{k:?} should retry");
        }
        for k in [NotFound, PermissionDenied, InvalidData, InvalidInput] {
            assert!(
                !is_retryable(&io::Error::new(k, "x")),
                "{k:?} should not retry"
            );
        }
    }
}
