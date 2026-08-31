use axum::{
    body::Body,
    extract::{Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use bytes::Bytes;
use futures_util::Stream;
use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::net::TcpListener;
use tokio::time::{sleep, Sleep};

#[derive(Clone, Debug)]
pub struct OriginConfig {
    pub default_size: usize,
    pub chunk_size: usize,
    pub delay_ms: u64,
    pub error_status: Option<u16>,
}

impl Default for OriginConfig {
    fn default() -> Self {
        Self {
            default_size: 64 * 1024,
            chunk_size: 8 * 1024,
            delay_ms: 0,
            error_status: None,
        }
    }
}

#[derive(Clone)]
pub struct OriginHandle {
    inner: Arc<Inner>,
}

struct Inner {
    counts: Mutex<HashMap<String, u64>>,
    total: AtomicU64,
}

impl Default for OriginHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl OriginHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                counts: Mutex::new(HashMap::new()),
                total: AtomicU64::new(0),
            }),
        }
    }

    fn record(&self, path: &str) {
        self.inner.total.fetch_add(1, Ordering::SeqCst);
        let mut counts = self.inner.counts.lock().expect("origin counts");
        *counts.entry(path.to_string()).or_insert(0) += 1;
    }

    pub fn path_hits(&self, path: &str) -> u64 {
        let counts = self.inner.counts.lock().expect("origin counts");
        counts.get(path).copied().unwrap_or(0)
    }

    pub fn total_hits(&self) -> u64 {
        self.inner.total.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> OriginStats {
        OriginStats {
            total_hits: self.total_hits(),
        }
    }
}

#[derive(serde::Serialize, Clone, Debug)]
pub struct OriginStats {
    pub total_hits: u64,
}

#[derive(Clone)]
struct AppState {
    config: OriginConfig,
    handle: OriginHandle,
}

#[derive(serde::Deserialize)]
struct SizeQuery {
    size: Option<usize>,
}

pub async fn serve(
    listener: TcpListener,
    payload_size: usize,
    chunk_size: usize,
    delay_ms: u64,
) {
    let handle = OriginHandle::new();
    let config = OriginConfig {
        default_size: payload_size,
        chunk_size,
        delay_ms,
        error_status: None,
    };
    run(listener, config, handle).await;
}

pub async fn run(listener: TcpListener, config: OriginConfig, handle: OriginHandle) {
    let state = AppState { config, handle };
    let app = Router::new()
        .route("/__stats", get(stats_handler))
        .fallback(object_handler)
        .with_state(state);

    axum::serve(listener, app)
        .await
        .expect("origin mock failed");
}

async fn stats_handler(State(state): State<AppState>) -> Json<OriginStats> {
    Json(state.handle.stats())
}

async fn object_handler(
    State(state): State<AppState>,
    Query(query): Query<SizeQuery>,
    req: Request,
) -> Response {
    let path = req.uri().path().to_string();
    state.handle.record(&path);

    if let Some(code) = state.config.error_status {
        let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return (status, "origin error").into_response();
    }

    let size = query.size.unwrap_or(state.config.default_size);
    let chunk_size = state.config.chunk_size.max(1);
    let delay = Duration::from_millis(state.config.delay_ms);

    let stream = ChunkStream {
        remaining: size,
        chunk_size,
        delay,
        sleep: None,
        waiting: delay > Duration::ZERO,
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .body(Body::from_stream(stream))
        .expect("origin response")
}

struct ChunkStream {
    remaining: usize,
    chunk_size: usize,
    delay: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
    waiting: bool,
}

impl Stream for ChunkStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.remaining == 0 {
            return Poll::Ready(None);
        }

        if this.waiting {
            if this.sleep.is_none() {
                this.sleep = Some(Box::pin(sleep(this.delay)));
            }
            if let Some(sleep) = this.sleep.as_mut() {
                match sleep.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.sleep = None;
                        this.waiting = false;
                    }
                }
            }
        }

        let n = this.remaining.min(this.chunk_size);
        this.remaining -= n;
        this.waiting = this.delay > Duration::ZERO && this.remaining > 0;
        Poll::Ready(Some(Ok(Bytes::from(vec![b'a'; n]))))
    }
}

pub async fn wait_for_tcp(addr: std::net::SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("nothing is listening on {addr}");
}
