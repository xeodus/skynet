use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::server::ServerConfig;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use telemetry::EdgeMetrics;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

type ProxyBody = BoxBody<Bytes, BoxError>;
type HttpClient = Client<HttpConnector, ProxyBody>;

pub async fn serve_plain(
    listener: TcpListener,
    origin_addr: SocketAddr,
    metrics: Arc<EdgeMetrics>,
) -> std::io::Result<()> {
    let client = http_client();

    loop {
        let (stream, _) = listener.accept().await?;

        let io = TokioIo::new(stream);
        let metrics = metrics.clone();
        let client = client.clone();

        tokio::spawn(async move {
            let _ = serve_connection(io, origin_addr, metrics, client).await;
        });
    }
}

pub async fn serve_tls(
    listener: TcpListener,
    origin_addr: SocketAddr,
    tls_config: Arc<ServerConfig>,
    metrics: Arc<EdgeMetrics>,
) -> std::io::Result<()> {
    let acceptor = TlsAcceptor::from(tls_config);
    let client = http_client();

    loop {
        let (stream, _) = listener.accept().await?;

        let acceptor = acceptor.clone();
        let metrics = metrics.clone();
        let client = client.clone();

        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = TokioIo::new(tls_stream);
                    let _ = serve_connection(io, origin_addr, metrics, client).await;
                }
                Err(_) => {}
            }
        });
    }
}

pub fn tls_server_config_from_pem(
    cert_pem: &str,
    key_pem: &str,
) -> Result<Arc<ServerConfig>, BoxError> {
    let mut cert_reader = cert_pem.as_bytes();
    let mut key_reader = key_pem.as_bytes();

    let cert_chain = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    if cert_chain.is_empty() {
        return Err("no certificates found in PEM".into());
    }

    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| e.to_string())?
        .ok_or("no private key found in PEM")?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| e.to_string())?;

    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(Arc::new(config))
}

fn http_client() -> HttpClient {
    Client::builder(TokioExecutor::new()).build_http()
}

async fn serve_connection<I>(
    io: I,
    origin_addr: SocketAddr,
    metrics: Arc<EdgeMetrics>,
    client: HttpClient,
) -> hyper::Result<()>
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let service = service_fn(move |req: Request<Incoming>| {
        let client = client.clone();
        let metrics = metrics.clone();

        async move { handle_request(req, origin_addr, metrics, client).await }
    });

    http1::Builder::new().serve_connection(io, service).await
}

async fn handle_request(
    req: Request<Incoming>,
    origin_addr: SocketAddr,
    metrics: Arc<EdgeMetrics>,
    client: HttpClient,
) -> Result<Response<ProxyBody>, BoxError> {
    let start = Instant::now();

    if req.uri().path() == "/__metrics" {
        return Ok(metrics_response(&metrics));
    }

    match forward_to_origin(req, origin_addr, client).await {
        Ok(upstream_res) => Ok(success_response(upstream_res, start, metrics)),
        Err(_) => Ok(error_response(
            hyper::StatusCode::BAD_GATEWAY,
            start,
            metrics,
        )),
    }
}

async fn forward_to_origin(
    req: Request<Incoming>,
    origin_addr: SocketAddr,
    client: HttpClient,
) -> Result<Response<Incoming>, BoxError> {
    let method = req.method().clone();

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());

    let upstream_uri = format!("http://{origin_addr}{path_and_query}");

    let mut builder = Request::builder().method(method).uri(upstream_uri);

    {
        let headers = builder
            .headers_mut()
            .expect("request builder should have headers");

        for (name, value) in req.headers() {
            if name == hyper::header::HOST || is_hop_by_hop(name.as_str()) {
                continue;
            }

            headers.insert(name.clone(), value.clone());
        }

        headers.insert(
            hyper::header::HOST,
            origin_addr.to_string().parse().map_err(|e| {
                format!("failed to parse origin host header: {e}")
            })?,
        );
    }

    let body: ProxyBody = req
        .into_body()
        .map_err(|e| Box::new(e) as BoxError)
        .boxed();

    let upstream_req = builder.body(body)?;

    let upstream_res = client
        .request(upstream_req)
        .await
        .map_err(|e| e.to_string())?;

    Ok(upstream_res)
}

fn success_response(
    res: Response<Incoming>,
    start: Instant,
    metrics: Arc<EdgeMetrics>,
) -> Response<ProxyBody> {
    let (mut parts, body) = res.into_parts();

    parts.headers = filter_hop_headers(parts.headers);

    let inner: BoxBody<Bytes, BoxError> = body
        .map_err(|e| Box::new(e) as BoxError)
        .boxed();

    let body: ProxyBody = CountingBody::new(inner, metrics, start).boxed();

    Response::from_parts(parts, body)
}

fn error_response(
    status: hyper::StatusCode,
    start: Instant,
    metrics: Arc<EdgeMetrics>,
) -> Response<ProxyBody> {
    let inner: BoxBody<Bytes, BoxError> = Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed();

    let body: ProxyBody = CountingBody::new(inner, metrics, start).boxed();

    Response::builder()
        .status(status)
        .body(body)
        .expect("error response should build")
}

fn metrics_response(metrics: &Arc<EdgeMetrics>) -> Response<ProxyBody> {
    let text = metrics.render();

    let body: ProxyBody = Full::new(Bytes::from(text))
        .map_err(|never| match never {})
        .boxed();

    Response::builder()
        .status(200)
        .header("content-type", "text/plain; version=0.0.4")
        .body(body)
        .expect("metrics response should build")
}

fn filter_hop_headers(headers: hyper::HeaderMap) -> hyper::HeaderMap {
    let mut filtered = hyper::HeaderMap::new();

    for (name, value) in headers.iter() {
        if !is_hop_by_hop(name.as_str()) {
            filtered.insert(name.clone(), value.clone());
        }
    }

    filtered
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

struct CountingBody<B> {
    inner: B,
    metrics: Arc<EdgeMetrics>,
    start: Instant,
    bytes: u64,
    finished: bool,
}

impl<B> CountingBody<B> {
    fn new(inner: B, metrics: Arc<EdgeMetrics>, start: Instant) -> Self {
        Self {
            inner,
            metrics,
            start,
            bytes: 0,
            finished: false,
        }
    }

    fn record(&mut self) {
        if !self.finished {
            self.finished = true;

            self.metrics.add_bytes(self.bytes);
            self.metrics.observe_latency(self.start.elapsed());
        }
    }
}

impl<B> Body for CountingBody<B>
where
    B: Body<Data = Bytes, Error = BoxError> + Unpin,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Pending => Poll::Pending,

            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.bytes += data.len() as u64;
                }

                Poll::Ready(Some(Ok(frame)))
            }

            Poll::Ready(Some(Err(err))) => {
                this.record();
                Poll::Ready(Some(Err(err)))
            }

            Poll::Ready(None) => {
                this.record();
                Poll::Ready(None)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<B> Drop for CountingBody<B> {
    fn drop(&mut self) {
        self.record();
    }
}
