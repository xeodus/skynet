use axum::{routing::get, Router};
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use std::convert::Infallible;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

pub async fn serve_mock_origin(
    listener: TcpListener,
    payload_size: usize,
    chunk_size: usize,
    delay_ms: u64,
) {
    let app = Router::new().route(
        "/large-file",
        get(move || async move {
            let total_chunks = payload_size / chunk_size;
            let chunk = vec![b'a'; chunk_size];

            let stream = stream::iter(0..total_chunks).then(move |_| {
                let chunk = chunk.clone();

                async move {
                    if delay_ms > 0 {
                        sleep(Duration::from_millis(delay_ms)).await;
                    }

                    Ok::<_, Infallible>(Bytes::from(chunk))
                }
            });

            axum::body::Body::from_stream(stream)
        }),
    );

    axum::serve(listener, app)
        .await
        .expect("mock origin server failed");
}
