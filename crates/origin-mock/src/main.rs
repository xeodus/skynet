use origin_mock::{run, OriginConfig, OriginHandle};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:18080".into());
    let size: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64 * 1024);
    let delay_ms: u64 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let listener = TcpListener::bind(&bind).await.expect("bind origin");
    eprintln!("origin-mock {bind} size={size} delay_ms={delay_ms}");
    run(
        listener,
        OriginConfig {
            default_size: size,
            chunk_size: 8 * 1024,
            delay_ms,
            error_status: None,
        },
        OriginHandle::new(),
    )
    .await;
}
