use control_plane::ControlConfig;
use std::net::SocketAddr;
use tokio::net::{TcpListener, UdpSocket};

#[tokio::main]
async fn main() {
    let bind: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:18090".into())
        .parse()
        .expect("bind addr");
    let dns_bind: SocketAddr = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:18053".into())
        .parse()
        .expect("dns addr");

    let listener = TcpListener::bind(bind).await.expect("bind control");
    let dns = UdpSocket::bind(dns_bind).await.expect("bind dns");
    eprintln!("control-plane http {bind} dns {dns_bind}");
    control_plane::serve_with_dns(listener, dns, ControlConfig::default())
        .await
        .expect("control-plane");
}
