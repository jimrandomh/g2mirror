//! Full-chain test: a websocket device connects through a real
//! g2mirror-server to a real g2mirror wrapper session and views it.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::io::AsyncBufReadExt as _;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn recv(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for ws message")
            .expect("ws closed")
            .expect("ws error");
        if let Message::Text(text) = msg {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

#[tokio::test]
async fn device_views_real_session_through_server() {
    let dir: PathBuf = std::env::temp_dir().join(format!("g2t-{}-chain", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let token = "chain-token";
    let hash = sha2::Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    std::fs::write(
        dir.join("config.json"),
        json!({"listen_addr": "127.0.0.1", "port": 0, "auth_token_hash": hash}).to_string(),
    )
    .unwrap();

    // A wrapped app that paints a marker, then repaints its size on SIGWINCH
    // (like a fullscreen app would).
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args([
            "sh",
            "-c",
            "trap 'stty size' WINCH; printf 'chain-marker\\n'; for i in 1 2 3 4 5 6 7 8 9 10; do sleep 0.3; done",
        ])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let wrapper_pid = wrapper.id().unwrap();

    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
        .env("G2MIRROR_DIR", &dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut server_out = tokio::io::BufReader::new(server.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), server_out.read_line(&mut line))
        .await
        .expect("server did not start")
        .unwrap();
    let addr = line.trim().rsplit(' ').next().unwrap().to_string();

    // Give the wrapper a moment to bind its socket and process app output.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .unwrap();
    ws.send(Message::text(
        json!({"type": "init", "version": 1, "auth_token": token,
               "device": "chain test", "width": 96, "height": 24})
        .to_string(),
    ))
    .await
    .unwrap();
    assert_eq!(recv(&mut ws).await["type"], "init");

    ws.send(Message::text(json!({"type": "list"}).to_string()))
        .await
        .unwrap();
    let sessions = recv(&mut ws).await;
    let socket = sessions["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["pid"] == wrapper_pid)
        .expect("wrapper session not listed")["socket"]
        .as_str()
        .unwrap()
        .to_string();

    ws.send(Message::text(json!({"type": "connect", "socket": socket}).to_string()))
        .await
        .unwrap();
    let connect = recv(&mut ws).await;
    assert_eq!(connect["type"], "connect");
    assert_eq!(connect["pid"], wrapper_pid);

    ws.send(Message::text(json!({"type": "view"}).to_string()))
        .await
        .unwrap();
    let snapshot = recv(&mut ws).await;
    assert_eq!(snapshot["type"], "snapshot");

    // Apply snapshot + streamed output to a device-sized emulator; the
    // app's SIGWINCH handler prints its new size, proving the resize
    // reached it through the whole chain.
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut device = vt100::Parser::new(24, 96, 0);
    device.process(&b64.decode(snapshot["data"].as_str().unwrap()).unwrap());
    assert!(device.screen().contents().contains("chain-marker"));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !device.screen().contents().contains("24 96") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "app never reported the device size; screen:\n{}",
            device.screen().contents()
        );
        let msg = recv(&mut ws).await;
        if msg["type"] == "output" {
            device.process(&b64.decode(msg["data"].as_str().unwrap()).unwrap());
        }
    }

    wrapper.kill().await.ok();
    server.kill().await.ok();
}
