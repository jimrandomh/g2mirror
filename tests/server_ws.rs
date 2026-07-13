//! End-to-end test of g2mirror-server: auth, session listing, and message
//! relay between a websocket device and a (fake) wrapper session socket.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio_tungstenite::tungstenite::Message;

fn test_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("g2t-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

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
            return serde_json::from_str(&text).expect("invalid JSON from server");
        }
    }
}

async fn send(ws: &mut Ws, msg: Value) {
    ws.send(Message::text(msg.to_string())).await.unwrap();
}

#[tokio::test]
async fn server_auth_list_connect_and_relay() {
    let dir = test_dir("server");
    let token = "test-token";
    let hash = sha2::Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    std::fs::write(
        dir.join("config.json"),
        json!({"listen_addr": "127.0.0.1", "port": 0, "auth_token_hash": hash}).to_string(),
    )
    .unwrap();

    // A fake wrapper session socket owned by this (live) test process.
    let socket_name = format!("{}-fake_cwd", std::process::id());
    let fake_wrapper = tokio::net::UnixListener::bind(dir.join(&socket_name)).unwrap();

    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
        .env("G2MIRROR_DIR", &dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut server_out = BufReader::new(server.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), server_out.read_line(&mut line))
        .await
        .expect("server did not start")
        .unwrap();
    let addr = line
        .trim()
        .rsplit(' ')
        .next()
        .expect("no listen address in server output");

    // The server monitor-connects to every live session socket at startup,
    // identifying itself with a monitor message. Keep this connection: bells
    // written to it must reach devices.
    let (monitor_stream, _) = tokio::time::timeout(Duration::from_secs(5), fake_wrapper.accept())
        .await
        .expect("server never opened a monitor connection")
        .unwrap();
    let (monitor_read, mut monitor_write) = monitor_stream.into_split();
    let mut monitor_reader = BufReader::new(monitor_read);
    let mut line = String::new();
    monitor_reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap(),
        json!({"type": "monitor", "version": 1})
    );

    // Bad token is rejected.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .unwrap();
    send(
        &mut ws,
        json!({"type": "init", "version": 1, "auth_token": "wrong",
               "device": "T", "width": 96, "height": 24}),
    )
    .await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "authentication failed");

    // Good token: init handshake.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .unwrap();
    send(
        &mut ws,
        json!({"type": "init", "version": 1, "auth_token": token,
               "device": "Jim's G2 glasses", "width": 96, "height": 24}),
    )
    .await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "init");
    assert_eq!(reply["version"], 1);
    assert_eq!(reply["readonly"], false);

    // List shows the fake session.
    send(&mut ws, json!({"type": "list"})).await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "sessions");
    let sessions = reply["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["socket"], socket_name.as_str());
    assert_eq!(sessions[0]["cwd_hint"], "fake_cwd");
    assert_eq!(sessions[0]["last_bell_at"], Value::Null);
    assert_eq!(sessions[0]["title"], Value::Null);

    // Bells and titles reported on the monitor connection are pushed to the
    // device and recorded for subsequent lists.
    monitor_write
        .write_all(
            b"{\"type\":\"bell\",\"at\":1234567890123}\n\
              {\"type\":\"title\",\"title\":\"long task \\u2014 running\"}\n",
        )
        .await
        .unwrap();
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "bell");
    assert_eq!(reply["socket"], socket_name.as_str());
    assert_eq!(reply["last_bell_at"], 1234567890123u64);
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "title");
    assert_eq!(reply["socket"], socket_name.as_str());
    assert_eq!(reply["title"], "long task — running");
    send(&mut ws, json!({"type": "list"})).await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["sessions"][0]["last_bell_at"], 1234567890123u64);
    assert_eq!(reply["sessions"][0]["title"], "long task — running");

    // Connect: the server dials the session socket and sends an init derived
    // from the device's init.
    send(&mut ws, json!({"type": "connect", "socket": socket_name})).await;
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), fake_wrapper.accept())
        .await
        .expect("server never dialed the session socket")
        .unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut wrapper_reader = BufReader::new(read_half);
    let mut line = String::new();
    wrapper_reader.read_line(&mut line).await.unwrap();
    let init: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(init["type"], "init");
    assert_eq!(init["version"], 1);
    assert_eq!(init["device"], "Jim's G2 glasses");
    assert_eq!(init["width"], 96);
    assert_eq!(init["height"], 24);

    // Wrapper -> device relay is verbatim.
    write_half
        .write_all(b"{\"type\":\"connect\",\"version\":1,\"pid\":42,\"command\":\"vim\",\"cwd\":\"/x\",\"host_width\":120,\"host_height\":40}\n")
        .await
        .unwrap();
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "connect");
    assert_eq!(reply["pid"], 42);
    assert_eq!(reply["command"], "vim");

    // Device -> wrapper relay is verbatim for non-server message types,
    // including input (the server is not read-only here).
    for msg in [json!({"type": "view"}), json!({"type": "input", "data": "aGkNCg=="})] {
        send(&mut ws, msg.clone()).await;
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), wrapper_reader.read_line(&mut line))
            .await
            .expect("message was not relayed")
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap(), msg);
    }

    // Wrapper hangup surfaces as a disconnected message.
    drop(write_half);
    drop(wrapper_reader);
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "disconnected");

    // Unknown message with no session connected gets an error.
    send(&mut ws, json!({"type": "view"})).await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "error");
}

#[tokio::test]
async fn readonly_server_rejects_input() {
    let dir = test_dir("roserver");
    let token = "ro-token";
    let hash = sha2::Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    std::fs::write(
        dir.join("config.json"),
        json!({"listen_addr": "127.0.0.1", "port": 0,
               "auth_token_hash": hash, "readonly": true})
        .to_string(),
    )
    .unwrap();

    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
        .env("G2MIRROR_DIR", &dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut server_out = BufReader::new(server.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), server_out.read_line(&mut line))
        .await
        .expect("server did not start")
        .unwrap();
    let addr = line.trim().rsplit(' ').next().unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .unwrap();
    send(
        &mut ws,
        json!({"type": "init", "version": 1, "auth_token": token,
               "device": "T", "width": 96, "height": 24}),
    )
    .await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "init");
    assert_eq!(reply["readonly"], true);

    // Input is refused by the server itself, before any session forwarding.
    send(&mut ws, json!({"type": "input", "data": "aGkNCg=="})).await;
    let reply = recv(&mut ws).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "server is read-only");
}
