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

fn sha256_hex(s: &str) -> String {
    sha2::Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

async fn start_server(dir: &std::path::Path) -> (tokio::process::Child, String) {
    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
        .env("G2MIRROR_DIR", dir)
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
    let addr = line.trim().rsplit(' ').next().unwrap().to_string();
    (server, addr)
}

async fn connect_device(addr: &str, token: &str) -> (Ws, Value) {
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
    (ws, reply)
}

#[tokio::test]
async fn multiple_tokens_have_separate_readonly_flags_and_size_ranks() {
    let dir = test_dir("tokens");
    std::fs::write(
        dir.join("config.json"),
        json!({
            "listen_addr": "127.0.0.1", "port": 0,
            "auth_tokens": [
                {"name": "glasses", "token_hash": sha256_hex("g-token"), "readonly": false},
                {"name": "spectator", "token_hash": sha256_hex("s-token")}
            ],
            "size_precedence": ["glasses", "host", "spectator"]
        })
        .to_string(),
    )
    .unwrap();
    let socket_name = format!("{}-fake_cwd", std::process::id());
    let fake_wrapper = tokio::net::UnixListener::bind(dir.join(&socket_name)).unwrap();
    let (mut server, addr) = start_server(&dir).await;
    // Absorb the server's monitor connection to the fake session.
    let (_monitor, _) = tokio::time::timeout(Duration::from_secs(5), fake_wrapper.accept())
        .await
        .expect("server never opened a monitor connection")
        .unwrap();

    // The spectator token authenticates read-only (per-token default) and
    // its input is refused by the server.
    let (mut spectator, reply) = connect_device(&addr, "s-token").await;
    assert_eq!(reply["type"], "init");
    assert_eq!(reply["readonly"], true);
    send(&mut spectator, json!({"type": "input", "data": "aGkNCg=="})).await;
    let reply = recv(&mut spectator).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "server is read-only");

    // Its session init carries the ranks from size_precedence: spectator at
    // index 2, host at index 1.
    send(&mut spectator, json!({"type": "connect", "socket": socket_name})).await;
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), fake_wrapper.accept())
        .await
        .expect("server never dialed the session socket")
        .unwrap();
    let mut wrapper_reader = BufReader::new(stream);
    let mut line = String::new();
    wrapper_reader.read_line(&mut line).await.unwrap();
    let init: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(init["size_rank"], 2);
    assert_eq!(init["host_size_rank"], 1);

    // The glasses token is writable and ranks first.
    let (mut glasses, reply) = connect_device(&addr, "g-token").await;
    assert_eq!(reply["readonly"], false);
    send(&mut glasses, json!({"type": "connect", "socket": socket_name})).await;
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), fake_wrapper.accept())
        .await
        .expect("server never dialed the session socket for glasses")
        .unwrap();
    let mut wrapper_reader = BufReader::new(stream);
    let mut line = String::new();
    wrapper_reader.read_line(&mut line).await.unwrap();
    let init: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(init["size_rank"], 0);
    assert_eq!(init["host_size_rank"], 1);

    // A token matching no entry is rejected.
    let (_ws, reply) = connect_device(&addr, "nope").await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "authentication failed");

    server.kill().await.ok();
}

/// Accept one connection on a fake session socket, greet it the way a
/// wrapper does (with the given real cwd), and return the reader/writer
/// plus the client's first message.
async fn accept_fake_session(
    listener: &tokio::net::UnixListener,
    cwd: &str,
) -> (
    BufReader<tokio::net::unix::OwnedReadHalf>,
    tokio::net::unix::OwnedWriteHalf,
    Value,
) {
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("no connection to the fake session")
        .unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let greeting = json!({
        "type": "connect", "version": 1, "pid": 42, "command": "app", "cwd": cwd,
        "host_width": 80, "host_height": 24, "readonly": false,
        "history": {"next": 0, "oldest": 0}
    });
    write_half
        .write_all((greeting.to_string() + "\n").as_bytes())
        .await
        .unwrap();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .expect("fake session got no first message")
        .unwrap();
    let first: Value = serde_json::from_str(&line).unwrap();
    (reader, write_half, first)
}

/// Read messages until a `sessions` reply arrives; return its socket names.
async fn list_socket_names(ws: &mut Ws) -> Vec<String> {
    send(ws, json!({"type": "list"})).await;
    loop {
        let msg = recv(ws).await;
        if msg["type"] == "sessions" {
            return msg["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s["socket"].as_str().unwrap().to_string())
                .collect();
        }
    }
}

#[tokio::test]
async fn filtered_token_restricts_list_connect_and_events() {
    let dir = test_dir("filter");
    std::fs::write(
        dir.join("config.json"),
        json!({
            "listen_addr": "127.0.0.1", "port": 0,
            "auth_tokens": [
                {"name": "glasses", "token_hash": sha256_hex("g-token"), "readonly": false},
                {"name": "robert", "token_hash": sha256_hex("r-token"), "filter": [
                    {"path": "/shared/.*"},
                    {"windowtitle": ".*SHARED.*"}
                ]}
            ]
        })
        .to_string(),
    )
    .unwrap();
    let pid = std::process::id();
    let s1_name = format!("{pid}-shared_proj");
    let s2_name = format!("{pid}-private_stuff");
    let s1 = tokio::net::UnixListener::bind(dir.join(&s1_name)).unwrap();
    let s2 = tokio::net::UnixListener::bind(dir.join(&s2_name)).unwrap();
    let (mut server, addr) = start_server(&dir).await;

    // The server monitors both sessions; the greetings carry their real
    // working directories, which is what path filters match against.
    let (_m1_read, _m1_write, first) = accept_fake_session(&s1, "/shared/proj").await;
    assert_eq!(first["type"], "monitor");
    let (_m2_read, mut m2_write, first) = accept_fake_session(&s2, "/private/stuff").await;
    assert_eq!(first["type"], "monitor");

    // Robert sees only the matching terminal (polling until the monitor
    // greetings have registered the cwds).
    let (mut robert, reply) = connect_device(&addr, "r-token").await;
    assert_eq!(reply["type"], "init");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let names = list_socket_names(&mut robert).await;
        if names == [s1_name.clone()] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "filtered list never settled: {names:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Connecting to a hidden terminal is refused.
    send(&mut robert, json!({"type": "connect", "socket": s2_name})).await;
    let reply = recv(&mut robert).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "terminal does not match your token's filter");

    // An unfiltered token sees both terminals.
    let (mut glasses, _) = connect_device(&addr, "g-token").await;
    let mut names = list_socket_names(&mut glasses).await;
    names.sort();
    let mut expected = vec![s1_name.clone(), s2_name.clone()];
    expected.sort();
    assert_eq!(names, expected);

    // A title change can make a terminal visible: the event reaches
    // robert, and connect now succeeds.
    m2_write
        .write_all(b"{\"type\":\"title\",\"title\":\"review SHARED with team\"}\n")
        .await
        .unwrap();
    let event = recv(&mut robert).await;
    assert_eq!(event["type"], "title");
    assert_eq!(event["socket"], s2_name.as_str());
    send(&mut robert, json!({"type": "connect", "socket": s2_name})).await;
    let (_sess_read, _sess_write, first) = accept_fake_session(&s2, "/private/stuff").await;
    assert_eq!(first["type"], "init", "connect must reach the session");
    let relayed = recv(&mut robert).await;
    assert_eq!(relayed["type"], "connect", "greeting must be relayed");

    // ...and a title change away from the pattern revokes it: robert is
    // force-disconnected and does not see the new title.
    m2_write
        .write_all(b"{\"type\":\"title\",\"title\":\"private again\"}\n")
        .await
        .unwrap();
    let msg = recv(&mut robert).await;
    assert_eq!(msg["type"], "disconnected", "got {msg} instead");
    assert_eq!(msg["reason"], "terminal no longer matches your token's filter");

    // Bells from hidden terminals are pushed to unfiltered devices only.
    m2_write
        .write_all(b"{\"type\":\"bell\",\"at\":1234567890123}\n")
        .await
        .unwrap();
    loop {
        let msg = recv(&mut glasses).await;
        if msg["type"] == "bell" {
            assert_eq!(msg["socket"], s2_name.as_str());
            break;
        }
        assert_eq!(msg["type"], "title", "unexpected {msg}");
    }
    let mut quiet = robert.next();
    let got = tokio::time::timeout(Duration::from_millis(500), &mut quiet).await;
    assert!(got.is_err(), "filtered device received: {got:?}");

    server.kill().await.ok();
}

/// Extract the printed one-time token (a 64-char hex line) from command
/// output.
fn printed_token(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find(|l| l.len() == 64 && l.chars().all(|c| c.is_ascii_hexdigit()))
        .unwrap_or_else(|| panic!("no token in output:\n{stdout}"))
        .to_string()
}

#[tokio::test]
async fn init_config_and_add_token_flow() {
    let dir = test_dir("addtok");
    let init_out = std::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
        .arg("--init-config")
        .env("G2MIRROR_DIR", &dir)
        .output()
        .unwrap();
    assert!(init_out.status.success());
    let glasses_token = printed_token(&init_out);

    let add_out = std::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
        .args(["--add-token", "coworker"])
        .env("G2MIRROR_DIR", &dir)
        .output()
        .unwrap();
    assert!(add_out.status.success());
    let coworker_token = printed_token(&add_out);

    // Duplicate names and the reserved "host" are refused.
    for name in ["coworker", "glasses", "host"] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_g2mirror-server"))
            .args(["--add-token", name])
            .env("G2MIRROR_DIR", &dir)
            .output()
            .unwrap();
        assert!(!out.status.success(), "--add-token {name} must fail");
    }

    let config: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("config.json")).unwrap()).unwrap();
    assert_eq!(config["auth_tokens"].as_array().unwrap().len(), 2);
    assert_eq!(config["size_precedence"], json!(["glasses", "host"]));

    // Make the port ephemeral so the test can't collide with a real server.
    let mut config = config;
    config["port"] = json!(0);
    std::fs::write(dir.join("config.json"), config.to_string()).unwrap();

    let (mut server, addr) = start_server(&dir).await;
    let (_ws, reply) = connect_device(&addr, &glasses_token).await;
    assert_eq!(reply["type"], "init");
    assert_eq!(reply["readonly"], false, "the initial token is writable");
    let (_ws, reply) = connect_device(&addr, &coworker_token).await;
    assert_eq!(reply["type"], "init");
    assert_eq!(reply["readonly"], true, "added tokens default to read-only");
    server.kill().await.ok();
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
