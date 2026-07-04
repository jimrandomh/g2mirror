//! End-to-end test of the wrapper's session socket protocol: spawn the real
//! g2mirror binary, connect as a client, and drive init/view/output/exit.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

fn test_dir(tag: &str) -> PathBuf {
    // Keep it short: socket paths are limited to ~104 bytes.
    let dir = std::env::temp_dir().join(format!("g2t-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn read_msg(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Value {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for session message")
        .expect("read failed");
    assert!(!line.is_empty(), "session closed unexpectedly");
    serde_json::from_str(&line).expect("invalid JSON from wrapper")
}

#[tokio::test]
async fn session_protocol_end_to_end() {
    let dir = test_dir("wrapper");
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args(["sh", "-c", "printf 'hello-local\\n'; sleep 1; printf 'later\\n'"])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let wrapper_pid = wrapper.id().unwrap();
    let mut host_stdout = wrapper.stdout.take().unwrap();

    // Wait until the app's first output has passed through the wrapper (so
    // it's in the wrapper's screen model before we ask for a snapshot), then
    // keep draining the host stream in the background.
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];
    while !String::from_utf8_lossy(&seen).contains("hello-local") {
        let n = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::io::AsyncReadExt::read(&mut host_stdout, &mut chunk),
        )
        .await
        .expect("app output never reached the host stream")
        .unwrap();
        assert!(n > 0, "wrapper stdout closed early");
        seen.extend_from_slice(&chunk[..n]);
    }
    tokio::spawn(async move {
        let mut sink = [0u8; 4096];
        while tokio::io::AsyncReadExt::read(&mut host_stdout, &mut sink)
            .await
            .is_ok_and(|n| n > 0)
        {}
    });

    // Wait for the session socket to appear.
    let socket_path = {
        let mut found = None;
        for _ in 0..100 {
            if let Some(entry) = std::fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .find(|e| e.file_name().to_string_lossy().starts_with(&format!("{wrapper_pid}-")))
            {
                found = Some(entry.path());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        found.expect("session socket never appeared")
    };

    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // First message from the wrapper is connect.
    let connect = read_msg(&mut reader).await;
    assert_eq!(connect["type"], "connect");
    assert_eq!(connect["version"], 1);
    assert_eq!(connect["pid"], wrapper_pid);
    assert!(connect["command"].as_str().unwrap().starts_with("sh -c"));

    // Init, then view.
    for msg in [
        json!({"type": "init", "version": 1, "device": "test glasses", "width": 96, "height": 24}),
        json!({"type": "view"}),
    ] {
        write_half
            .write_all((msg.to_string() + "\n").as_bytes())
            .await
            .unwrap();
    }

    // Snapshot arrives first; then output streams until the app exits.
    // Feed everything into a device-sized emulator and check the result.
    let mut device = vt100::Parser::new(24, 96, 0);
    let snapshot = read_msg(&mut reader).await;
    assert_eq!(snapshot["type"], "snapshot", "view must be answered with a snapshot");
    let data = base64_decode(snapshot["data"].as_str().unwrap());
    device.process(&data);
    assert!(
        device.screen().contents().contains("hello-local"),
        "snapshot must contain pre-view screen content"
    );

    let exit = loop {
        let msg = read_msg(&mut reader).await;
        match msg["type"].as_str().unwrap() {
            "output" => device.process(&base64_decode(msg["data"].as_str().unwrap())),
            "exit" => break msg,
            other => panic!("unexpected message type {other}"),
        }
    };
    assert_eq!(exit["status"], 0);
    assert!(
        device.screen().contents().contains("later"),
        "streamed output must reach the device emulator"
    );

    // The wrapper removes its socket on exit.
    wrapper.wait().await.unwrap();
    assert!(!socket_path.exists(), "socket file must be removed on exit");
}

fn base64_decode(s: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}
