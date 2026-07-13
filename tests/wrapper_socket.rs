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

#[tokio::test]
async fn monitor_gets_debounced_bells_and_does_not_block_viewers() {
    let dir = test_dir("bell");
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args([
            "sh",
            "-c",
            // A window title, then two bells 200ms apart: the first bell
            // reports immediately, the second is held by the 3s debounce
            // window.
            "printf '\\033]2;agent busy\\007'; sleep 0.4; \
             printf '\\a'; sleep 0.2; printf '\\a'; sleep 3",
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

    // Connect as the monitor.
    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut monitor = BufReader::new(read_half);
    assert_eq!(read_msg(&mut monitor).await["type"], "connect");
    write_half
        .write_all(b"{\"type\":\"monitor\",\"version\":1}\n")
        .await
        .unwrap();

    // A monitor attaching after the app set its title learns it right away.
    let title = read_msg(&mut monitor).await;
    assert_eq!(title["type"], "title");
    assert_eq!(title["title"], "agent busy");

    // The first bell arrives promptly with a plausible timestamp.
    let before = now_ms();
    let bell = read_msg(&mut monitor).await;
    assert_eq!(bell["type"], "bell");
    let at = bell["at"].as_u64().unwrap();
    assert!(at >= before && at <= now_ms() + 1000, "bell at {at} out of range");

    // The second bell (inside the window) is debounced: nothing for ~1s.
    let mut line = String::new();
    let quiet = tokio::time::timeout(Duration::from_secs(1), monitor.read_line(&mut line)).await;
    assert!(quiet.is_err(), "second bell was not debounced: {line}");

    // The monitor does not occupy the viewer slot: a device can still
    // init and view.
    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let (viewer_read, mut viewer_write) = stream.into_split();
    let mut viewer = BufReader::new(viewer_read);
    assert_eq!(read_msg(&mut viewer).await["type"], "connect");
    viewer_write
        .write_all(
            b"{\"type\":\"init\",\"version\":1,\"device\":\"t\",\"width\":96,\"height\":24}\n\
              {\"type\":\"view\"}\n",
        )
        .await
        .unwrap();
    // Viewers also learn the current title on attach, before the snapshot.
    let title = read_msg(&mut viewer).await;
    assert_eq!(title["type"], "title");
    assert_eq!(title["title"], "agent busy");
    assert_eq!(read_msg(&mut viewer).await["type"], "snapshot");

    // The held bell is reported when the debounce window expires.
    let trailing = read_msg(&mut monitor).await;
    assert_eq!(trailing["type"], "bell");
    assert!(trailing["at"].as_u64().unwrap() >= at);

    wrapper.kill().await.ok();
}

#[tokio::test]
async fn title_flag_sets_initial_title() {
    let dir = test_dir("title");
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args(["--title", "initial title", "sh", "-c", "sleep 2"])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let wrapper_pid = wrapper.id().unwrap();

    // The host terminal gets the title escape sequence at startup.
    let mut host_stdout = wrapper.stdout.take().unwrap();
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];
    while !String::from_utf8_lossy(&seen).contains("\x1b]2;initial title\x07") {
        let n = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::io::AsyncReadExt::read(&mut host_stdout, &mut chunk),
        )
        .await
        .expect("title sequence never reached the host stream")
        .unwrap();
        assert!(n > 0, "wrapper stdout closed early");
        seen.extend_from_slice(&chunk[..n]);
    }

    // A monitor learns the initial title on attach, even though the wrapped
    // program never set one.
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
    let mut monitor = BufReader::new(read_half);
    assert_eq!(read_msg(&mut monitor).await["type"], "connect");
    write_half
        .write_all(b"{\"type\":\"monitor\",\"version\":1}\n")
        .await
        .unwrap();
    let title = read_msg(&mut monitor).await;
    assert_eq!(title["type"], "title");
    assert_eq!(title["title"], "initial title");

    wrapper.kill().await.ok();
}

async fn find_socket(dir: &std::path::Path, pid: u32) -> PathBuf {
    for _ in 0..100 {
        if let Some(entry) = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .find(|e| e.file_name().to_string_lossy().starts_with(&format!("{pid}-")))
        {
            return entry.path();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("session socket never appeared");
}

#[tokio::test]
async fn input_reaches_the_wrapped_program() {
    let dir = test_dir("input");
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args(["cat"])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let socket_path = find_socket(&dir, wrapper.id().unwrap()).await;

    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut viewer = BufReader::new(read_half);
    assert_eq!(read_msg(&mut viewer).await["type"], "connect");

    let input = base64_encode(b"hello from glasses\r");
    write_half
        .write_all(
            format!(
                "{}\n{}\n{}\n",
                r#"{"type":"init","version":1,"device":"t","width":96,"height":24}"#,
                r#"{"type":"view"}"#,
                format_args!(r#"{{"type":"input","data":"{input}"}}"#),
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert_eq!(read_msg(&mut viewer).await["type"], "snapshot");

    // The pty echoes the typed line, so it comes back in the output stream.
    let mut device = vt100::Parser::new(24, 96, 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !device.screen().contents().contains("hello from glasses") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "input never echoed back; screen:\n{}",
            device.screen().contents()
        );
        let msg = read_msg(&mut viewer).await;
        if msg["type"] == "output" {
            device.process(&base64_decode(msg["data"].as_str().unwrap()));
        }
    }

    wrapper.kill().await.ok();
}

#[tokio::test]
async fn readonly_wrapper_rejects_input_without_dropping_connection() {
    let dir = test_dir("readonly");
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args(["--readonly", "sh", "-c", "sleep 2"])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let socket_path = find_socket(&dir, wrapper.id().unwrap()).await;

    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut viewer = BufReader::new(read_half);
    let connect = read_msg(&mut viewer).await;
    assert_eq!(connect["type"], "connect");
    assert_eq!(connect["readonly"], true);

    let input = base64_encode(b"denied\r");
    write_half
        .write_all(
            format!(
                "{}\n{}\n",
                r#"{"type":"init","version":1,"device":"t","width":96,"height":24}"#,
                format_args!(r#"{{"type":"input","data":"{input}"}}"#),
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let reply = read_msg(&mut viewer).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "session is read-only");

    // The connection survives the rejection: view still works.
    write_half.write_all(b"{\"type\":\"view\"}\n").await.unwrap();
    assert_eq!(read_msg(&mut viewer).await["type"], "snapshot");

    wrapper.kill().await.ok();
}

#[tokio::test]
async fn history_covers_output_scrolled_before_connect() {
    let dir = test_dir("history");
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        // 60 lines scroll through the 24-row (fallback-size) screen before
        // any client connects.
        .args(["sh", "-c", "seq 1 60; sleep 3"])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let socket_path = find_socket(&dir, wrapper.id().unwrap()).await;

    // Give the app time to finish printing before we look.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut viewer = BufReader::new(read_half);
    let connect = read_msg(&mut viewer).await;
    assert_eq!(connect["type"], "connect");
    let next = connect["history"]["next"].as_u64().unwrap();
    assert!(next > 30, "lines that scrolled pre-connect are archived");
    assert_eq!(connect["history"]["oldest"], 0);

    write_half
        .write_all(
            b"{\"type\":\"init\",\"version\":1,\"device\":\"t\",\"width\":96,\"height\":24}\n\
              {\"type\":\"view\"}\n",
        )
        .await
        .unwrap();
    let snapshot = read_msg(&mut viewer).await;
    assert_eq!(snapshot["type"], "snapshot");
    let history_next = snapshot["history_next"].as_u64().unwrap();

    // Page backwards until we have everything, then verify that history plus
    // the snapshot cover all 60 numbers in order.
    let mut texts: Vec<String> = Vec::new();
    let mut before = history_next;
    loop {
        write_half
            .write_all(format!("{{\"type\":\"history\",\"before\":{before},\"limit\":10}}\n").as_bytes())
            .await
            .unwrap();
        let reply = loop {
            let msg = read_msg(&mut viewer).await;
            if msg["type"] == "history_lines" {
                break msg;
            }
            assert_eq!(msg["type"], "output", "unexpected {msg}");
        };
        let lines = reply["lines"].as_array().unwrap();
        let start = reply["start"].as_u64().unwrap();
        let mut chunk: Vec<String> = lines
            .iter()
            .map(|l| {
                let mut p = vt100::Parser::new(1, l["width"].as_u64().unwrap() as u16, 0);
                p.process(&base64_decode(l["data"].as_str().unwrap()));
                p.screen().contents().trim_end().to_string()
            })
            .collect();
        chunk.extend(texts);
        texts = chunk;
        if start == 0 {
            break;
        }
        assert_eq!(lines.len(), 10, "full pages until the oldest line");
        before = start;
    }

    let mut device = vt100::Parser::new(24, 96, 0);
    device.process(&base64_decode(snapshot["data"].as_str().unwrap()));
    let screen = device.screen().contents();
    let all = format!("{}\n{}", texts.join("\n"), screen);
    let numbers: Vec<u32> = all
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect();
    assert_eq!(
        numbers,
        (1..=60).collect::<Vec<u32>>(),
        "history plus snapshot must cover all output in order:\n{all}"
    );

    wrapper.kill().await.ok();
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
