//! Full-chain test of g2mirror-view: a real wrapper and server, with the
//! viewer TUI driven through a pty — list, attach (history + live view),
//! typed input, detach, quit.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

fn test_dir(tag: &str) -> PathBuf {
    // Keep it short: socket paths are limited to ~104 bytes.
    let dir = std::env::temp_dir().join(format!("g2t-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

/// Feed the viewer's pty output into `term` until `pred` holds.
async fn read_until(
    pty: &mut pty_process::OwnedReadPty,
    term: &mut vt100::Parser,
    what: &str,
    pred: impl Fn(&vt100::Screen) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut buf = [0u8; 16 * 1024];
    while !pred(term.screen()) {
        let n = tokio::time::timeout_at(deadline, pty.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}; screen:\n{}",
                term.screen().contents()))
            .expect("viewer pty closed early");
        term.process(&buf[..n]);
    }
}

/// All lines (scrollback plus screen), oldest first, trimmed, blanks
/// dropped.
fn all_lines(term: &mut vt100::Parser) -> Vec<String> {
    term.screen_mut().set_scrollback(usize::MAX);
    let total = term.screen().scrollback();
    let mut lines = Vec::new();
    for i in 0..total {
        term.screen_mut().set_scrollback(total - i);
        lines.push(term.screen().rows(0, 200).next().unwrap_or_default());
    }
    term.screen_mut().set_scrollback(0);
    lines.extend(term.screen().rows(0, 200));
    lines
        .into_iter()
        .map(|l| l.trim_end().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

#[tokio::test]
async fn viewer_tui_lists_attaches_mirrors_and_detaches() {
    let dir = test_dir("vtui");
    let token = "viewer-token";
    let hash: String = sha2::Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::fs::write(
        dir.join("config.json"),
        json!({
            "listen_addr": "127.0.0.1", "port": 0,
            "auth_tokens": [{"name": "viewer", "token_hash": hash, "readonly": false}]
        })
        .to_string(),
    )
    .unwrap();

    // The wrapped app scrolls 40 numbered lines through the wrapper's
    // default 24-row screen (the first ~16 land in the history archive),
    // then echoes whatever is typed.
    let mut wrapper = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args(["sh", "-c", "seq 1 40; cat"])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let wrapper_pid = wrapper.id().unwrap();
    find_socket(&dir, wrapper_pid).await;
    // Let seq finish scrolling before the viewer snapshots.
    tokio::time::sleep(Duration::from_millis(400)).await;

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
    let addr = line.trim().rsplit(' ').next().unwrap().to_string();

    // The viewer runs on its own 30x100 pty.
    let (pty, pts) = pty_process::open().unwrap();
    pty.resize(pty_process::Size::new(30, 100)).unwrap();
    let mut viewer = pty_process::Command::new(env!("CARGO_BIN_EXE_g2mirror-view"))
        .arg(format!("g2mirror://{token}@{addr}"))
        .spawn(pts)
        .unwrap();
    let (mut vread, mut vwrite) = pty.into_split();
    let mut term = vt100::Parser::new(30, 100, 500);

    // The session list appears, showing the wrapper's session.
    let pid_str = wrapper_pid.to_string();
    read_until(&mut vread, &mut term, "the session list", |s| {
        s.contents().contains(&pid_str)
    })
    .await;

    // Attach. The scrolled-off history plus the live viewport must cover
    // all 40 lines, in order, across the viewer's scrollback and screen.
    vwrite.write_all(b"\r").await.unwrap();
    read_until(&mut vread, &mut term, "the mirrored viewport", |s| {
        s.contents().contains("40") && !s.contents().contains("g2mirror-view")
    })
    .await;
    let numbers: Vec<u32> = all_lines(&mut term)
        .iter()
        .filter_map(|l| l.trim().parse().ok())
        .collect();
    assert_eq!(
        numbers,
        (1..=40).collect::<Vec<u32>>(),
        "history plus viewport must cover all output in order; lines:\n{:?}",
        all_lines(&mut term)
    );

    // Typed keys are forwarded to the wrapped app (cat echoes them back
    // through the mirror).
    vwrite.write_all(b"tui-echo\r").await.unwrap();
    read_until(&mut vread, &mut term, "the echoed input", |s| {
        s.contents().contains("tui-echo")
    })
    .await;

    // Ctrl+D detaches back to the session list.
    vwrite.write_all(&[0x04]).await.unwrap();
    read_until(&mut vread, &mut term, "the list after detach", |s| {
        s.contents().contains("g2mirror-view")
    })
    .await;

    // Quit: the viewer exits cleanly and leaves the alternate screen, so
    // the mirrored content (still on the main screen) is what remains.
    vwrite.write_all(b"q").await.unwrap();
    // Keep draining the pty (its output buffer is small; the viewer's
    // final writes would block otherwise) until the viewer closes it.
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while let Ok(Ok(n)) = tokio::time::timeout_at(deadline, vread.read(&mut buf)).await {
        if n == 0 {
            break;
        }
    }
    let status = tokio::time::timeout(Duration::from_secs(5), viewer.wait())
        .await
        .expect("viewer did not exit on q")
        .unwrap();
    assert!(status.success(), "viewer exited with {status:?}");

    server.kill().await.ok();
    wrapper.kill().await.ok();
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Regression test: wrappers from before snapshots stated their dimensions
/// (protocol additions are ignored by old peers in both directions) must
/// still be viewable — the viewer falls back to the dimensions it declared
/// at init, which is what an old single-viewer wrapper resizes the app to.
#[tokio::test]
async fn viewer_tolerates_snapshots_without_dimensions() {
    let dir = test_dir("vold");
    let token = "old-token";
    let hash: String = sha2::Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::fs::write(
        dir.join("config.json"),
        json!({
            "listen_addr": "127.0.0.1", "port": 0,
            "auth_tokens": [{"name": "viewer", "token_hash": hash, "readonly": false}]
        })
        .to_string(),
    )
    .unwrap();

    // A fake old-style wrapper session socket, owned by this live process.
    let socket_name = format!("{}-old_cwd", std::process::id());
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
    let addr = line.trim().rsplit(' ').next().unwrap().to_string();

    // Answer connections the way the pre-dimensions wrapper did: the
    // monitor connection just stays open; the viewer's session connection
    // gets connect/snapshot/history_lines without any of the new fields.
    let fake = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (stream, _) = fake_wrapper.accept().await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);
            write_half
                .write_all(
                    format!(
                        "{}\n",
                        json!({"type": "connect", "version": 1, "pid": std::process::id(),
                               "command": "old-app", "cwd": "/old", "host_width": 80,
                               "host_height": 24, "readonly": false,
                               "history": {"next": 2, "oldest": 0}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let first: Value = serde_json::from_str(&line).unwrap();
            if first["type"] == "monitor" {
                held.push((reader, write_half));
                continue;
            }
            assert_eq!(first["type"], "init");
            // view -> old-format snapshot (no width/height).
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["type"],
                "view"
            );
            let snapshot = base64_encode(b"\x1b[0m\x1b[H\x1b[2Jold-wrapper-content");
            write_half
                .write_all(
                    format!(
                        "{}\n",
                        json!({"type": "snapshot", "data": snapshot, "history_next": 2})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            // history -> two plain lines.
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["type"],
                "history"
            );
            write_half
                .write_all(
                    format!(
                        "{}\n",
                        json!({"type": "history_lines", "start": 0, "oldest": 0, "next": 2,
                               "lines": [
                                   {"data": base64_encode(b"old-h1"), "width": 80, "wrapped": false},
                                   {"data": base64_encode(b"old-h2"), "width": 80, "wrapped": false}
                               ]})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            held.push((reader, write_half));
        }
    });

    let (pty, pts) = pty_process::open().unwrap();
    pty.resize(pty_process::Size::new(30, 100)).unwrap();
    let mut viewer = pty_process::Command::new(env!("CARGO_BIN_EXE_g2mirror-view"))
        .arg(format!("g2mirror://{token}@{addr}"))
        .spawn(pts)
        .unwrap();
    let (mut vread, mut vwrite) = pty.into_split();
    let mut term = vt100::Parser::new(30, 100, 500);

    read_until(&mut vread, &mut term, "the session list", |s| {
        s.contents().contains("old_cwd")
    })
    .await;
    vwrite.write_all(b"\r").await.unwrap();
    read_until(&mut vread, &mut term, "the old-style viewport", |s| {
        s.contents().contains("old-wrapper-content")
    })
    .await;
    let lines = all_lines(&mut term);
    assert!(
        lines.iter().any(|l| l == "old-h1") && lines.iter().any(|l| l == "old-h2"),
        "history lines must be printed; lines:\n{lines:?}"
    );

    fake.abort();
    viewer.kill().await.ok();
    server.kill().await.ok();
}
