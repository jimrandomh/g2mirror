//! End-to-end test of the detached-session CLI: `g2mirror --detached`
//! starts a headless session, `--list` shows it as DETACHED, `--attach`
//! (driven through a pty) claims it, mirrors it, detaches with Ctrl+\, and
//! propagates the app's exit.

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

fn test_dir(tag: &str) -> PathBuf {
    // Keep it short: socket paths are limited to ~104 bytes.
    let dir = std::env::temp_dir().join(format!("g2t-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Kill a leaked detached wrapper on drop (it is setsid'd, so a failing
/// test would otherwise leave it running forever).
struct KillOnDrop(u32);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = std::process::Command::new("kill")
            .args(["-9", &self.0.to_string()])
            .status();
    }
}

fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .is_ok_and(|s| s.success())
}

/// Feed a pty's output into `term` until `pred` holds.
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
            .unwrap_or_else(|_| {
                panic!("timed out waiting for {what}; screen:\n{}", term.screen().contents())
            })
            .expect("pty closed early");
        term.process(&buf[..n]);
    }
}

/// Drain a pty until the child closes it, returning everything read.
async fn drain_to_eof(pty: &mut pty_process::OwnedReadPty) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while let Ok(Ok(n)) = tokio::time::timeout_at(deadline, pty.read(&mut buf)).await {
        if n == 0 {
            break;
        }
        all.extend_from_slice(&buf[..n]);
    }
    all
}

/// Spawn `g2mirror --attach [args]` on a fresh 24x80 pty.
fn spawn_attach(
    dir: &std::path::Path,
    args: &[&str],
) -> (
    tokio::process::Child,
    pty_process::OwnedReadPty,
    pty_process::OwnedWritePty,
) {
    let (pty, pts) = pty_process::open().unwrap();
    pty.resize(pty_process::Size::new(24, 80)).unwrap();
    let child = pty_process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .arg("-a")
        .args(args)
        .env("G2MIRROR_DIR", dir)
        .spawn(pts)
        .unwrap();
    let (read, write) = pty.into_split();
    (child, read, write)
}

#[tokio::test]
async fn detached_list_attach_detach_and_reattach() {
    let dir = test_dir("acli");

    // Start a detached session; its socket name is printed on stdout.
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .args([
            "--detached", "--title", "det-cat", "--",
            "sh", "-c", "printf 'detached-hello\\n'; cat",
        ])
        .env("G2MIRROR_DIR", &dir)
        .current_dir("/")
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "--detached failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let socket_name = String::from_utf8(out.stdout).unwrap().trim().to_string();
    let pid: u32 = socket_name.split('-').next().unwrap().parse().unwrap();
    let _cleanup = KillOnDrop(pid);
    let socket_path = dir.join(&socket_name);
    assert!(socket_path.exists(), "--detached must wait for the socket");

    // --list shows it as DETACHED with its pid and title.
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_g2mirror"))
        .arg("--list")
        .env("G2MIRROR_DIR", &dir)
        .output()
        .await
        .unwrap();
    let listing = String::from_utf8(out.stdout).unwrap();
    assert!(out.status.success());
    let line = listing
        .lines()
        .find(|l| l.contains(&pid.to_string()))
        .unwrap_or_else(|| panic!("pid missing from listing:\n{listing}"));
    assert!(line.contains("DETACHED"), "listing was:\n{listing}");
    assert!(line.contains("det-cat"), "listing was:\n{listing}");

    // Attach by pid: the app's output appears, and typed keys reach it
    // (cat echoes them back through the mirror).
    let (mut attach, mut aread, mut awrite) = spawn_attach(&dir, &[&pid.to_string()]);
    let mut term = vt100::Parser::new(24, 80, 500);
    read_until(&mut aread, &mut term, "the mirrored screen", |s| {
        s.contents().contains("detached-hello")
    })
    .await;
    awrite.write_all(b"att-echo\r").await.unwrap();
    read_until(&mut aread, &mut term, "the echoed input", |s| {
        s.contents().contains("att-echo")
    })
    .await;

    // While claimed, a second --attach finds nothing detached and points
    // at --force.
    let (mut second, mut sread, _swrite) = spawn_attach(&dir, &[]);
    let output = String::from_utf8_lossy(&drain_to_eof(&mut sread).await).to_string();
    assert!(output.contains("--force"), "second attach said: {output}");
    let status = tokio::time::timeout(Duration::from_secs(5), second.wait())
        .await
        .expect("second attach did not exit")
        .unwrap();
    assert!(!status.success(), "second attach must fail");

    // Ctrl+\ detaches: the client exits 0, the session lives on.
    awrite.write_all(&[0x1c]).await.unwrap();
    let tail = String::from_utf8_lossy(&drain_to_eof(&mut aread).await).to_string();
    let status = tokio::time::timeout(Duration::from_secs(5), attach.wait())
        .await
        .expect("attach did not exit on Ctrl+\\")
        .unwrap();
    assert!(status.success(), "detach must exit 0 (got {status:?}); output: {tail}");
    assert!(tail.contains("reattach"), "detach message missing: {tail}");
    assert!(pid_alive(pid), "session must survive detach");
    assert!(socket_path.exists(), "socket must survive detach");

    // Reattach without a pattern (it is the only detached session): the
    // screen still shows the earlier interaction. Ctrl+D then ends the
    // wrapped cat, and the app's exit propagates to the attach client.
    let (mut attach, mut aread, mut awrite) = spawn_attach(&dir, &[]);
    let mut term = vt100::Parser::new(24, 80, 500);
    read_until(&mut aread, &mut term, "the reattached screen", |s| {
        s.contents().contains("att-echo")
    })
    .await;
    awrite.write_all(&[0x04]).await.unwrap();
    drain_to_eof(&mut aread).await;
    let status = tokio::time::timeout(Duration::from_secs(5), attach.wait())
        .await
        .expect("attach did not exit when the app ended")
        .unwrap();
    assert!(status.success(), "app exited 0, so the attach client must too");

    // The wrapper is gone and its socket removed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while socket_path.exists() || pid_alive(pid) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "wrapper did not exit after its app ended"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
