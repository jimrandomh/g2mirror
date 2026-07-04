//! g2mirror: run a CLI app in a pty and mirror it to Even Realities G2
//! smart glasses.
//!
//! The wrapped app's output goes to the host terminal, and — while a device
//! is viewing — is also parsed into a vt100 screen model at the device's
//! dimensions and streamed to the device over a unix domain socket session
//! (see PROTOCOL.md). Ctrl+G simulates a device connect/disconnect at 96x24
//! without needing a real client.

mod control;
mod mirror;
mod raw_guard;

use std::os::unix::process::ExitStatusExt as _;
use std::process::ExitStatus;

use anyhow::Context as _;
use g2mirror::protocol::{self, FromSession, ToSession, PROTOCOL_VERSION};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::signal::unix::{signal, SignalKind};

use control::{Client, ClientState, ControlListener};
use mirror::{Mirror, View};

/// Ctrl+G: simulate a device connect/disconnect.
const HOTKEY: u8 = 0x07;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(program) = args.next() else {
        eprintln!("usage: g2mirror <command> [args...]");
        eprintln!("  Ctrl+G simulates glasses connect/disconnect");
        std::process::exit(2);
    };
    let args: Vec<_> = args.collect();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    match runtime.block_on(run(program, args)) {
        Ok(status) => std::process::exit(exit_code(status)),
        Err(e) => {
            eprintln!("g2mirror: {e:#}");
            std::process::exit(1);
        }
    }
}

fn exit_code(status: ExitStatus) -> i32 {
    status
        .code()
        .unwrap_or_else(|| status.signal().map_or(1, |sig| 128 + sig))
}

fn host_size() -> (u16, u16) {
    rustix::termios::tcgetwinsize(rustix::stdio::stdout())
        .map(|ws| (ws.ws_row, ws.ws_col))
        .unwrap_or((24, 80))
}

async fn run(
    program: std::ffi::OsString,
    args: Vec<std::ffi::OsString>,
) -> anyhow::Result<ExitStatus> {
    let (host_rows, host_cols) = host_size();

    let control = ControlListener::bind()?;

    let (pty, pts) = pty_process::open().context("failed to open pty")?;
    pty.resize(pty_process::Size::new(host_rows, host_cols))
        .context("failed to set initial pty size")?;
    let mut child = pty_process::Command::new(&program)
        .args(&args)
        .spawn(pts)
        .with_context(|| format!("failed to spawn {}", program.to_string_lossy()))?;
    let command_line = std::iter::once(&program)
        .chain(&args)
        .map(|a| a.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");

    // Raw mode so keystrokes (including our hotkey) reach us unbuffered. If
    // stdin isn't a tty (e.g. tests, pipes), run without it.
    let _raw = if rustix::termios::isatty(rustix::stdio::stdin()) {
        Some(raw_guard::RawGuard::new().context("failed to enter raw mode")?)
    } else {
        None
    };

    let (mut pty_read, mut pty_write) = pty.into_split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut winch = signal(SignalKind::window_change())?;

    let mut mirror = Mirror::new(host_rows, host_cols);
    let mut client: Option<Client> = None;
    let mut stdin_buf = [0u8; 4096];
    let mut pty_buf = [0u8; 64 * 1024];
    let mut stdin_open = true;

    let status = loop {
        tokio::select! {
            // Keyboard input: intercept the hotkey, forward the rest to the
            // child.
            n = stdin.read(&mut stdin_buf), if stdin_open => match n {
                Ok(0) => stdin_open = false,
                Ok(n) => {
                    let mut rest = &stdin_buf[..n];
                    while let Some(pos) = rest.iter().position(|&b| b == HOTKEY) {
                        pty_write.write_all(&rest[..pos]).await?;
                        toggle_simulated(&mut mirror, &pty_write, &mut stdout).await?;
                        rest = &rest[pos + 1..];
                    }
                    pty_write.write_all(rest).await?;
                }
                Err(e) => return Err(e).context("error reading stdin"),
            },

            // Child output: translate through the mirror; repaint the host
            // and stream to a viewing client.
            n = pty_read.read(&mut pty_buf) => match n {
                // EOF/EIO on the pty master means the child side is gone.
                Ok(0) | Err(_) => break child.wait().await?,
                Ok(n) => {
                    let out = mirror.process(&pty_buf[..n]);
                    stdout.write_all(&out.host).await?;
                    stdout.flush().await?;
                    if let (Some(data), Some(c)) = (out.remote, client.as_mut())
                        && c.state == ClientState::Viewing && !data.is_empty() {
                            let msg = FromSession::Output {
                                data: protocol::encode_terminal_bytes(&data),
                            };
                            if c.send(&msg).await.is_err() {
                                drop_client(&mut client, &mut mirror, &pty_write, &mut stdout)
                                    .await?;
                            }
                        }
                }
            },

            // Host terminal resized.
            _ = winch.recv() => {
                let (rows, cols) = host_size();
                let t = mirror.host_resized(rows, cols);
                apply_transition(&t, &pty_write, &mut stdout).await?;
            }

            // A client connected to the session socket.
            conn = control.accept() => {
                let stream = conn.context("session socket accept failed")?;
                let mut new_client = Client::new(stream);
                let connect = FromSession::Connect {
                    version: PROTOCOL_VERSION,
                    pid: std::process::id(),
                    command: command_line.clone(),
                    cwd: std::env::current_dir()
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    host_width: host_size().1,
                    host_height: host_size().0,
                };
                if client.is_some() {
                    let _ = new_client
                        .send(&FromSession::Error {
                            message: "another client is already connected".into(),
                        })
                        .await;
                    // new_client dropped, connection closes.
                } else if new_client.send(&connect).await.is_ok() {
                    client = Some(new_client);
                }
            }

            // A message from the connected client.
            msg = async { client.as_mut().unwrap().next_message().await },
                    if client.is_some() => {
                match msg {
                    Ok(Some(msg)) => {
                        let c = client.as_mut().unwrap();
                        if let Err(e) =
                            handle_client_message(msg, c, &mut mirror, &pty_write, &mut stdout)
                                .await
                        {
                            let _ = c.send(&FromSession::Error {
                                message: format!("{e:#}"),
                            }).await;
                            drop_client(&mut client, &mut mirror, &pty_write, &mut stdout).await?;
                        }
                    }
                    // EOF or protocol garbage: drop the client.
                    Ok(None) | Err(_) => {
                        drop_client(&mut client, &mut mirror, &pty_write, &mut stdout).await?;
                    }
                }
            }

            // Child exited: drain any final output, then finish.
            status = child.wait() => {
                drain_pty(&mut pty_read, &mut mirror, &mut stdout, &mut client, &mut pty_buf)
                    .await?;
                break status?;
            }
        }
    };

    if let Some(mut c) = client.take() {
        let _ = c
            .send(&FromSession::Exit {
                status: status.code(),
            })
            .await;
    }
    stdout.write_all(&mirror.cleanup()).await?;
    stdout.flush().await?;
    drop(control); // removes the socket file
    Ok(status)
}

/// Handle one message from the session client. An error return drops the
/// client.
async fn handle_client_message(
    msg: ToSession,
    client: &mut Client,
    mirror: &mut Mirror,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    match (msg, client.state) {
        (ToSession::Init { version, device, width, height }, ClientState::AwaitingInit) => {
            anyhow::ensure!(
                version == PROTOCOL_VERSION,
                "unsupported protocol version {version} (expected {PROTOCOL_VERSION})"
            );
            anyhow::ensure!(width > 0 && height > 0, "invalid device dimensions");
            client.device = device;
            client.width = width;
            client.height = height;
            client.state = ClientState::Ready;
            Ok(())
        }
        (ToSession::Init { .. }, _) => anyhow::bail!("duplicate init"),
        (_, ClientState::AwaitingInit) => anyhow::bail!("first message must be init"),
        (ToSession::View, _) => {
            // Replaces any active view, including the simulated one and a
            // re-sent view from the same client (which just re-snapshots).
            let t = mirror.start_view(View {
                rows: client.height,
                cols: client.width,
                simulated: false,
            });
            apply_transition(&t, pty_write, stdout).await?;
            let snapshot = FromSession::Snapshot {
                data: protocol::encode_terminal_bytes(&t.remote_output.unwrap_or_default()),
            };
            client.send(&snapshot).await?;
            client.state = ClientState::Viewing;
            Ok(())
        }
        (ToSession::Unview, ClientState::Viewing) => {
            let t = mirror.end_view();
            apply_transition(&t, pty_write, stdout).await?;
            client.state = ClientState::Ready;
            Ok(())
        }
        (ToSession::Unview, ClientState::Ready) => Ok(()), // idempotent
    }
}

/// Ctrl+G: toggle the simulated device view. Ignored while a real client is
/// viewing.
async fn toggle_simulated(
    mirror: &mut Mirror,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    let t = match mirror.view() {
        None => mirror.start_view(View {
            rows: mirror::SIM_ROWS,
            cols: mirror::SIM_COLS,
            simulated: true,
        }),
        Some(v) if v.simulated => mirror.end_view(),
        Some(_) => return Ok(()),
    };
    apply_transition(&t, pty_write, stdout).await
}

/// Disconnect the session client, ending its view if it had one.
async fn drop_client(
    client: &mut Option<Client>,
    mirror: &mut Mirror,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    let was_viewing = client
        .take()
        .is_some_and(|c| c.state == ClientState::Viewing);
    if was_viewing {
        let t = mirror.end_view();
        apply_transition(&t, pty_write, stdout).await?;
    }
    Ok(())
}

async fn apply_transition(
    t: &mirror::Transition,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    if let Some((rows, cols)) = t.child_size {
        // Resizing the pty delivers SIGWINCH to the child, prompting it to
        // repaint at the new dimensions.
        pty_write.resize(pty_process::Size::new(rows, cols))?;
    }
    stdout.write_all(&t.host_output).await?;
    stdout.flush().await?;
    Ok(())
}

/// After the child exits, read whatever it wrote just before exiting,
/// delivering it to the host terminal and (best-effort) a viewing client.
async fn drain_pty(
    pty_read: &mut pty_process::OwnedReadPty,
    mirror: &mut Mirror,
    stdout: &mut tokio::io::Stdout,
    client: &mut Option<Client>,
    buf: &mut [u8],
) -> anyhow::Result<()> {
    let deadline = std::time::Duration::from_millis(50);
    while let Ok(Ok(n)) = tokio::time::timeout(deadline, pty_read.read(buf)).await {
        if n == 0 {
            break;
        }
        let out = mirror.process(&buf[..n]);
        stdout.write_all(&out.host).await?;
        if let (Some(data), Some(c)) = (out.remote, client.as_mut())
            && c.state == ClientState::Viewing && !data.is_empty() {
                let msg = FromSession::Output {
                    data: protocol::encode_terminal_bytes(&data),
                };
                if c.send(&msg).await.is_err() {
                    *client = None;
                }
            }
    }
    stdout.flush().await?;
    Ok(())
}
