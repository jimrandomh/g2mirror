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

use control::{BellDebouncer, Client, ClientState, ControlListener};
use mirror::{Mirror, View};

/// Ctrl+G: simulate a device connect/disconnect.
const HOTKEY: u8 = 0x07;

/// Bell notifications are debounced to at most one per this window.
const BELL_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(3);

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

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
    // Connection slots: a freshly accepted connection is `pending` until its
    // first message classifies it as the viewer (a device, via init) or the
    // monitor (g2mirror-server, via monitor). One of each at a time; the
    // monitor does not count as a viewer.
    let mut pending: Option<Client> = None;
    let mut viewer: Option<Client> = None;
    let mut monitor: Option<Client> = None;
    let mut bell = BellDebouncer::new(BELL_DEBOUNCE);
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

            // Child output: translate through the mirror; repaint the host,
            // stream to a viewing client, report bells to the monitor.
            n = pty_read.read(&mut pty_buf) => match n {
                // EOF/EIO on the pty master means the child side is gone.
                Ok(0) | Err(_) => break child.wait().await?,
                Ok(n) => {
                    let out = mirror.process(&pty_buf[..n]);
                    stdout.write_all(&out.host).await?;
                    stdout.flush().await?;
                    if out.bells > 0 && monitor.is_some()
                        && let Some(at) = bell.on_bell(std::time::Instant::now(), now_ms()) {
                            send_bell(&mut monitor, at).await;
                        }
                    if let (Some(data), Some(c)) = (out.remote, viewer.as_mut())
                        && c.state == ClientState::Viewing && !data.is_empty() {
                            let msg = FromSession::Output {
                                data: protocol::encode_terminal_bytes(&data),
                            };
                            if c.send(&msg).await.is_err() {
                                drop_viewer(&mut viewer, &mut mirror, &pty_write, &mut stdout)
                                    .await?;
                            }
                        }
                    if let Some(title) = out.title {
                        send_title(&mut monitor, &title).await;
                        if let Some(c) = viewer.as_mut()
                            && c.send(&FromSession::Title { title }).await.is_err() {
                                drop_viewer(&mut viewer, &mut mirror, &pty_write, &mut stdout)
                                    .await?;
                            }
                    }
                }
            },

            // A bell held by the debounce window is due to be reported.
            _ = async {
                tokio::time::sleep_until(
                    tokio::time::Instant::from_std(bell.deadline().unwrap()),
                )
                .await
            }, if bell.deadline().is_some() => {
                if let Some(at) = bell.fire(std::time::Instant::now()) {
                    send_bell(&mut monitor, at).await;
                }
            }

            // Host terminal resized.
            _ = winch.recv() => {
                let (rows, cols) = host_size();
                let t = mirror.host_resized(rows, cols);
                apply_transition(&t, &pty_write, &mut stdout).await?;
            }

            // A new connection: greet it and wait for its first message.
            conn = control.accept() => {
                let stream = conn.context("session socket accept failed")?;
                let mut new_client = Client::new(stream);
                if pending.is_some() {
                    let _ = new_client
                        .send(&FromSession::Error {
                            message: "another connection is being set up; retry".into(),
                        })
                        .await;
                    // new_client dropped, connection closes.
                } else {
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
                    if new_client.send(&connect).await.is_ok() {
                        pending = Some(new_client);
                    }
                }
            }

            // A pending connection's first message classifies it.
            msg = async { pending.as_mut().unwrap().next_message().await },
                    if pending.is_some() => {
                let mut p = pending.take().unwrap();
                match msg {
                    Ok(Some(ToSession::Init { version, device, width, height })) => {
                        let reject = if version != PROTOCOL_VERSION {
                            Some(format!(
                                "unsupported protocol version {version} \
                                 (expected {PROTOCOL_VERSION})"
                            ))
                        } else if width == 0 || height == 0 {
                            Some("invalid device dimensions".into())
                        } else if viewer.is_some() {
                            Some("another client is already connected".into())
                        } else {
                            None
                        };
                        match reject {
                            Some(message) => {
                                let _ = p.send(&FromSession::Error { message }).await;
                            }
                            None => {
                                p.device = device;
                                p.width = width;
                                p.height = height;
                                p.state = ClientState::Ready;
                                viewer = Some(p);
                                if let Some(t) = mirror.title().map(str::to_string) {
                                    send_title(&mut viewer, &t).await;
                                }
                            }
                        }
                    }
                    Ok(Some(ToSession::Monitor { version })) => {
                        if version == PROTOCOL_VERSION {
                            // Replaces a previous monitor (e.g. after a
                            // server restart whose old connection hasn't
                            // been noticed as dead yet).
                            monitor = Some(p);
                            if let Some(t) = mirror.title().map(str::to_string) {
                                send_title(&mut monitor, &t).await;
                            }
                        } else {
                            let _ = p.send(&FromSession::Error {
                                message: format!("unsupported protocol version {version}"),
                            }).await;
                        }
                    }
                    Ok(Some(_)) => {
                        let _ = p.send(&FromSession::Error {
                            message: "first message must be init or monitor".into(),
                        }).await;
                    }
                    // EOF or garbage: connection dropped.
                    Ok(None) | Err(_) => {}
                }
            }

            // A message from the viewer.
            msg = async { viewer.as_mut().unwrap().next_message().await },
                    if viewer.is_some() => {
                match msg {
                    Ok(Some(msg)) => {
                        let c = viewer.as_mut().unwrap();
                        if let Err(e) =
                            handle_viewer_message(msg, c, &mut mirror, &pty_write, &mut stdout)
                                .await
                        {
                            let _ = c.send(&FromSession::Error {
                                message: format!("{e:#}"),
                            }).await;
                            drop_viewer(&mut viewer, &mut mirror, &pty_write, &mut stdout).await?;
                        }
                    }
                    // EOF or protocol garbage: drop the viewer.
                    Ok(None) | Err(_) => {
                        drop_viewer(&mut viewer, &mut mirror, &pty_write, &mut stdout).await?;
                    }
                }
            }

            // Monitors don't speak after their first message; poll only to
            // notice hangups (ignoring anything else).
            msg = async { monitor.as_mut().unwrap().next_message().await },
                    if monitor.is_some() => {
                if !matches!(msg, Ok(Some(_))) {
                    monitor = None;
                }
            }

            // Child exited: drain any final output, then finish.
            status = child.wait() => {
                drain_pty(&mut pty_read, &mut mirror, &mut stdout, &mut viewer, &mut pty_buf)
                    .await?;
                break status?;
            }
        }
    };

    if let Some(mut c) = viewer.take() {
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

/// Report a bell to the monitor connection, dropping it if the send fails.
async fn send_bell(monitor: &mut Option<Client>, at: u64) {
    if let Some(m) = monitor.as_mut()
        && m.send(&FromSession::Bell { at }).await.is_err()
    {
        *monitor = None;
    }
}

/// Report a title to a connection, dropping it if the send fails. (Safe for
/// connections without an active view; the viewer's mid-view send failures
/// are handled by `drop_viewer` at the call sites that need it.)
async fn send_title(conn: &mut Option<Client>, title: &str) {
    if let Some(c) = conn.as_mut()
        && c.send(&FromSession::Title {
            title: title.to_string(),
        })
        .await
        .is_err()
    {
        *conn = None;
    }
}

/// Handle one message from the viewer (already past init). An error return
/// drops the viewer.
async fn handle_viewer_message(
    msg: ToSession,
    client: &mut Client,
    mirror: &mut Mirror,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    match (msg, client.state) {
        (ToSession::Init { .. }, _) => anyhow::bail!("duplicate init"),
        (ToSession::Monitor { .. }, _) => anyhow::bail!("already initialized as a viewer"),
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
        (ToSession::Unview, _) => Ok(()), // idempotent
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

/// Disconnect the viewer, ending its view if it had one.
async fn drop_viewer(
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
