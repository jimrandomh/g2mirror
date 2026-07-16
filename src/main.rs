//! g2mirror: run a CLI app in a pty and mirror it to Even Realities G2
//! smart glasses.
//!
//! The wrapped app's output goes to the host terminal, and — while a device
//! is viewing — is also parsed into a vt100 screen model at the device's
//! dimensions and streamed to the device over a unix domain socket session
//! (see PROTOCOL.md). Ctrl+G simulates a device connect/disconnect at 96x24
//! without needing a real client.

mod control;

use std::os::unix::process::ExitStatusExt as _;
use std::process::ExitStatus;

use anyhow::Context as _;
use g2mirror::protocol::{self, FromSession, HistoryExtent, HistoryLine, ToSession, PROTOCOL_VERSION};
use g2mirror::{history, mirror, raw_guard};
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

fn usage() -> ! {
    eprintln!(
        "usage: g2mirror [--title <title>] [--readonly] [--scrollback <lines>] <command> [args...]"
    );
    eprintln!("  --title       initial window title, until the program sets one itself");
    eprintln!("  --readonly    reject input from connected devices");
    eprintln!(
        "  --scrollback  history lines retained for devices (default {})",
        history::DEFAULT_MAX_LINES
    );
    eprintln!("  Ctrl+G simulates glasses connect/disconnect");
    std::process::exit(2);
}

fn main() {
    let mut args = std::env::args_os().skip(1).peekable();
    let mut title: Option<String> = None;
    let mut readonly = false;
    let mut scrollback = history::DEFAULT_MAX_LINES;
    let program = loop {
        let Some(arg) = args.next() else { usage() };
        match arg.to_str() {
            Some("--title") => match args.next() {
                Some(value) => title = Some(value.to_string_lossy().into_owned()),
                None => {
                    eprintln!("g2mirror: --title requires a value");
                    usage();
                }
            },
            Some(s) if s.starts_with("--title=") => {
                title = Some(s["--title=".len()..].to_string());
            }
            Some("--readonly") => readonly = true,
            Some("--scrollback") => match args.next().and_then(|v| v.to_str()?.parse().ok()) {
                Some(lines) => scrollback = lines,
                None => {
                    eprintln!("g2mirror: --scrollback requires a number of lines");
                    usage();
                }
            },
            Some("--") => match args.next() {
                Some(program) => break program,
                None => usage(),
            },
            Some(s) if s.starts_with('-') && s.len() > 1 => {
                eprintln!("g2mirror: unknown option {s}");
                usage();
            }
            _ => break arg,
        }
    };
    let args: Vec<_> = args.collect();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    match runtime.block_on(run(program, args, title, readonly, scrollback)) {
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
    // A pty can report 0x0 (e.g. under `script` without a real terminal);
    // fall back rather than running the mirror at degenerate dimensions.
    match rustix::termios::tcgetwinsize(rustix::stdio::stdout()) {
        Ok(ws) if ws.ws_row > 0 && ws.ws_col > 0 => (ws.ws_row, ws.ws_col),
        _ => (24, 80),
    }
}

async fn run(
    program: std::ffi::OsString,
    args: Vec<std::ffi::OsString>,
    title: Option<String>,
    readonly: bool,
    scrollback: usize,
) -> anyhow::Result<ExitStatus> {
    let (host_rows, host_cols) = host_size();

    // Point the user at server setup while the screen is still ours; a
    // fullscreen child will repaint over it, but it shows before launch.
    if let Ok(dir) = g2mirror::paths::g2mirror_dir() {
        let config = g2mirror::paths::config_path(&dir);
        if !config.exists() {
            eprintln!(
                "g2mirror: {} not found; to enable device connections, run \
                 `g2mirror-server --init-config` and then `g2mirror-server`",
                config.display()
            );
        }
    }

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
    mirror.set_history_limit(scrollback);
    if let Some(t) = title {
        // Show it on the host terminal too; strip control characters so an
        // exotic title can't break out of the escape sequence.
        let clean: String = t.chars().filter(|c| !c.is_control()).collect();
        stdout
            .write_all(format!("\x1b]2;{clean}\x07").as_bytes())
            .await?;
        stdout.flush().await?;
        mirror.set_title(clean);
    }
    // Connection slots: a freshly accepted connection is pending until its
    // first message classifies it as a viewer (a device, via init) or the
    // monitor (g2mirror-server, via monitor; at most one, and it does not
    // count as a viewer). Several viewers may be connected and viewing at
    // once; the wrapped app is sized to the best-ranked viewing client (or
    // left at host size when the host outranks them all), and everyone gets
    // the same output stream at that size.
    let mut pendings: Vec<Client> = Vec::new();
    let mut viewers: Vec<Client> = Vec::new();
    let mut monitor: Option<Client> = None;
    // Rank of the host terminal in the size-precedence order, as reported
    // by the most recent init; until one says otherwise, viewers outrank
    // the host.
    let mut host_rank: u32 = u32::MAX;
    let mut next_client_id: u64 = 0;
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
                    let mut lost_one = false;
                    if let Some(data) = out.remote
                        && !data.is_empty() {
                            let msg = FromSession::Output {
                                data: protocol::encode_terminal_bytes(&data),
                            };
                            for c in viewers.iter_mut()
                                .filter(|c| c.state == ClientState::Viewing) {
                                if c.send(&msg).await.is_err() {
                                    c.dead = true;
                                    lost_one = true;
                                }
                            }
                        }
                    if let Some(title) = out.title {
                        send_title(&mut monitor, &title).await;
                        let msg = FromSession::Title { title };
                        for c in viewers.iter_mut() {
                            if c.send(&msg).await.is_err() {
                                c.dead = true;
                                lost_one = true;
                            }
                        }
                    }
                    if lost_one {
                        sweep_and_refresh(
                            &mut viewers, &mut mirror, host_rank, &pty_write, &mut stdout,
                        ).await?;
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
                // If the host outranks the viewers, the view size follows.
                sweep_and_refresh(&mut viewers, &mut mirror, host_rank, &pty_write, &mut stdout)
                    .await?;
            }

            // A new connection: greet it and wait for its first message.
            conn = control.accept() => {
                let stream = conn.context("session socket accept failed")?;
                let mut new_client = Client::new(stream, next_client_id);
                next_client_id += 1;
                let connect = FromSession::Connect {
                    version: PROTOCOL_VERSION,
                    pid: std::process::id(),
                    command: command_line.clone(),
                    cwd: std::env::current_dir()
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    host_width: host_size().1,
                    host_height: host_size().0,
                    readonly,
                    history: {
                        let (next, oldest) = mirror.history_extent();
                        HistoryExtent { next, oldest }
                    },
                };
                if new_client.send(&connect).await.is_ok() {
                    pendings.push(new_client);
                }
            }

            // A pending connection's first message classifies it.
            (i, msg) = next_from_any(&mut pendings), if !pendings.is_empty() => {
                let mut p = pendings.remove(i);
                match msg {
                    Ok(Some(ToSession::Init {
                        version, device, width, height, size_rank, host_size_rank,
                    })) => {
                        let reject = if version != PROTOCOL_VERSION {
                            Some(format!(
                                "unsupported protocol version {version} \
                                 (expected {PROTOCOL_VERSION})"
                            ))
                        } else if width == 0 || height == 0 {
                            Some("invalid device dimensions".into())
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
                                p.size_rank = size_rank.unwrap_or(0);
                                if let Some(rank) = host_size_rank {
                                    host_rank = rank;
                                }
                                p.state = ClientState::Ready;
                                let mut alive = true;
                                if let Some(t) = mirror.title().map(str::to_string) {
                                    alive = p.send(&FromSession::Title { title: t })
                                        .await
                                        .is_ok();
                                }
                                if alive {
                                    viewers.push(p);
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

            // A message from one of the viewers.
            (i, msg) = next_from_any(&mut viewers), if !viewers.is_empty() => {
                match msg {
                    Ok(Some(msg)) => {
                        if let Err(e) = handle_viewer_message(
                            msg, i, &mut viewers, &mut mirror, host_rank,
                            &mut pty_write, &mut stdout, readonly,
                        )
                        .await
                        {
                            let _ = viewers[i].send(&FromSession::Error {
                                message: format!("{e:#}"),
                            }).await;
                            viewers[i].dead = true;
                        }
                    }
                    // EOF or protocol garbage: drop the viewer.
                    Ok(None) | Err(_) => viewers[i].dead = true,
                }
                sweep_and_refresh(&mut viewers, &mut mirror, host_rank, &pty_write, &mut stdout)
                    .await?;
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
                drain_pty(&mut pty_read, &mut mirror, &mut stdout, &mut viewers, &mut pty_buf)
                    .await?;
                break status?;
            }
        }
    };

    for mut c in viewers.drain(..) {
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

/// Wait for the next message from any of `clients` (must be non-empty).
/// Cancel-safe: each client's partially read input stays buffered in it.
async fn next_from_any(clients: &mut [Client]) -> (usize, anyhow::Result<Option<ToSession>>) {
    let futures = clients
        .iter_mut()
        .enumerate()
        .map(|(i, c)| Box::pin(async move { (i, c.next_message().await) }));
    futures_util::future::select_all(futures).await.0
}

/// Re-decide the view from the size-precedence ranks of everyone currently
/// viewing (against the host terminal's own rank) and apply the outcome:
/// start/restart the view at the winning dimensions, or end it when nobody
/// views anymore. When the dimensions change, every viewing client gets a
/// fresh snapshot (their streams restart at the new size); `force_for`
/// additionally restarts at unchanged dimensions and snapshots just that
/// client (used when it sent `view`, so it needs a snapshot in any case —
/// the rebuilt stream stays seamless for the others because the model is
/// re-primed with its own current state).
///
/// Send failures mark clients dead without removing them (callers may hold
/// indices); `sweep_and_refresh` is the removal point.
async fn refresh_view(
    viewers: &mut [Client],
    mirror: &mut Mirror,
    host_rank: u32,
    force_for: Option<usize>,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    let best = viewers
        .iter()
        .filter(|c| !c.dead && c.state == ClientState::Viewing)
        .min_by_key(|c| (c.size_rank, c.id));
    let target = best.map(|c| {
        if c.size_rank <= host_rank {
            (c.height, c.width)
        } else {
            mirror.host_size()
        }
    });
    match target {
        None => {
            // Nobody is viewing; a hotkey-simulated view is not ours to end.
            if mirror.view().is_some_and(|v| !v.simulated) {
                let t = mirror.end_view();
                apply_transition(&t, pty_write, stdout).await?;
            }
        }
        Some((rows, cols)) => {
            let changed = mirror
                .view()
                .is_none_or(|v| v.simulated || (v.rows, v.cols) != (rows, cols));
            if changed || force_for.is_some() {
                let t = mirror.start_view(View {
                    rows,
                    cols,
                    simulated: false,
                });
                apply_transition(&t, pty_write, stdout).await?;
                let snapshot = FromSession::Snapshot {
                    data: protocol::encode_terminal_bytes(&t.remote_output.unwrap_or_default()),
                    width: cols,
                    height: rows,
                    // Everything archived so far (including rows just
                    // flushed by the view-start crop) predates this
                    // snapshot.
                    history_next: mirror.history_extent().0,
                };
                for (i, c) in viewers
                    .iter_mut()
                    .enumerate()
                    .filter(|(_, c)| !c.dead && c.state == ClientState::Viewing)
                {
                    if (changed || force_for == Some(i)) && c.send(&snapshot).await.is_err() {
                        c.dead = true;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Remove dead clients and re-decide the view, repeating until no further
/// client dies during the snapshot broadcasts.
async fn sweep_and_refresh(
    viewers: &mut Vec<Client>,
    mirror: &mut Mirror,
    host_rank: u32,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
) -> anyhow::Result<()> {
    loop {
        viewers.retain(|c| !c.dead);
        refresh_view(viewers, mirror, host_rank, None, pty_write, stdout).await?;
        if viewers.iter().all(|c| !c.dead) {
            return Ok(());
        }
    }
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

/// Handle one message from viewer `i` (already past init). An error return
/// drops that viewer. May mark other viewers dead (snapshot broadcasts) but
/// never removes entries, so `i` stays valid; the caller sweeps afterwards.
#[allow(clippy::too_many_arguments)]
async fn handle_viewer_message(
    msg: ToSession,
    i: usize,
    viewers: &mut [Client],
    mirror: &mut Mirror,
    host_rank: u32,
    pty_write: &mut pty_process::OwnedWritePty,
    stdout: &mut tokio::io::Stdout,
    readonly: bool,
) -> anyhow::Result<()> {
    match (msg, viewers[i].state) {
        (ToSession::Init { .. }, _) => anyhow::bail!("duplicate init"),
        (ToSession::Monitor { .. }, _) => anyhow::bail!("already initialized as a viewer"),
        (ToSession::Input { data }, _) => {
            if readonly {
                // Reject without dropping the connection: a read-only
                // session is a policy answer, not a protocol violation.
                viewers[i]
                    .send(&FromSession::Error {
                        message: "session is read-only".into(),
                    })
                    .await?;
                return Ok(());
            }
            let bytes = protocol::decode_terminal_bytes(&data)
                .map_err(|e| anyhow::anyhow!("invalid input encoding: {e}"))?;
            pty_write.write_all(&bytes).await?;
            Ok(())
        }
        (ToSession::View, _) => {
            // The refresh restarts the view unconditionally, so this client
            // gets its snapshot even when the winning dimensions are
            // unchanged (and a re-sent view still re-snapshots); the other
            // viewers are re-snapshotted only if the dimensions changed.
            viewers[i].state = ClientState::Viewing;
            refresh_view(viewers, mirror, host_rank, Some(i), pty_write, stdout).await
        }
        (ToSession::History { before, limit }, _) => {
            let limit = limit
                .unwrap_or(history::DEFAULT_FETCH_LIMIT)
                .min(history::DEFAULT_FETCH_LIMIT);
            let (start, records) = mirror.history().fetch(before, limit);
            let (next, oldest) = mirror.history_extent();
            let lines = records
                .into_iter()
                .map(|r| HistoryLine {
                    data: protocol::encode_terminal_bytes(&r.bytes),
                    width: r.width,
                    wrapped: r.wrapped,
                })
                .collect();
            viewers[i]
                .send(&FromSession::HistoryLines {
                    start,
                    oldest,
                    next,
                    lines,
                })
                .await?;
            Ok(())
        }
        (ToSession::Unview, ClientState::Viewing) => {
            viewers[i].state = ClientState::Ready;
            refresh_view(viewers, mirror, host_rank, None, pty_write, stdout).await
        }
        (ToSession::Unview, _) => Ok(()), // idempotent
    }
}

/// Ctrl+G: toggle the simulated device view. Ignored while a real client is
/// viewing (a real view exists exactly when a non-simulated view is active).
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
/// delivering it to the host terminal and (best-effort) the viewing clients.
async fn drain_pty(
    pty_read: &mut pty_process::OwnedReadPty,
    mirror: &mut Mirror,
    stdout: &mut tokio::io::Stdout,
    viewers: &mut Vec<Client>,
    buf: &mut [u8],
) -> anyhow::Result<()> {
    let deadline = std::time::Duration::from_millis(50);
    while let Ok(Ok(n)) = tokio::time::timeout(deadline, pty_read.read(buf)).await {
        if n == 0 {
            break;
        }
        let out = mirror.process(&buf[..n]);
        stdout.write_all(&out.host).await?;
        if let Some(data) = out.remote
            && !data.is_empty() {
                let msg = FromSession::Output {
                    data: protocol::encode_terminal_bytes(&data),
                };
                for c in viewers.iter_mut().filter(|c| c.state == ClientState::Viewing) {
                    if c.send(&msg).await.is_err() {
                        c.dead = true;
                    }
                }
                viewers.retain(|c| !c.dead);
            }
    }
    stdout.flush().await?;
    Ok(())
}
