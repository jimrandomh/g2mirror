//! g2mirror: run a CLI app in a pty and mirror it to Even Realities G2
//! smart glasses.
//!
//! The wrapped app's output goes to the host terminal, and — while a device
//! is viewing — is also parsed into a vt100 screen model at the device's
//! dimensions and streamed to the device over a unix domain socket session
//! (see PROTOCOL.md). Ctrl+G simulates a device connect/disconnect at 96x24
//! without needing a real client.

mod attach;
mod control;

use std::os::unix::process::ExitStatusExt as _;
use std::process::ExitStatus;

use anyhow::Context as _;
use g2mirror::protocol::{
    self, FromSession, HistoryExtent, HistoryLine, Role, ToSession, PROTOCOL_VERSION,
};
use g2mirror::{history, mirror, raw_guard};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::signal::unix::{signal, SignalKind};

use control::{BellDebouncer, Client, ClientState, ControlListener};
use mirror::{Mirror, View};

/// Ctrl+G: simulate a device connect/disconnect.
const HOTKEY: u8 = 0x07;

/// Ctrl+L (also forwarded to the child): while a view taller than the host
/// terminal is active, re-push the hidden top rows into the host's native
/// scrollback and repaint.
const REFRESH_KEY: u8 = 0x0c;

/// Bell notifications are debounced to at most one per this window.
const BELL_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(3);

/// Caps on client-requested input delays: each pause is clamped to this
/// many milliseconds, and a single `input` message may carry at most this
/// many delay entries.
const MAX_INPUT_DELAY_MS: u64 = 1000;
const MAX_INPUT_DELAYS: usize = 32;

/// Input bytes waiting on a client-requested pause (the `input` message's
/// `delays` field). Chunks are written to the pty from the main loop when
/// their pause elapses, so a pause never stalls output mirroring; while the
/// queue is non-empty all further input is appended behind it, keeping
/// bytes in the order the client sent them.
#[derive(Default)]
struct InputQueue {
    /// (pause before writing, bytes to write).
    queue: std::collections::VecDeque<(std::time::Duration, Vec<u8>)>,
    /// When the head chunk may be written; set by `pump` when it starts the
    /// head's pause, so it is `Some` whenever a pause is actually running.
    due: Option<tokio::time::Instant>,
}

impl InputQueue {
    /// Split `bytes` at the delay offsets (validated by the caller as
    /// non-decreasing and within bounds) and append the chunks.
    fn push(&mut self, bytes: Vec<u8>, delays: &[protocol::InputDelay]) {
        let mut prev = 0usize;
        let mut pause = std::time::Duration::ZERO;
        for d in delays {
            let at = usize::try_from(d.at).unwrap_or(bytes.len());
            self.queue.push_back((pause, bytes[prev..at].to_vec()));
            pause = std::time::Duration::from_millis(d.ms.min(MAX_INPUT_DELAY_MS));
            prev = at;
        }
        self.queue.push_back((pause, bytes[prev..].to_vec()));
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.due
    }

    /// Write every chunk whose pause has elapsed, then start the next
    /// pending pause (if any) and record its deadline.
    async fn pump(&mut self, pty_write: &mut pty_process::OwnedWritePty) -> anyhow::Result<()> {
        while let Some((pause, _)) = self.queue.front() {
            let now = tokio::time::Instant::now();
            let due = *self.due.get_or_insert(now + *pause);
            if due > now {
                return Ok(());
            }
            let (_, bytes) = self.queue.pop_front().unwrap();
            self.due = None;
            if !bytes.is_empty() {
                pty_write.write_all(&bytes).await?;
            }
        }
        Ok(())
    }
}

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
    eprintln!("       g2mirror --detached [same options] <command> [args...]");
    eprintln!("       g2mirror --list | -l");
    eprintln!("       g2mirror --attach | -a [pattern] [--force] [--detach-key <key>]");
    eprintln!("  --title       initial window title, until the program sets one itself");
    eprintln!("  --readonly    reject input from connected devices");
    eprintln!(
        "  --scrollback  history lines retained for devices (default {})",
        history::DEFAULT_MAX_LINES
    );
    eprintln!("  --detached    run without a terminal; claim later with --attach");
    eprintln!("  --list        list live sessions (detached ones first)");
    eprintln!("  --attach      give a detached session this terminal; the pattern");
    eprintln!("                matches pid, title, or cwd. Ctrl+\\ detaches again");
    eprintln!("  Ctrl+G simulates glasses connect/disconnect");
    eprintln!("  Ctrl+L (passed through) refreshes mirrored scrollback while viewing");
    std::process::exit(2);
}

/// Options for wrapping a command (`g2mirror [flags] <command>`), whether
/// hosted in this terminal, headless, or being daemonized via `--detached`.
struct WrapOpts {
    program: std::ffi::OsString,
    args: Vec<std::ffi::OsString>,
    title: Option<String>,
    readonly: bool,
    scrollback: usize,
    /// Run without a host terminal: no raw mode, no stdin, no host
    /// rendering; the session lives until the command exits and is
    /// claimable with `--attach`.
    headless: bool,
    /// Headless pty size until the first viewer resizes it: (rows, cols).
    initial_size: (u16, u16),
    /// Server launch preset name, reported in the connect greeting.
    launched: Option<String>,
}

fn main() {
    let argv: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    match argv.first().and_then(|a| a.to_str()) {
        Some("--list" | "-l") => {
            if argv.len() > 1 {
                eprintln!("g2mirror: --list takes no further arguments");
                usage();
            }
            run_blocking(attach::list_sessions())
        }
        Some("--attach" | "-a") => {
            let opts = attach::parse_attach_args(&argv[1..]).unwrap_or_else(|e| {
                eprintln!("g2mirror: {e:#}");
                usage();
            });
            run_blocking(attach::attach(opts))
        }
        _ => {}
    }

    let mut args = argv.into_iter().peekable();
    let mut title: Option<String> = None;
    let mut readonly = false;
    let mut scrollback = history::DEFAULT_MAX_LINES;
    let mut headless = false;
    let mut detached = false;
    let mut initial_size = (24u16, 80u16);
    let mut launched: Option<String> = None;
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
            Some("--headless") => headless = true,
            Some("--detached") => detached = true,
            Some("--initial-size") => {
                match args.next().and_then(|v| parse_size(v.to_str()?)) {
                    Some(size) => initial_size = size,
                    None => {
                        eprintln!("g2mirror: --initial-size requires <cols>x<rows>, e.g. 80x24");
                        usage();
                    }
                }
            }
            Some("--launched") => match args.next() {
                Some(value) => launched = Some(value.to_string_lossy().into_owned()),
                None => {
                    eprintln!("g2mirror: --launched requires a preset name");
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
    let opts = WrapOpts {
        program,
        args: args.collect(),
        title,
        readonly,
        scrollback,
        headless,
        initial_size,
        launched,
    };

    if detached {
        match spawn_detached(&opts) {
            Ok(()) => std::process::exit(0),
            Err(e) => {
                eprintln!("g2mirror: {e:#}");
                std::process::exit(1);
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    match runtime.block_on(run(opts)) {
        Ok(status) => std::process::exit(exit_code(status)),
        Err(e) => {
            eprintln!("g2mirror: {e:#}");
            std::process::exit(1);
        }
    }
}

/// Run a future to completion on a fresh runtime and exit with its code
/// (the `--list`/`--attach` entry points).
fn run_blocking(fut: impl std::future::Future<Output = anyhow::Result<i32>>) -> ! {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    match runtime.block_on(fut) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("g2mirror: {e:#}");
            std::process::exit(1);
        }
    }
}

/// `<cols>x<rows>` (the order sizes are usually spoken in) -> (rows, cols).
fn parse_size(s: &str) -> Option<(u16, u16)> {
    let (cols, rows) = s.split_once('x')?;
    let (cols, rows): (u16, u16) = (cols.parse().ok()?, rows.parse().ok()?);
    (cols > 0 && rows > 0).then_some((rows, cols))
}

/// `--detached`: start a headless copy of ourselves running the command,
/// in its own session (setsid) so it survives this shell and any parent
/// (e.g. g2mirror-server) exiting. Prints the session socket name on
/// stdout — parsed by g2mirror-server's launch path; keep the format
/// stable — and a human hint on stderr.
fn spawn_detached(opts: &WrapOpts) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("cannot find own executable")?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--headless");
    if let Some(t) = &opts.title {
        cmd.arg("--title").arg(t);
    }
    if opts.readonly {
        cmd.arg("--readonly");
    }
    cmd.arg("--scrollback").arg(opts.scrollback.to_string());
    let (rows, cols) = opts.initial_size;
    cmd.arg("--initial-size").arg(format!("{cols}x{rows}"));
    if let Some(l) = &opts.launched {
        cmd.arg("--launched").arg(l);
    }
    cmd.arg("--").arg(&opts.program).args(&opts.args);
    // stderr stays piped so a child that dies during startup (e.g. failing
    // to bind its socket) can be reported instead of silently vanishing.
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    unsafe {
        use std::os::unix::process::CommandExt as _;
        cmd.pre_exec(|| {
            rustix::process::setsid()?;
            Ok(())
        });
    }
    let mut child = cmd.spawn().context("failed to spawn headless wrapper")?;
    // getcwd is symlink-free, so this matches the socket name the child
    // computes for itself.
    let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("/"));
    let socket = g2mirror::paths::socket_name(child.id(), &cwd);
    let socket_path = g2mirror::paths::g2mirror_dir()?.join(&socket);
    // Only report success once the session socket exists; a launch that
    // prints a socket name nothing will ever answer on helps nobody.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if socket_path.exists() {
            break;
        }
        if let Some(status) = child.try_wait()? {
            use std::io::Read as _;
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            anyhow::bail!(
                "headless wrapper exited during startup ({status}): {}",
                stderr.trim()
            );
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "headless wrapper did not create its session socket in time"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    println!("{socket}");
    eprintln!(
        "g2mirror: detached session started (pid {}); attach with: g2mirror -a {}",
        child.id(),
        child.id()
    );
    Ok(())
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

/// The wrapper's own terminal. In headless mode there is none: writes are
/// discarded, while the mirror still computes host output (which keeps its
/// model and history archive exactly as in hosted mode).
struct HostTerm {
    stdout: tokio::io::Stdout,
    enabled: bool,
}

impl HostTerm {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.enabled && !bytes.is_empty() {
            self.stdout.write_all(bytes).await?;
            self.stdout.flush().await?;
        }
        Ok(())
    }
}

async fn run(opts: WrapOpts) -> anyhow::Result<ExitStatus> {
    let WrapOpts {
        program,
        args,
        title,
        readonly,
        scrollback,
        headless,
        initial_size,
        launched,
    } = opts;
    let (host_rows, host_cols) = if headless { initial_size } else { host_size() };

    // Point the user at server setup while the screen is still ours; a
    // fullscreen child will repaint over it, but it shows before launch.
    if !headless && let Ok(dir) = g2mirror::paths::g2mirror_dir() {
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
    // stdin isn't a tty (e.g. tests, pipes, headless), run without it.
    let _raw = if !headless && rustix::termios::isatty(rustix::stdio::stdin()) {
        Some(raw_guard::RawGuard::new().context("failed to enter raw mode")?)
    } else {
        None
    };

    let (mut pty_read, mut pty_write) = pty.into_split();
    let mut stdin = tokio::io::stdin();
    let mut host = HostTerm {
        stdout: tokio::io::stdout(),
        enabled: !headless,
    };
    let mut winch = signal(SignalKind::window_change())?;

    let mut mirror = Mirror::new(host_rows, host_cols);
    mirror.set_history_limit(scrollback);
    if let Some(t) = title {
        // Show it on the host terminal too; strip control characters so an
        // exotic title can't break out of the escape sequence.
        let clean: String = t.chars().filter(|c| !c.is_control()).collect();
        host.write(format!("\x1b]2;{clean}\x07").as_bytes()).await?;
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
    // Whether a host-role client currently holds a headless session's host
    // slot; flips are reported to the monitor and viewers (host_changed).
    let mut host_attached = false;
    let mut next_client_id: u64 = 0;
    let mut bell = BellDebouncer::new(BELL_DEBOUNCE);
    let mut input_queue = InputQueue::default();
    let mut stdin_buf = [0u8; 4096];
    let mut pty_buf = [0u8; 64 * 1024];
    let mut stdin_open = !headless;

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
                        toggle_simulated(&mut mirror, &pty_write, &mut host).await?;
                        rest = &rest[pos + 1..];
                    }
                    pty_write.write_all(rest).await?;
                    if stdin_buf[..n].contains(&REFRESH_KEY) {
                        // Ctrl+L (already forwarded above): refresh the
                        // mirrored scrollback and repaint while viewing.
                        let out = mirror.refresh_scrollback();
                        host.write(&out).await?;
                    }
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
                    host.write(&out.host).await?;
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
                            &mut viewers, &mut monitor, &mut mirror, host_rank, headless,
                            &mut host_attached, &pty_write, &mut host,
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

            // A queued input chunk's client-requested pause has elapsed.
            _ = async {
                tokio::time::sleep_until(input_queue.deadline().unwrap()).await
            }, if input_queue.deadline().is_some() => {
                input_queue.pump(&mut pty_write).await?;
            }

            // Host terminal resized.
            _ = winch.recv(), if !headless => {
                let (rows, cols) = host_size();
                let t = mirror.host_resized(rows, cols);
                apply_transition(&t, &pty_write, &mut host).await?;
                // If the host outranks the viewers, the view size follows.
                sweep_and_refresh(
                    &mut viewers, &mut monitor, &mut mirror, host_rank, headless,
                    &mut host_attached, &pty_write, &mut host,
                ).await?;
            }

            // A new connection: greet it and wait for its first message.
            conn = control.accept() => {
                let stream = conn.context("session socket accept failed")?;
                let mut new_client = Client::new(stream, next_client_id);
                next_client_id += 1;
                // In headless mode the "host" dimensions are the mirror's
                // current model size (the last view size, or the initial
                // size before any view).
                let (greet_rows, greet_cols) = if headless {
                    mirror.host_size()
                } else {
                    host_size()
                };
                let connect = FromSession::Connect {
                    version: PROTOCOL_VERSION,
                    pid: std::process::id(),
                    command: command_line.clone(),
                    cwd: std::env::current_dir()
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    host_width: greet_cols,
                    host_height: greet_rows,
                    readonly,
                    history: {
                        let (next, oldest) = mirror.history_extent();
                        HistoryExtent { next, oldest }
                    },
                    title: mirror.title().map(str::to_string),
                    headless,
                    detached: headless && !host_attached,
                    launched: launched.clone(),
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
                        version, device, width, height, size_rank, host_size_rank, role, force,
                    })) => {
                        let host_role_held = viewers
                            .iter()
                            .any(|c| !c.dead && c.role == Role::Host);
                        let reject = if version != PROTOCOL_VERSION {
                            Some(format!(
                                "unsupported protocol version {version} \
                                 (expected {PROTOCOL_VERSION})"
                            ))
                        } else if width == 0 || height == 0 {
                            Some("invalid device dimensions".into())
                        } else if role == Role::Host && !headless {
                            Some(
                                "this session has a real host terminal; \
                                 use g2mirror-view to watch it".into(),
                            )
                        } else if role == Role::Host && host_role_held && !force {
                            Some(
                                "session is already attached; \
                                 g2mirror --attach --force takes it over".into(),
                            )
                        } else {
                            None
                        };
                        match reject {
                            Some(message) => {
                                let _ = p.send(&FromSession::Error { message }).await;
                            }
                            None => {
                                if role == Role::Host && host_role_held {
                                    // --force: displace the current holder.
                                    for c in viewers
                                        .iter_mut()
                                        .filter(|c| c.role == Role::Host)
                                    {
                                        let _ = c.send(&FromSession::Error {
                                            message: "host role taken over \
                                                      by another attach client".into(),
                                        }).await;
                                        c.dead = true;
                                    }
                                }
                                p.device = device;
                                p.width = width;
                                p.height = height;
                                p.size_rank = size_rank.unwrap_or(0);
                                p.role = role;
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
                        // A host-role claim (or a forced takeover that then
                        // died) may have flipped the detached state; the
                        // sweep also cleans up a displaced holder.
                        sweep_and_refresh(
                            &mut viewers, &mut monitor, &mut mirror, host_rank, headless,
                            &mut host_attached, &pty_write, &mut host,
                        ).await?;
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
                            msg, i, &mut viewers, &mut mirror, host_rank, headless,
                            &mut pty_write, &mut host, readonly, &mut input_queue,
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
                sweep_and_refresh(
                    &mut viewers, &mut monitor, &mut mirror, host_rank, headless,
                    &mut host_attached, &pty_write, &mut host,
                ).await?;
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
                drain_pty(&mut pty_read, &mut mirror, &mut host, &mut viewers, &mut pty_buf)
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
    host.write(&mirror.cleanup()).await?;
    drop(control); // removes the socket file
    Ok(status)
}

/// A client's rank in the size-precedence order: host-role clients stand in
/// for the host terminal.
fn effective_rank(c: &Client, host_rank: u32) -> u32 {
    match c.role {
        Role::Host => host_rank,
        Role::Viewer => c.size_rank,
    }
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
#[allow(clippy::too_many_arguments)]
async fn refresh_view(
    viewers: &mut [Client],
    mirror: &mut Mirror,
    host_rank: u32,
    headless: bool,
    force_for: Option<usize>,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut HostTerm,
) -> anyhow::Result<()> {
    let best = viewers
        .iter()
        .filter(|c| !c.dead && c.state == ClientState::Viewing)
        .min_by_key(|c| (effective_rank(c, host_rank), c.id));
    let target = best.map(|c| {
        if headless || effective_rank(c, host_rank) <= host_rank {
            (c.height, c.width)
        } else {
            // A physical host terminal outranks every viewer: the stream
            // runs at its size.
            mirror.host_size()
        }
    });
    match target {
        None => {
            // Nobody is viewing. Headless: keep the view (and thus the pty
            // size) exactly as the last viewer left it, so the app doesn't
            // reflow every time the glasses blink off. Hosted: end the view
            // — though a hotkey-simulated one is not ours to end.
            if !headless && mirror.view().is_some_and(|v| !v.simulated) {
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
/// client dies during the snapshot broadcasts. Also the point where changes
/// to the host-role occupancy (a claim, or the holder dying) are noticed
/// and reported as `host_changed`.
#[allow(clippy::too_many_arguments)]
async fn sweep_and_refresh(
    viewers: &mut Vec<Client>,
    monitor: &mut Option<Client>,
    mirror: &mut Mirror,
    host_rank: u32,
    headless: bool,
    host_attached: &mut bool,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut HostTerm,
) -> anyhow::Result<()> {
    loop {
        let now = viewers.iter().any(|c| !c.dead && c.role == Role::Host);
        if now != *host_attached {
            *host_attached = now;
            let msg = FromSession::HostChanged { attached: now };
            if let Some(m) = monitor.as_mut()
                && m.send(&msg).await.is_err()
            {
                *monitor = None;
            }
            for c in viewers.iter_mut().filter(|c| !c.dead) {
                if c.send(&msg).await.is_err() {
                    c.dead = true;
                }
            }
        }
        viewers.retain(|c| !c.dead);
        refresh_view(viewers, mirror, host_rank, headless, None, pty_write, stdout).await?;
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
    headless: bool,
    pty_write: &mut pty_process::OwnedWritePty,
    stdout: &mut HostTerm,
    readonly: bool,
    input_queue: &mut InputQueue,
) -> anyhow::Result<()> {
    match (msg, viewers[i].state) {
        (ToSession::Init { .. }, _) => anyhow::bail!("duplicate init"),
        (ToSession::Monitor { .. }, _) => anyhow::bail!("already initialized as a viewer"),
        (ToSession::Resize { width, height }, _) => {
            anyhow::ensure!(width > 0 && height > 0, "invalid resize dimensions");
            viewers[i].width = width;
            viewers[i].height = height;
            // If this client controls the view size, everyone follows it.
            refresh_view(viewers, mirror, host_rank, headless, None, pty_write, stdout).await
        }
        (ToSession::Input { data, delays }, _) => {
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
            anyhow::ensure!(
                delays.len() <= MAX_INPUT_DELAYS,
                "too many input delays (max {MAX_INPUT_DELAYS})"
            );
            let mut prev = 0u64;
            for d in &delays {
                anyhow::ensure!(
                    d.at >= prev && d.at <= bytes.len() as u64,
                    "input delay offsets must be non-decreasing and within the data"
                );
                prev = d.at;
            }
            input_queue.push(bytes, &delays);
            input_queue.pump(pty_write).await?;
            Ok(())
        }
        (ToSession::View, _) => {
            // The refresh restarts the view unconditionally, so this client
            // gets its snapshot even when the winning dimensions are
            // unchanged (and a re-sent view still re-snapshots); the other
            // viewers are re-snapshotted only if the dimensions changed.
            viewers[i].state = ClientState::Viewing;
            refresh_view(viewers, mirror, host_rank, headless, Some(i), pty_write, stdout).await
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
            refresh_view(viewers, mirror, host_rank, headless, None, pty_write, stdout).await
        }
        (ToSession::Unview, _) => Ok(()), // idempotent
    }
}

/// Ctrl+G: toggle the simulated device view. Ignored while a real client is
/// viewing (a real view exists exactly when a non-simulated view is active).
async fn toggle_simulated(
    mirror: &mut Mirror,
    pty_write: &pty_process::OwnedWritePty,
    stdout: &mut HostTerm,
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
    stdout: &mut HostTerm,
) -> anyhow::Result<()> {
    if let Some((rows, cols)) = t.child_size {
        // Resizing the pty delivers SIGWINCH to the child, prompting it to
        // repaint at the new dimensions.
        pty_write.resize(pty_process::Size::new(rows, cols))?;
    }
    stdout.write(&t.host_output).await?;
    Ok(())
}

/// After the child exits, read whatever it wrote just before exiting,
/// delivering it to the host terminal and (best-effort) the viewing clients.
async fn drain_pty(
    pty_read: &mut pty_process::OwnedReadPty,
    mirror: &mut Mirror,
    stdout: &mut HostTerm,
    viewers: &mut Vec<Client>,
    buf: &mut [u8],
) -> anyhow::Result<()> {
    let deadline = std::time::Duration::from_millis(50);
    while let Ok(Ok(n)) = tokio::time::timeout(deadline, pty_read.read(buf)).await {
        if n == 0 {
            break;
        }
        let out = mirror.process(&buf[..n]);
        stdout.write(&out.host).await?;
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
    Ok(())
}
