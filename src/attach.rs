//! `g2mirror --list` and `g2mirror --attach`: discover local sessions by
//! probing the sockets in ~/.g2mirror, and claim a detached (headless)
//! session into the current terminal.
//!
//! The attach client is a session-protocol viewer that declares
//! `role: host`, so its terminal size occupies the `"host"` slot of the
//! server's `size_precedence` order. Rendering reuses the same Mirror
//! machinery as g2mirror-view: history is printed into this terminal's
//! native scrollback, the live viewport is mirrored bottom-anchored, and
//! Ctrl+L re-pushes hidden top rows. Ctrl+\ detaches, leaving the session
//! running.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use g2mirror::mirror::{Mirror, View};
use g2mirror::protocol::{self, FromSession, HistoryLine, Role, ToSession, PROTOCOL_VERSION};
use g2mirror::raw_guard::RawGuard;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::signal::unix::{signal, SignalKind};

/// Default detach key: Ctrl+\ — unlike the viewer's Ctrl+D, ^D must reach a
/// claimed shell (it means "exit"), and ^\ (SIGQUIT) is rarely wanted.
const DEFAULT_DETACH_KEY: u8 = 0x1c;
const REFRESH_KEY: u8 = 0x0c;

/// How long a probe may spend connecting to + greeting one socket.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

const ENTER_ALT: &[u8] = b"\x1b[?1049h\x1b[?25l";
const LEAVE_ALT: &[u8] = b"\x1b[?25h\x1b[?1049l";
const SGR_RESET: &[u8] = b"\x1b[0m";
/// While watching (`--watch`), report mouse button presses (X10 mode, SGR
/// encoding) so a click can take the session back. X10 reports presses
/// only, so no release report is left over to leak into the app once the
/// host role is back. Switched off again before reclaiming.
const MOUSE_ON: &[u8] = b"\x1b[?9h\x1b[?1006h";
const MOUSE_OFF: &[u8] = b"\x1b[?1006l\x1b[?9l";

pub struct AttachOpts {
    pub pattern: Option<String>,
    pub force: bool,
    /// On losing the host role to another attach client, keep watching
    /// instead of exiting; a keypress or click takes the role back.
    pub watch: bool,
    pub detach_key: Option<u8>,
}

pub fn parse_attach_args(args: &[std::ffi::OsString]) -> anyhow::Result<AttachOpts> {
    let mut opts = AttachOpts {
        pattern: None,
        force: false,
        watch: false,
        detach_key: Some(DEFAULT_DETACH_KEY),
    };
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.to_str() {
            Some("--force") => opts.force = true,
            Some("--watch") => opts.watch = true,
            Some("--detach-key") => {
                let value = it
                    .next()
                    .and_then(|v| v.to_str())
                    .context("--detach-key requires a value (e.g. ctrl-\\, ctrl-g, none)")?;
                opts.detach_key = parse_detach_key(value)?;
            }
            Some(s) if !s.starts_with('-') && opts.pattern.is_none() => {
                opts.pattern = Some(s.to_string());
            }
            _ => anyhow::bail!("unexpected --attach argument {:?}", arg.to_string_lossy()),
        }
    }
    Ok(opts)
}

/// `none`, or a control key written as `ctrl-x`, `C-x`, or `^x`.
fn parse_detach_key(s: &str) -> anyhow::Result<Option<u8>> {
    if s.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    let key = s
        .strip_prefix("ctrl-")
        .or_else(|| s.strip_prefix("ctrl+"))
        .or_else(|| s.strip_prefix("C-"))
        .or_else(|| s.strip_prefix("^"))
        .with_context(|| format!("cannot parse detach key {s:?} (want e.g. ctrl-\\ or none)"))?;
    let [c] = key.as_bytes() else {
        anyhow::bail!("cannot parse detach key {s:?}: want a single character after the prefix");
    };
    let ctrl = c.to_ascii_uppercase();
    anyhow::ensure!(
        (b'@'..=b'_').contains(&ctrl),
        "{s:?} does not name a control character"
    );
    Ok(Some(ctrl & 0x1f))
}

/// What a probe of one session socket learned (the `connect` greeting).
#[derive(Clone)]
struct Probe {
    path: PathBuf,
    pid: u32,
    command: String,
    cwd: String,
    title: Option<String>,
    headless: bool,
    detached: bool,
    launched: Option<String>,
    /// When the app last wrote output (unix epoch ms), if it has.
    last_output_at: Option<u64>,
    /// Socket file age — effectively the session's uptime.
    age: Option<std::time::Duration>,
}

/// Connect briefly to every live session socket and collect greetings.
/// Detached sessions sort first (then headless, then by pid).
async fn probe_all() -> anyhow::Result<Vec<Probe>> {
    let dir = g2mirror::paths::g2mirror_dir().context("failed to open ~/.g2mirror")?;
    let mut probes = Vec::new();
    for entry in std::fs::read_dir(&dir)
        .with_context(|| format!("failed to read {}", dir.display()))?
        .flatten()
    {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !g2mirror::paths::is_valid_socket_name(&name)
            || !g2mirror::paths::socket_pid(&name).is_some_and(g2mirror::paths::pid_exists)
        {
            continue;
        }
        let path = entry.path();
        let age = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.elapsed().ok());
        if let Ok(Ok(Some(probe))) =
            tokio::time::timeout(PROBE_TIMEOUT, probe_one(&path, age)).await
        {
            probes.push(probe);
        }
    }
    probes.sort_by_key(|p| (!p.detached, !p.headless, p.pid));
    Ok(probes)
}

async fn probe_one(path: &Path, age: Option<std::time::Duration>) -> anyhow::Result<Option<Probe>> {
    let mut conn = SessionClient::connect(path).await?;
    let Some(FromSession::Connect {
        pid,
        command,
        cwd,
        title,
        headless,
        detached,
        launched,
        last_output_at,
        ..
    }) = conn.next().await?
    else {
        return Ok(None);
    };
    Ok(Some(Probe {
        path: path.to_path_buf(),
        pid,
        command,
        cwd,
        title,
        headless,
        detached,
        launched,
        last_output_at,
        age,
    }))
}

fn tilde(path: &str) -> String {
    match std::env::var("HOME") {
        Ok(home) if path.starts_with(&home) => format!("~{}", &path[home.len()..]),
        _ => path.to_string(),
    }
}

fn uptime(age: Option<std::time::Duration>) -> String {
    let Some(age) = age else {
        return String::new();
    };
    let secs = age.as_secs();
    match secs {
        0..=99 => format!("up {secs}s"),
        100..=5999 => format!("up {}m", secs / 60),
        _ => format!("up {}h", secs / 3600),
    }
}

fn describe(p: &Probe) -> String {
    let state = if p.detached {
        "DETACHED"
    } else if p.headless {
        "attached"
    } else {
        "        "
    };
    let what = match (&p.title, &p.launched) {
        (Some(t), _) => t.clone(),
        (None, Some(l)) => format!("[{l}]"),
        (None, None) => p.command.clone(),
    };
    // Output within the last few seconds (the wrapper reports activity at
    // most every 2s, so 5s of slack covers the reporting interval).
    let active = p.last_output_at.is_some_and(|at| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .is_ok_and(|now| now.as_millis().saturating_sub(u128::from(at)) < 5000)
    });
    format!(
        "{state}  {:>6}  {}  {}  {}{}",
        p.pid,
        tilde(&p.cwd),
        what,
        uptime(p.age),
        if active { "  [active]" } else { "" }
    )
}

/// `g2mirror --list`.
pub async fn list_sessions() -> anyhow::Result<i32> {
    let probes = probe_all().await?;
    if probes.is_empty() {
        println!("no live g2mirror sessions");
        return Ok(0);
    }
    for p in &probes {
        println!("{}", describe(p).trim_end());
    }
    if probes.iter().any(|p| p.detached) {
        println!();
        println!("claim a DETACHED session with: g2mirror -a [pid or pattern]");
    }
    Ok(0)
}

/// Case-insensitive substring match against the fields a human would
/// identify a session by.
fn probe_matches(p: &Probe, pattern: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let mut fields = vec![p.pid.to_string(), p.cwd.clone(), p.command.clone()];
    fields.extend(p.title.clone());
    fields.extend(p.launched.clone());
    fields.iter().any(|f| f.to_lowercase().contains(&pattern))
}

/// `g2mirror --attach`.
pub async fn attach(opts: AttachOpts) -> anyhow::Result<i32> {
    anyhow::ensure!(
        rustix::termios::isatty(rustix::stdio::stdin()),
        "--attach needs a terminal"
    );
    let probes = probe_all().await?;
    // Without --force only detached sessions are claimable; with it, any
    // headless session (taking the role over). --watch also accepts a held
    // headless session, starting out watching it. Sessions with a real host
    // terminal are never claimable — g2mirror-view covers watching those.
    let candidates: Vec<Probe> = probes
        .iter()
        .filter(|p| {
            if opts.force || opts.watch {
                p.headless
            } else {
                p.detached
            }
        })
        .filter(|p| {
            opts.pattern
                .as_deref()
                .is_none_or(|pat| probe_matches(p, pat))
        })
        .cloned()
        .collect();
    let target = match candidates.len() {
        0 => {
            let mut msg = match &opts.pattern {
                Some(pat) => format!("no detached session matches {pat:?}"),
                None => "no detached sessions".to_string(),
            };
            let claimed = probes
                .iter()
                .filter(|p| p.headless && !p.detached)
                .filter(|p| {
                    opts.pattern
                        .as_deref()
                        .is_none_or(|pat| probe_matches(p, pat))
                })
                .count();
            if !opts.force && claimed > 0 {
                msg.push_str(&format!(
                    " ({claimed} already attached; --force takes one over)"
                ));
            }
            if probes.is_empty() {
                msg.push_str("; start one with: g2mirror --detached <command>");
            } else {
                msg.push_str("\nlive sessions:");
                for p in &probes {
                    msg.push_str(&format!("\n  {}", describe(p).trim_end()));
                }
            }
            anyhow::bail!("{msg}");
        }
        1 => candidates.into_iter().next().unwrap(),
        _ => match pick(&candidates).await? {
            Some(p) => p,
            None => return Ok(0),
        },
    };
    let start_watching = opts.watch && !opts.force && !target.detached;
    run_attached(
        &target,
        opts.force,
        opts.detach_key,
        opts.watch,
        start_watching,
    )
    .await
}

/// Several candidates: a minimal alt-screen picker (arrows/enter/q).
async fn pick(candidates: &[Probe]) -> anyhow::Result<Option<Probe>> {
    let _raw = RawGuard::new().context("failed to enter raw mode")?;
    let mut stdout = tokio::io::stdout();
    let mut stdin = tokio::io::stdin();
    let mut selected = 0usize;
    let mut buf = [0u8; 256];
    stdout.write_all(ENTER_ALT).await?;
    let result = loop {
        let mut out = b"\x1b[H\x1b[2J".to_vec();
        out.extend_from_slice("g2mirror \u{2014} pick a session to attach\r\n\r\n".as_bytes());
        for (i, p) in candidates.iter().enumerate() {
            let line = describe(p);
            let line = line.trim_end();
            if i == selected {
                out.extend_from_slice(format!("\x1b[7m> {line}\x1b[0m\r\n").as_bytes());
            } else {
                out.extend_from_slice(format!("  {line}\r\n").as_bytes());
            }
        }
        out.extend_from_slice(
            "\r\n  \u{2191}/\u{2193} select \u{00b7} enter attach \u{00b7} q cancel".as_bytes(),
        );
        stdout.write_all(&out).await?;
        stdout.flush().await?;

        let n = stdin.read(&mut buf).await?;
        if n == 0 {
            break None;
        }
        let mut done = None;
        let mut i = 0;
        let bytes = &buf[..n];
        while i < bytes.len() {
            match bytes[i] {
                0x03 | 0x04 | b'q' => done = Some(None),
                b'\r' | b'\n' => done = Some(Some(candidates[selected].clone())),
                b'k' => selected = selected.saturating_sub(1),
                b'j' => selected = (selected + 1).min(candidates.len() - 1),
                0x1b if i + 2 < bytes.len() && matches!(bytes[i + 1], b'[' | b'O') => {
                    match bytes[i + 2] {
                        b'A' => selected = selected.saturating_sub(1),
                        b'B' => selected = (selected + 1).min(candidates.len() - 1),
                        _ => {}
                    }
                    i += 3;
                    continue;
                }
                _ => {}
            }
            i += 1;
        }
        if let Some(result) = done {
            break result;
        }
    };
    stdout.write_all(LEAVE_ALT).await?;
    stdout.flush().await?;
    Ok(result)
}

fn host_size() -> (u16, u16) {
    match rustix::termios::tcgetwinsize(rustix::stdio::stdout()) {
        Ok(ws) if ws.ws_row > 0 && ws.ws_col > 0 => (ws.ws_row, ws.ws_col),
        _ => (24, 80),
    }
}

fn cup(row: u16, col: u16) -> String {
    format!("\x1b[{};{}H", row + 1, col + 1)
}

/// Attach-session phases, mirroring g2mirror-view's live half.
enum Mode {
    /// Sent `view`, waiting for the snapshot.
    AwaitSnapshot,
    /// Got the snapshot; waiting for the history reply before printing.
    /// Live output arriving meanwhile is buffered.
    FetchingHistory {
        stream_rows: u16,
        stream_cols: u16,
        snapshot: Vec<u8>,
        pending: Vec<Vec<u8>>,
        /// Stale history replies to discard (a re-snapshot re-requests).
        skip_replies: u32,
    },
    /// Mirroring to this terminal.
    Live { mirror: Box<Mirror> },
}

/// Which role the next connection to the session takes.
enum Phase {
    /// Hold the host role: this terminal drives the app. `carry` is input
    /// to forward once live (the keystroke that ended a watch).
    Host {
        force: bool,
        fetch_history: bool,
        carry: Vec<u8>,
    },
    /// `--watch`: view a session another attach client holds, without
    /// affecting its size, until a keypress or click takes it back.
    Watch,
}

/// How one connection to the session ended.
enum PhaseEnd {
    /// The wrapped app exited, the session went away, or stdin closed.
    Ended,
    /// The detach key was pressed; the session keeps running.
    Detached,
    /// Another attach client took the host role over with --force.
    Displaced,
    /// Watching, the user asked for control back; carries the keystrokes
    /// to forward once the host role is held (mouse reports dropped).
    Reclaim(Vec<u8>),
}

/// Claim the session and mirror it until the app exits (its exit status is
/// propagated) or the detach key is pressed (returns 0). With `watch`,
/// losing the host role to another attach client switches this terminal to
/// watching instead of exiting, and a keypress or click there takes the
/// role back (displacing the other client in turn).
async fn run_attached(
    probe: &Probe,
    force: bool,
    detach_key: Option<u8>,
    watch: bool,
    start_watching: bool,
) -> anyhow::Result<i32> {
    // Raw mode spans every phase, so nothing echoes while switching.
    let _raw = RawGuard::new().context("failed to enter raw mode")?;
    // One stdin for every phase: tokio reads it on a blocking thread that
    // can't be cancelled, so a phase ending mid-read leaves that read in
    // flight. Reusing the handle hands its bytes to the next phase; a fresh
    // handle per phase would silently drop them (the keystroke or click
    // that should have ended a watch).
    let mut stdin = tokio::io::stdin();
    let mut phase = if start_watching {
        Phase::Watch
    } else {
        Phase::Host {
            force,
            fetch_history: true,
            carry: Vec::new(),
        }
    };
    loop {
        let (end, exit_status) = run_phase(probe, phase, detach_key, &mut stdin).await?;
        phase = match end {
            PhaseEnd::Displaced if watch => Phase::Watch,
            PhaseEnd::Displaced => anyhow::bail!("session closed: {}", protocol::HOST_TAKEN_OVER),
            PhaseEnd::Reclaim(carry) => Phase::Host {
                force: true,
                fetch_history: false,
                carry,
            },
            PhaseEnd::Detached => {
                eprintln!(
                    "g2mirror: detached; the session keeps running \u{2014} reattach with: g2mirror -a {}",
                    probe.pid
                );
                return Ok(0);
            }
            PhaseEnd::Ended => {
                return Ok(match exit_status {
                    Some(status) => {
                        eprintln!("g2mirror: session ended (exit status {status})");
                        status
                    }
                    None => {
                        eprintln!("g2mirror: session ended");
                        1
                    }
                });
            }
        };
    }
}

/// One connection to the session in `phase`'s role, until it ends. Returns
/// how it ended and the app's exit status, if it exited.
async fn run_phase(
    probe: &Probe,
    phase: Phase,
    detach_key: Option<u8>,
    stdin: &mut tokio::io::Stdin,
) -> anyhow::Result<(PhaseEnd, Option<i32>)> {
    let mut conn = SessionClient::connect(&probe.path)
        .await
        .with_context(|| format!("cannot connect to {}", probe.path.display()))?;
    let Some(FromSession::Connect {
        readonly,
        history,
        title,
        ..
    }) = conn.next().await?
    else {
        anyhow::bail!("session did not send a connect greeting");
    };
    let history_oldest = history.oldest;
    let (rows, cols) = host_size();
    let watching = matches!(phase, Phase::Watch);
    let (role, force, size_rank, fetch_history, carry) = match phase {
        Phase::Host {
            force,
            fetch_history,
            carry,
        } => (Role::Host, force, None, fetch_history, carry),
        // The worst rank there is: every other viewer outranks a watcher,
        // and so does the host (a better rank, or the host role on a tie),
        // so watching never resizes the app.
        Phase::Watch => (Role::Viewer, false, Some(u32::MAX), false, Vec::new()),
    };
    conn.send(&ToSession::Init {
        version: PROTOCOL_VERSION,
        device: if watching {
            "g2mirror --attach (watching)"
        } else {
            "g2mirror --attach"
        }
        .into(),
        width: cols,
        height: rows,
        size_rank,
        host_size_rank: None,
        role,
        force,
    })
    .await?;
    conn.send(&ToSession::View).await?;
    if readonly && fetch_history {
        eprint!("g2mirror: note: this session is read-only; keystrokes will be ignored\r\n");
    }

    let mut app = Attached {
        conn,
        stdout: tokio::io::stdout(),
        rows,
        cols,
        mode: Mode::AwaitSnapshot,
        history_oldest,
        detach_key,
        exit_status: None,
        last_error: None,
        detached: false,
        watching,
        fetch_history,
        carry,
        title,
        reclaim: None,
    };
    app.write_title().await?;
    let result = app.run(stdin).await;

    // Restore the terminal whatever happened.
    let mut out = Vec::new();
    if let Mode::Live { mirror } = &app.mode {
        out.extend_from_slice(&mirror.cleanup());
    }
    if watching {
        out.extend_from_slice(MOUSE_OFF);
    }
    out.extend_from_slice(SGR_RESET);
    out.extend_from_slice(b"\x1b[?25h");
    app.stdout.write_all(&out).await?;
    app.stdout.flush().await?;
    let exit_status = app.exit_status;
    drop(app.conn);
    Ok((result?, exit_status))
}

struct Attached {
    conn: SessionClient,
    stdout: tokio::io::Stdout,
    rows: u16,
    cols: u16,
    mode: Mode,
    history_oldest: u64,
    detach_key: Option<u8>,
    /// Set when the wrapped app exits; the wrapper closes the socket next.
    exit_status: Option<i32>,
    /// Last error the wrapper sent while live (e.g. a --force takeover
    /// notice just before it drops us); reported if the connection then
    /// closes unexpectedly.
    last_error: Option<String>,
    detached: bool,
    /// Viewing without the host role (`--watch`): any key or click
    /// reclaims instead of being forwarded.
    watching: bool,
    /// Print the session's scrollback into this terminal on going live.
    /// Only the first phase does; later ones would duplicate it.
    fetch_history: bool,
    /// Input to forward once live (the keystroke that ended a watch).
    carry: Vec<u8>,
    /// The session's window title, if it has set one.
    title: Option<String>,
    /// Set by a keypress or click while watching.
    reclaim: Option<Vec<u8>>,
}

impl Attached {
    async fn run(&mut self, stdin: &mut tokio::io::Stdin) -> anyhow::Result<PhaseEnd> {
        let mut winch = signal(SignalKind::window_change())?;
        let mut keybuf = [0u8; 4096];
        loop {
            tokio::select! {
                msg = self.conn.next() => match msg? {
                    Some(msg) => {
                        if !self.on_message(msg).await? {
                            return Ok(PhaseEnd::Ended);
                        }
                    }
                    None => {
                        // Clean EOF: after `exit` this is the normal end;
                        // otherwise the wrapper vanished (or dropped us,
                        // e.g. a --force takeover).
                        if self.exit_status.is_none() && !self.detached {
                            match self.last_error.take() {
                                Some(e) if e == protocol::HOST_TAKEN_OVER => {
                                    return Ok(PhaseEnd::Displaced);
                                }
                                Some(e) => anyhow::bail!("session closed: {e}"),
                                None => anyhow::bail!("session closed unexpectedly"),
                            }
                        }
                        return Ok(PhaseEnd::Ended);
                    }
                },
                n = stdin.read(&mut keybuf) => match n {
                    Ok(0) => return Ok(PhaseEnd::Ended),
                    Ok(n) => {
                        let bytes: Vec<u8> = keybuf[..n].to_vec();
                        self.on_keys(&bytes).await?;
                        if self.detached {
                            return Ok(PhaseEnd::Detached);
                        }
                        if let Some(carry) = self.reclaim.take() {
                            return Ok(PhaseEnd::Reclaim(carry));
                        }
                    }
                    Err(e) => return Err(e).context("error reading stdin"),
                },
                _ = winch.recv() => {
                    let (rows, cols) = host_size();
                    self.rows = rows;
                    self.cols = cols;
                    if let Mode::Live { mirror } = &mut self.mode {
                        let out = mirror.host_resized(rows, cols).host_output;
                        self.stdout.write_all(&out).await?;
                        self.stdout.flush().await?;
                    }
                    self.conn.send(&ToSession::Resize { width: cols, height: rows }).await?;
                }
            }
        }
    }

    /// Handle one session message; false means "stop" (used by errors that
    /// abort the attach before it goes live).
    async fn on_message(&mut self, msg: FromSession) -> anyhow::Result<bool> {
        match msg {
            FromSession::Snapshot {
                data,
                width,
                height,
                history_next,
            } => {
                let Ok(snapshot) = protocol::decode_terminal_bytes(&data) else {
                    return Ok(true);
                };
                self.on_snapshot(snapshot, height, width, history_next).await?;
            }
            FromSession::Output { data } => {
                let Ok(bytes) = protocol::decode_terminal_bytes(&data) else {
                    return Ok(true);
                };
                match &mut self.mode {
                    Mode::Live { mirror } => {
                        let out = mirror.process(&bytes);
                        self.stdout.write_all(&out.host).await?;
                        if out.bells > 0 {
                            self.stdout.write_all(b"\x07").await?;
                        }
                        if self.watching {
                            // The app's own output may have switched mouse
                            // reporting off; watching needs clicks.
                            self.stdout.write_all(MOUSE_ON).await?;
                        }
                        self.stdout.flush().await?;
                    }
                    Mode::FetchingHistory { pending, .. } => pending.push(bytes),
                    Mode::AwaitSnapshot => {}
                }
            }
            FromSession::HistoryLines { lines, .. } => {
                if let Mode::FetchingHistory { skip_replies, .. } = &mut self.mode {
                    if *skip_replies > 0 {
                        *skip_replies -= 1;
                    } else {
                        self.go_live(lines).await?;
                    }
                }
            }
            FromSession::Title { title } => {
                self.title = Some(title);
                self.write_title().await?;
            }
            FromSession::Exit { status } => self.exit_status = Some(status.unwrap_or(1)),
            FromSession::Error { message } => {
                // Before the view is up an error is fatal (a refused init:
                // not headless, already attached). While live it's a policy
                // reply (e.g. input to a read-only session); remember it
                // only in case the wrapper closes on us next.
                if !matches!(self.mode, Mode::Live { .. }) {
                    anyhow::bail!("{message}");
                }
                self.last_error = Some(message);
            }
            _ => {}
        }
        Ok(true)
    }

    async fn on_snapshot(
        &mut self,
        snapshot: Vec<u8>,
        mut stream_rows: u16,
        mut stream_cols: u16,
        history_next: u64,
    ) -> anyhow::Result<()> {
        if stream_rows == 0 || stream_cols == 0 {
            (stream_rows, stream_cols) = (self.rows, self.cols);
        }
        match &mut self.mode {
            Mode::AwaitSnapshot => {
                self.mode = Mode::FetchingHistory {
                    stream_rows,
                    stream_cols,
                    snapshot,
                    pending: Vec::new(),
                    skip_replies: 0,
                };
                if self.fetch_history && history_next > self.history_oldest {
                    self.conn
                        .send(&ToSession::History {
                            before: history_next,
                            limit: None,
                        })
                        .await?;
                } else {
                    self.go_live(Vec::new()).await?;
                }
            }
            // Stream dimensions changed (e.g. the glasses attached or
            // detached): restart the local view from the fresh snapshot.
            Mode::Live { mirror } => {
                let mut out = mirror
                    .start_view(View {
                        rows: stream_rows,
                        cols: stream_cols,
                        simulated: false,
                    })
                    .host_output;
                out.extend_from_slice(&mirror.process(&snapshot).host);
                if self.watching {
                    out.extend_from_slice(MOUSE_ON);
                }
                self.stdout.write_all(&out).await?;
                self.stdout.flush().await?;
            }
            // A re-snapshot while the history reply is in flight restarts
            // the stream; re-request the history.
            Mode::FetchingHistory {
                stream_rows: r,
                stream_cols: c,
                snapshot: snap,
                pending,
                skip_replies,
            } => {
                *r = stream_rows;
                *c = stream_cols;
                *snap = snapshot;
                pending.clear();
                if history_next > self.history_oldest {
                    *skip_replies += 1;
                    self.conn
                        .send(&ToSession::History {
                            before: history_next,
                            limit: None,
                        })
                        .await?;
                } else {
                    self.go_live(Vec::new()).await?;
                }
            }
        }
        Ok(())
    }

    /// Print the fetched history into this terminal (and thus its native
    /// scrollback) and start the live view — the same dance as
    /// g2mirror-view's go_live.
    async fn go_live(&mut self, lines: Vec<HistoryLine>) -> anyhow::Result<()> {
        let Mode::FetchingHistory {
            stream_rows,
            stream_cols,
            snapshot,
            pending,
            ..
        } = std::mem::replace(&mut self.mode, Mode::AwaitSnapshot)
        else {
            return Ok(());
        };
        let mut out = SGR_RESET.to_vec();
        out.extend_from_slice(b"\x1b[?7h"); // history lines rely on autowrap
        if self.fetch_history {
            out.extend_from_slice(cup(self.rows.saturating_sub(1), 0).as_bytes());
            let mut continuation = false;
            for line in &lines {
                if !continuation {
                    out.extend_from_slice(b"\r\n");
                }
                if let Ok(bytes) = protocol::decode_terminal_bytes(&line.data) {
                    out.extend_from_slice(&bytes);
                }
                out.extend_from_slice(SGR_RESET);
                continuation = line.wrapped;
            }
            // Scroll the history clear of the bottom-anchored live region.
            let region_rows = stream_rows.min(self.rows);
            out.extend_from_slice(cup(self.rows.saturating_sub(1), 0).as_bytes());
            out.extend_from_slice(&b"\r\n".repeat(usize::from(region_rows)));
        } else {
            // Switching between watching and holding the session: the
            // history is already in this terminal's scrollback, so repaint
            // from a clean screen rather than scrolling another copy of the
            // view into it.
            out.extend_from_slice(b"\x1b[H\x1b[2J");
        }

        let mut mirror = Box::new(Mirror::new(self.rows, self.cols));
        out.extend_from_slice(
            &mirror
                .start_view(View {
                    rows: stream_rows,
                    cols: stream_cols,
                    simulated: false,
                })
                .host_output,
        );
        out.extend_from_slice(&mirror.process(&snapshot).host);
        for chunk in &pending {
            out.extend_from_slice(&mirror.process(chunk).host);
        }
        if self.watching {
            out.extend_from_slice(MOUSE_ON);
        }
        self.stdout.write_all(&out).await?;
        self.stdout.flush().await?;
        self.mode = Mode::Live { mirror };
        if !self.carry.is_empty() {
            let carry = std::mem::take(&mut self.carry);
            self.conn
                .send(&ToSession::Input {
                    data: protocol::encode_terminal_bytes(&carry),
                    delays: Vec::new(),
                })
                .await?;
        }
        Ok(())
    }

    /// Set this terminal's title from the session's, marked while watching.
    async fn write_title(&mut self) -> anyhow::Result<()> {
        let base: String = self
            .title
            .as_deref()
            .unwrap_or("")
            .chars()
            .filter(|c| !c.is_control())
            .collect();
        let title = if !self.watching {
            base
        } else if base.is_empty() {
            "watching \u{2014} click or press a key to take control".to_string()
        } else {
            format!("[watching] {base} \u{2014} click or press a key to take control")
        };
        // The first phase leaves an untitled terminal alone; later ones
        // always write, clearing a watching title on the way back.
        if title.is_empty() && self.fetch_history {
            return Ok(());
        }
        self.stdout
            .write_all(format!("\x1b]2;{title}\x07").as_bytes())
            .await?;
        self.stdout.flush().await?;
        Ok(())
    }

    async fn on_keys(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        if self.watching {
            // Any key or click takes the session back; the detach key still
            // detaches, leaving the session to whoever holds it.
            // Focus reports (the app may have turned them on) aren't a
            // request for control: tabs gaining or losing focus as the user
            // moves around must not pull the session back and forth.
            let bytes = strip_focus_reports(bytes);
            if self.detach_key.is_some_and(|k| bytes.contains(&k)) {
                self.detached = true;
                let _ = self.conn.send(&ToSession::Unview).await;
            } else if !bytes.is_empty() {
                self.reclaim = Some(strip_mouse_reports(&bytes));
            }
            return Ok(());
        }
        let (input, detach) = match self
            .detach_key
            .and_then(|k| bytes.iter().position(|&b| b == k))
        {
            Some(pos) => (&bytes[..pos], true),
            None => (bytes, false),
        };
        // Ctrl+L: re-push hidden top rows into the local scrollback and
        // repaint; still forwarded, preserving the app's own ^L behavior.
        if input.contains(&REFRESH_KEY)
            && let Mode::Live { mirror } = &self.mode
        {
            let out = mirror.refresh_scrollback();
            self.stdout.write_all(&out).await?;
            self.stdout.flush().await?;
        }
        if !input.is_empty() {
            self.conn
                .send(&ToSession::Input {
                    data: protocol::encode_terminal_bytes(input),
                    delays: Vec::new(),
                })
                .await?;
        }
        if detach {
            self.detached = true;
            let _ = self.conn.send(&ToSession::Unview).await;
        }
        Ok(())
    }
}

/// Drop mouse reports (SGR `ESC [ < b ; x ; y M/m`, X10 `ESC [ M b x y`)
/// from input typed while watching: a click only takes the session back,
/// it isn't forwarded, but keystrokes are.
fn strip_mouse_reports(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let rest = &bytes[i..];
        if let Some(params) = rest.strip_prefix(b"\x1b[<")
            && let Some(end) = params.iter().position(|&b| b == b'M' || b == b'm')
            && params[..end]
                .iter()
                .all(|&b| b.is_ascii_digit() || b == b';')
        {
            i += 3 + end + 1;
            continue;
        }
        if rest.starts_with(b"\x1b[M") && rest.len() >= 6 {
            i += 6;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// Drop focus-in/out reports (`ESC [ I`, `ESC [ O`) from input.
fn strip_focus_reports(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if matches!(bytes[i..], [0x1b, b'[', b'I' | b'O', ..]) {
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// Typed newline-delimited-JSON framing over the session socket (the
/// client-side dual of the wrapper's `control::Client`).
struct SessionClient {
    stream: UnixStream,
    buf: Vec<u8>,
}

impl SessionClient {
    async fn connect(path: &Path) -> anyhow::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self {
            stream,
            buf: Vec::new(),
        })
    }

    /// Next message; Ok(None) on clean EOF. Cancel-safe (partial lines stay
    /// buffered).
    async fn next(&mut self) -> anyhow::Result<Option<FromSession>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let line = &line[..line.len() - 1];
                if line.is_empty() {
                    continue;
                }
                let msg = serde_json::from_slice(line).with_context(|| {
                    format!("bad session message: {}", String::from_utf8_lossy(line))
                })?;
                return Ok(Some(msg));
            }
            let mut chunk = [0u8; 16 * 1024];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn send(&mut self, msg: &ToSession) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(msg)?;
        line.push(b'\n');
        self.stream.write_all(&line).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detach_keys_parse() {
        assert_eq!(parse_detach_key("none").unwrap(), None);
        assert_eq!(parse_detach_key("ctrl-\\").unwrap(), Some(0x1c));
        assert_eq!(parse_detach_key("^\\").unwrap(), Some(0x1c));
        assert_eq!(parse_detach_key("C-g").unwrap(), Some(0x07));
        assert_eq!(parse_detach_key("ctrl-A").unwrap(), Some(0x01));
        assert_eq!(parse_detach_key("ctrl+_").unwrap(), Some(0x1f));
        assert!(parse_detach_key("x").is_err());
        assert!(parse_detach_key("ctrl-").is_err());
        assert!(parse_detach_key("ctrl-!").is_err());
        assert!(parse_detach_key("ctrl-xy").is_err());
    }

    #[test]
    fn attach_args_parse() {
        let opts = parse_attach_args(&[]).unwrap();
        assert_eq!(opts.pattern, None);
        assert!(!opts.force);
        assert_eq!(opts.detach_key, Some(DEFAULT_DETACH_KEY));

        let args: Vec<std::ffi::OsString> =
            ["4321", "--force", "--detach-key", "none"].iter().map(Into::into).collect();
        let opts = parse_attach_args(&args).unwrap();
        assert_eq!(opts.pattern.as_deref(), Some("4321"));
        assert!(opts.force);
        assert_eq!(opts.detach_key, None);

        let args: Vec<std::ffi::OsString> = ["a", "b"].iter().map(Into::into).collect();
        assert!(parse_attach_args(&args).is_err(), "two patterns must be rejected");

        assert!(!parse_attach_args(&[]).unwrap().watch);
        let args: Vec<std::ffi::OsString> = ["--watch", "4321"].iter().map(Into::into).collect();
        let opts = parse_attach_args(&args).unwrap();
        assert!(opts.watch);
        assert_eq!(opts.pattern.as_deref(), Some("4321"));
    }

    #[test]
    fn focus_reports_are_stripped_but_keys_kept() {
        assert_eq!(strip_focus_reports(b"\x1b[I"), b"");
        assert_eq!(strip_focus_reports(b"\x1b[Ohi\x1b[I"), b"hi");
        assert_eq!(strip_focus_reports(b"\x1b[A\x1bO"), b"\x1b[A\x1bO");
    }

    #[test]
    fn mouse_reports_are_stripped_but_keys_kept() {
        assert_eq!(strip_mouse_reports(b"\x1b[<0;12;5M"), b"");
        assert_eq!(strip_mouse_reports(b"\x1b[<0;12;5Mhi\x1b[<0;12;5m"), b"hi");
        assert_eq!(strip_mouse_reports(b"\x1b[M !!x"), b"x");
        assert_eq!(strip_mouse_reports(b"ls\r"), b"ls\r");
        // Arrow keys and other escape sequences are keystrokes, not clicks.
        assert_eq!(strip_mouse_reports(b"\x1b[A"), b"\x1b[A");
        assert_eq!(strip_mouse_reports(b"\x1b[<x"), b"\x1b[<x");
    }

    #[test]
    fn probe_matching_is_case_insensitive_across_fields() {
        let probe = Probe {
            path: PathBuf::from("/tmp/x"),
            pid: 4321,
            command: "claude --continue".into(),
            cwd: "/Users/jim/repositories/g2mirror".into(),
            title: Some("Fixing the Frobnicator".into()),
            headless: true,
            detached: true,
            launched: Some("shell".into()),
            last_output_at: None,
            age: None,
        };
        for pat in ["4321", "g2mirror", "frobnicator", "SHELL", "claude"] {
            assert!(probe_matches(&probe, pat), "{pat}");
        }
        assert!(!probe_matches(&probe, "htop"));
    }
}
