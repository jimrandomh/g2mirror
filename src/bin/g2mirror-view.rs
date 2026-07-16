//! g2mirror-view: a terminal client for g2mirror-server, for humans without
//! smart glasses (e.g. a coworker following a shared project's terminals).
//!
//! Connects with `g2mirror-view g2mirror://<token>@<host>[:port]`, shows the
//! list of live terminals (arrow keys + enter to attach), and mirrors the
//! selected one into the local terminal: recent scrollback history is
//! printed into the local terminal's own scrollback, followed by a live
//! view of the current viewport. Ctrl+D detaches back to the list; all
//! other keys are forwarded to the wrapped app unless the token or the
//! session is read-only. Width mismatches are tolerated in both directions:
//! the stream arrives at the best-ranked viewer's size (see
//! `size_precedence` in the server config) and is cropped bottom-left to
//! fit, exactly like the wrapper's own host terminal rendering.

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _};
use g2mirror::mirror::{Mirror, View};
use g2mirror::protocol::{self, SessionInfo, PROTOCOL_VERSION};
use g2mirror::raw_guard::RawGuard;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::signal::unix::{signal, SignalKind};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEFAULT_PORT: u16 = 8737;
/// How often the session list is refreshed while it is on screen.
const LIST_REFRESH: std::time::Duration = std::time::Duration::from_secs(2);

const ENTER_ALT: &[u8] = b"\x1b[?1049h\x1b[?25l";
const LEAVE_ALT: &[u8] = b"\x1b[?25h\x1b[?1049l";
const SGR_RESET: &[u8] = b"\x1b[0m";

fn usage() -> ! {
    eprintln!("usage: g2mirror-view g2mirror://<token>@<host>[:<port>]");
    eprintln!("       g2mirror-view g2mirrors://<token>@<host>[:<port>]");
    eprintln!("  the token comes from g2mirror-server --init-config / --add-token");
    eprintln!("  g2mirror://  — plain websocket (private network), default port {DEFAULT_PORT}");
    eprintln!("  g2mirrors:// — TLS (e.g. a tailscale funnel hostname), default port 443");
    std::process::exit(2);
}

/// Split a `g2mirror[s]://<token>@<host>[:<port>]` connection string into
/// the token and a dialable websocket URL (`g2mirrors` means TLS, for
/// endpoints like a tailscale funnel hostname).
fn parse_connection_string(s: &str) -> anyhow::Result<(String, String)> {
    let (scheme, rest, default_port) = if let Some(rest) = s.strip_prefix("g2mirrors://") {
        ("wss", rest, 443)
    } else if let Some(rest) = s.strip_prefix("g2mirror://") {
        ("ws", rest, DEFAULT_PORT)
    } else {
        anyhow::bail!("connection string must start with g2mirror:// or g2mirrors://");
    };
    let (token, host) = rest
        .rsplit_once('@')
        .context("connection string must contain <token>@<host>")?;
    anyhow::ensure!(!token.is_empty(), "connection string has an empty token");
    anyhow::ensure!(!host.is_empty(), "connection string has an empty host");
    // Append the default port unless one is present ("[::1]" has colons but
    // its last colon-suffix still contains the closing bracket).
    let addr = match host.rfind(':') {
        Some(i) if !host[i..].contains(']') => host.to_string(),
        _ => format!("{host}:{default_port}"),
    };
    Ok((token.to_string(), format!("{scheme}://{addr}")))
}

fn host_size() -> (u16, u16) {
    // A pty can report 0x0 (e.g. under `script` without a real terminal);
    // fall back rather than declaring degenerate dimensions.
    match rustix::termios::tcgetwinsize(rustix::stdio::stdout()) {
        Ok(ws) if ws.ws_row > 0 && ws.ws_col > 0 => (ws.ws_row, ws.ws_col),
        _ => (24, 80),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn cup(row: u16, col: u16) -> String {
    format!("\x1b[{};{}H", row + 1, col + 1)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(conn), None) = (args.next(), args.next()) else {
        usage()
    };
    if conn == "--help" || conn == "-h" {
        usage();
    }
    let (token, url) = match parse_connection_string(&conn) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("g2mirror-view: {e:#}");
            usage();
        }
    };
    if !rustix::termios::isatty(rustix::stdio::stdin()) {
        eprintln!("g2mirror-view: stdin is not a terminal");
        std::process::exit(1);
    }
    // rustls ships without a process-level crypto provider compiled in;
    // select one explicitly or wss:// connections panic at handshake time.
    // (Err just means one is already installed.)
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    if let Err(e) = runtime.block_on(run(token, url)) {
        eprintln!("g2mirror-view: {e:#}");
        std::process::exit(1);
    }
}

/// What the viewer is doing, i.e. which screen the user is looking at and
/// which protocol messages are expected next.
enum Mode {
    /// The session list (alternate screen).
    List,
    /// Sent `connect`, waiting for the session's `connect` greeting.
    Connecting,
    /// Sent `view`, waiting for the snapshot.
    AwaitSnapshot,
    /// Got the snapshot, waiting for the history reply before leaving the
    /// alternate screen; live output arriving meanwhile is buffered.
    FetchingHistory {
        stream_rows: u16,
        stream_cols: u16,
        snapshot: Vec<u8>,
        pending: Vec<Vec<u8>>,
        /// Stale history replies to discard (a re-snapshot re-requests).
        skip_replies: u32,
    },
    /// Mirroring to the local terminal (main screen).
    Live { mirror: Box<Mirror> },
}

struct App {
    stdout: tokio::io::Stdout,
    addr: String,
    rows: u16,
    cols: u16,
    /// The dimensions declared in our init, fixed for the connection.
    /// Wrappers older than the snapshot width/height fields size the
    /// stream to exactly these.
    init_rows: u16,
    init_cols: u16,
    mode: Mode,
    sessions: Vec<SessionInfo>,
    selected: usize,
    /// One-line notice shown in the list header (errors, detach reasons).
    status: String,
    token_readonly: bool,
    session_readonly: bool,
    /// Oldest fetchable history index of the attached session.
    history_oldest: u64,
    quit: bool,
}

async fn run(token: String, url: String) -> anyhow::Result<()> {
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("failed to connect to {url}"))?;

    let (rows, cols) = host_size();
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "init",
            "version": PROTOCOL_VERSION,
            "auth_token": token,
            "device": "g2mirror-view",
            "width": cols,
            "height": rows,
        }),
    )
    .await?;
    let reply = next_text(&mut ws)
        .await?
        .context("server closed the connection during the handshake")?;
    let reply: serde_json::Value =
        serde_json::from_str(&reply).context("bad handshake reply")?;
    let token_readonly = match reply.get("type").and_then(|t| t.as_str()) {
        Some("init") => reply.get("readonly").and_then(|r| r.as_bool()).unwrap_or(true),
        _ => anyhow::bail!(
            "server rejected the connection: {}",
            reply.get("message").and_then(|m| m.as_str()).unwrap_or("unknown error")
        ),
    };

    let _raw = RawGuard::new().context("failed to enter raw mode")?;
    let mut app = App {
        stdout: tokio::io::stdout(),
        addr: url,
        rows,
        cols,
        init_rows: rows,
        init_cols: cols,
        mode: Mode::List,
        sessions: Vec::new(),
        selected: 0,
        status: String::new(),
        token_readonly,
        session_readonly: false,
        history_oldest: 0,
        quit: false,
    };
    app.stdout.write_all(ENTER_ALT).await?;
    app.render_list().await?;

    let mut stdin = tokio::io::stdin();
    let mut winch = signal(SignalKind::window_change())?;
    let mut refresh = tokio::time::interval(LIST_REFRESH);
    let mut keybuf = [0u8; 4096];

    let result = loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if let Err(e) = app.on_ws_text(&mut ws, &text).await {
                        break Err(e);
                    }
                }
                Some(Ok(Message::Close(_))) | None => {
                    break Err(anyhow::anyhow!("server closed the connection"));
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => break Err(e).context("websocket error"),
            },
            n = stdin.read(&mut keybuf) => match n {
                Ok(0) => break Ok(()),
                Ok(n) => {
                    let bytes: Vec<u8> = keybuf[..n].to_vec();
                    if let Err(e) = app.on_keys(&mut ws, &bytes).await {
                        break Err(e);
                    }
                    if app.quit {
                        break Ok(());
                    }
                }
                Err(e) => break Err(e).context("error reading stdin"),
            },
            _ = winch.recv() => {
                let (rows, cols) = host_size();
                if let Err(e) = app.on_winch(rows, cols).await {
                    break Err(e);
                }
            }
            _ = refresh.tick() => {
                if matches!(app.mode, Mode::List)
                    && let Err(e) = send_json(&mut ws, serde_json::json!({"type": "list"})).await {
                        break Err(e);
                    }
            }
        }
    };

    // Best-effort clean shutdown so the server logs a close, not a reset.
    let _ = ws.close(None).await;

    // Restore the terminal whatever happened: end a live view under the
    // cursor, or leave the alternate screen.
    let mut out = Vec::new();
    match &app.mode {
        Mode::Live { mirror } => out.extend_from_slice(&mirror.cleanup()),
        _ => out.extend_from_slice(LEAVE_ALT),
    }
    out.extend_from_slice(SGR_RESET);
    out.extend_from_slice(b"\x1b[?25h");
    app.stdout.write_all(&out).await?;
    app.stdout.flush().await?;
    result
}

impl App {
    /// Handle one message from the server (server-scoped or relayed from
    /// the attached session).
    async fn on_ws_text(&mut self, ws: &mut Ws, text: &str) -> anyhow::Result<()> {
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(text) else {
            return Ok(());
        };
        let field = |name: &str| msg.get(name).cloned().unwrap_or_default();
        match msg.get("type").and_then(|t| t.as_str()) {
            Some("sessions") => {
                self.sessions = serde_json::from_value(field("sessions")).unwrap_or_default();
                self.selected = self
                    .selected
                    .min(self.sessions.len().saturating_sub(1));
                if matches!(self.mode, Mode::List) {
                    self.render_list().await?;
                }
            }
            // Server-scoped bell/title events carry a socket name; update
            // the list entry. (A `title` without one is the attached
            // session's own title change, mirrored to our terminal.)
            Some("bell") if msg.get("socket").is_some() => {
                let socket = field("socket");
                if let Some(s) = self
                    .sessions
                    .iter_mut()
                    .find(|s| Some(s.socket.as_str()) == socket.as_str())
                {
                    s.last_bell_at = field("last_bell_at").as_u64();
                }
                if matches!(self.mode, Mode::List) {
                    self.render_list().await?;
                }
            }
            Some("title") if msg.get("socket").is_some() => {
                let socket = field("socket");
                if let Some(s) = self
                    .sessions
                    .iter_mut()
                    .find(|s| Some(s.socket.as_str()) == socket.as_str())
                {
                    s.title = field("title").as_str().map(str::to_string);
                }
                if matches!(self.mode, Mode::List) {
                    self.render_list().await?;
                }
            }
            Some("title") => {
                if let (Mode::Live { .. }, Some(title)) = (&self.mode, field("title").as_str()) {
                    let clean: String = title.chars().filter(|c| !c.is_control()).collect();
                    self.stdout
                        .write_all(format!("\x1b]2;{clean}\x07").as_bytes())
                        .await?;
                    self.stdout.flush().await?;
                }
            }
            Some("connect") => {
                if matches!(self.mode, Mode::Connecting) {
                    self.session_readonly = field("readonly").as_bool().unwrap_or(false);
                    self.history_oldest = msg
                        .pointer("/history/oldest")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    send_json(ws, serde_json::json!({"type": "view"})).await?;
                    self.mode = Mode::AwaitSnapshot;
                    self.status = "attaching...".into();
                    self.render_list().await?;
                }
            }
            Some("snapshot") => self.on_snapshot(ws, &msg).await?,
            Some("output") => {
                let Some(data) = field("data").as_str().map(str::to_string) else {
                    return Ok(());
                };
                let Ok(bytes) = protocol::decode_terminal_bytes(&data) else {
                    return Ok(());
                };
                match &mut self.mode {
                    Mode::Live { .. } => self.process_live(&bytes).await?,
                    Mode::FetchingHistory { pending, .. } => pending.push(bytes),
                    _ => {}
                }
            }
            Some("history_lines") => {
                if let Mode::FetchingHistory { skip_replies, .. } = &mut self.mode {
                    if *skip_replies > 0 {
                        *skip_replies -= 1;
                        return Ok(());
                    }
                    let lines = field("lines");
                    self.go_live(lines.as_array().cloned().unwrap_or_default())
                        .await?;
                }
            }
            Some("exit") => {
                let status = field("status")
                    .as_i64()
                    .map_or("killed by a signal".to_string(), |s| format!("exit status {s}"));
                self.status = format!("session ended ({status})");
                // The wrapper closes the socket next; `disconnected`
                // finishes the detach.
            }
            Some("disconnected") => {
                if self.status.is_empty() || !self.status.starts_with("session ended") {
                    self.status = format!(
                        "disconnected ({})",
                        field("reason").as_str().unwrap_or("unknown")
                    );
                }
                self.back_to_list(ws).await?;
            }
            Some("error") => {
                let message = field("message").as_str().unwrap_or("unknown error").to_string();
                match self.mode {
                    // An error during attach setup aborts the attach.
                    Mode::Connecting | Mode::AwaitSnapshot | Mode::FetchingHistory { .. } => {
                        self.status = format!("error: {message}");
                        self.back_to_list(ws).await?;
                    }
                    Mode::List => {
                        self.status = format!("error: {message}");
                        self.render_list().await?;
                    }
                    // While live, errors (e.g. a rejected input) have no
                    // status bar to land in; drop them.
                    Mode::Live { .. } => {}
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_snapshot(&mut self, ws: &mut Ws, msg: &serde_json::Value) -> anyhow::Result<()> {
        let Some(data) = msg.get("data").and_then(|d| d.as_str()) else {
            return Ok(());
        };
        let Ok(snapshot) = protocol::decode_terminal_bytes(data) else {
            return Ok(());
        };
        let mut stream_cols = msg.get("width").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
        let mut stream_rows = msg.get("height").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
        if stream_rows == 0 || stream_cols == 0 {
            // A wrapper from before snapshots stated their dimensions: it
            // supports one viewer and sizes the stream to our init dims.
            (stream_rows, stream_cols) = (self.init_rows, self.init_cols);
        }
        let history_next = msg
            .get("history_next")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        match &mut self.mode {
            Mode::AwaitSnapshot => {
                if history_next > self.history_oldest {
                    send_json(ws, serde_json::json!({"type": "history", "before": history_next}))
                        .await?;
                    self.mode = Mode::FetchingHistory {
                        stream_rows,
                        stream_cols,
                        snapshot,
                        pending: Vec::new(),
                        skip_replies: 0,
                    };
                } else {
                    self.mode = Mode::FetchingHistory {
                        stream_rows,
                        stream_cols,
                        snapshot,
                        pending: Vec::new(),
                        skip_replies: 0,
                    };
                    self.go_live(Vec::new()).await?;
                }
            }
            // The stream's dimensions changed (a better-ranked viewer came
            // or went): restart the local view from the fresh snapshot.
            Mode::Live { mirror } => {
                let mut out = mirror.start_view(View {
                    rows: stream_rows,
                    cols: stream_cols,
                    simulated: false,
                }).host_output;
                out.extend_from_slice(&mirror.process(&snapshot).host);
                self.stdout.write_all(&out).await?;
                self.stdout.flush().await?;
            }
            // A re-snapshot while the history reply is in flight restarts
            // the stream: previous buffered output is baked into it, and
            // the splice point moved, so re-request the history.
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
                    send_json(ws, serde_json::json!({"type": "history", "before": history_next}))
                        .await?;
                } else {
                    self.go_live(Vec::new()).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Leave the alternate screen, print the fetched history into the local
    /// terminal (and thus its native scrollback), and start the live view.
    async fn go_live(&mut self, lines: Vec<serde_json::Value>) -> anyhow::Result<()> {
        let Mode::FetchingHistory {
            stream_rows,
            stream_cols,
            snapshot,
            pending,
            ..
        } = std::mem::replace(&mut self.mode, Mode::List)
        else {
            return Ok(());
        };
        let mut out = LEAVE_ALT.to_vec();
        out.extend_from_slice(SGR_RESET);
        out.extend_from_slice(b"\x1b[?7h"); // history lines rely on autowrap
        // Print history as flowing text from the bottom row, so it scrolls
        // naturally past whatever was on the screen before (usually the
        // shell prompt that launched us). A record with `wrapped` set
        // continues on the next record, re-wrapping at our own width.
        out.extend_from_slice(cup(self.rows.saturating_sub(1), 0).as_bytes());
        let mut continuation = false;
        for line in &lines {
            if !continuation {
                out.extend_from_slice(b"\r\n");
            }
            if let Some(data) = line.get("data").and_then(|d| d.as_str())
                && let Ok(bytes) = protocol::decode_terminal_bytes(data)
            {
                out.extend_from_slice(&bytes);
            }
            out.extend_from_slice(SGR_RESET);
            continuation = line
                .get("wrapped")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
        }
        // Scroll the history clear of the bottom-anchored live region.
        let region_rows = stream_rows.min(self.rows);
        out.extend_from_slice(cup(self.rows.saturating_sub(1), 0).as_bytes());
        out.extend_from_slice(&b"\r\n".repeat(usize::from(region_rows)));

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
        self.stdout.write_all(&out).await?;
        self.stdout.flush().await?;
        self.mode = Mode::Live { mirror };
        self.status = String::new();
        Ok(())
    }

    /// Feed live output through the mirror to the local terminal.
    async fn process_live(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let Mode::Live { mirror } = &mut self.mode else {
            return Ok(());
        };
        let out = mirror.process(bytes);
        self.stdout.write_all(&out.host).await?;
        if out.bells > 0 {
            self.stdout.write_all(b"\x07").await?;
        }
        self.stdout.flush().await?;
        Ok(())
    }

    /// Detach (or finish being detached) and show the session list again.
    async fn back_to_list(&mut self, ws: &mut Ws) -> anyhow::Result<()> {
        if let Mode::Live { mirror } = &self.mode {
            let mut out = mirror.cleanup();
            out.extend_from_slice(ENTER_ALT);
            self.stdout.write_all(&out).await?;
        }
        self.mode = Mode::List;
        self.session_readonly = false;
        self.render_list().await?;
        send_json(ws, serde_json::json!({"type": "list"})).await?;
        Ok(())
    }

    /// Keyboard input, interpreted per mode.
    async fn on_keys(&mut self, ws: &mut Ws, bytes: &[u8]) -> anyhow::Result<()> {
        match &self.mode {
            Mode::List => {
                for key in parse_list_keys(bytes) {
                    match key {
                        ListKey::Up => self.selected = self.selected.saturating_sub(1),
                        ListKey::Down => {
                            self.selected = (self.selected + 1)
                                .min(self.sessions.len().saturating_sub(1));
                        }
                        ListKey::Enter => {
                            if let Some(session) = self.sessions.get(self.selected) {
                                self.status = format!("connecting to {}...", session.socket);
                                send_json(
                                    ws,
                                    serde_json::json!({
                                        "type": "connect",
                                        "socket": session.socket,
                                    }),
                                )
                                .await?;
                                self.mode = Mode::Connecting;
                            }
                        }
                        ListKey::Refresh => {
                            send_json(ws, serde_json::json!({"type": "list"})).await?;
                        }
                        ListKey::Quit => {
                            self.quit = true;
                            return Ok(());
                        }
                    }
                }
                self.render_list().await?;
            }
            // Mid-attach: only Ctrl+D (abort) is meaningful.
            Mode::Connecting | Mode::AwaitSnapshot | Mode::FetchingHistory { .. } => {
                if bytes.contains(&0x04) {
                    self.status = "detached".into();
                    send_json(ws, serde_json::json!({"type": "disconnect"})).await?;
                }
            }
            Mode::Live { .. } => {
                let (input, detach) = match bytes.iter().position(|&b| b == 0x04) {
                    Some(pos) => (&bytes[..pos], true),
                    None => (bytes, false),
                };
                if !input.is_empty() && !self.token_readonly && !self.session_readonly {
                    send_json(
                        ws,
                        serde_json::json!({
                            "type": "input",
                            "data": protocol::encode_terminal_bytes(input),
                        }),
                    )
                    .await?;
                }
                if detach {
                    self.status = "detached".into();
                    send_json(ws, serde_json::json!({"type": "unview"})).await?;
                    send_json(ws, serde_json::json!({"type": "disconnect"})).await?;
                    // `disconnected` will arrive and run back_to_list; do it
                    // eagerly so the UI doesn't hang on a stalled server.
                    self.back_to_list(ws).await?;
                }
            }
        }
        Ok(())
    }

    async fn on_winch(&mut self, rows: u16, cols: u16) -> anyhow::Result<()> {
        self.rows = rows;
        self.cols = cols;
        match &mut self.mode {
            Mode::Live { mirror } => {
                let out = mirror.host_resized(rows, cols).host_output;
                self.stdout.write_all(&out).await?;
                self.stdout.flush().await?;
            }
            Mode::List => self.render_list().await?,
            _ => {}
        }
        Ok(())
    }

    /// Redraw the session list (alternate screen).
    async fn render_list(&mut self) -> anyhow::Result<()> {
        if !matches!(
            self.mode,
            Mode::List | Mode::Connecting | Mode::AwaitSnapshot
        ) {
            return Ok(());
        }
        let readonly = if self.token_readonly { " (read-only)" } else { "" };
        let mut lines = vec![
            format!("g2mirror-view \u{2014} {}{}", self.addr, readonly),
            self.status.clone(),
            String::new(),
        ];
        if self.sessions.is_empty() {
            lines.push("  no live terminals".into());
        }
        for (i, s) in self.sessions.iter().enumerate() {
            let marker = if i == self.selected { ">" } else { " " };
            let bell = s
                .last_bell_at
                .map(|at| format!("  [bell {}]", ago(at)))
                .unwrap_or_default();
            let title = s.title.as_deref().unwrap_or("");
            let line = format!(
                "{marker} {:<8} {}  {}{}",
                s.pid, s.cwd_hint, title, bell
            );
            let line: String = line.chars().take(usize::from(self.cols)).collect();
            if i == self.selected {
                lines.push(format!("\x1b[7m{line}\x1b[0m"));
            } else {
                lines.push(line);
            }
        }
        lines.push(String::new());
        lines.push("  \u{2191}/\u{2193} select \u{00b7} enter attach \u{00b7} r refresh \u{00b7} q quit \u{00b7} ^D detaches a view".into());
        let mut out = b"\x1b[H\x1b[2J".to_vec();
        out.extend_from_slice(lines.join("\r\n").as_bytes());
        self.stdout.write_all(&out).await?;
        self.stdout.flush().await?;
        Ok(())
    }
}

/// "12s"/"5m"/"2h" since a unix-ms timestamp.
fn ago(at: u64) -> String {
    let secs = now_ms().saturating_sub(at) / 1000;
    match secs {
        0..=99 => format!("{secs}s ago"),
        100..=5999 => format!("{}m ago", secs / 60),
        _ => format!("{}h ago", secs / 3600),
    }
}

enum ListKey {
    Up,
    Down,
    Enter,
    Refresh,
    Quit,
}

/// Interpret raw keyboard bytes as list-navigation keys. Unrecognized bytes
/// are dropped (this screen takes no free-form input).
fn parse_list_keys(bytes: &[u8]) -> Vec<ListKey> {
    let mut keys = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            0x03 | 0x04 | b'q' => keys.push(ListKey::Quit),
            b'\r' | b'\n' => keys.push(ListKey::Enter),
            b'k' => keys.push(ListKey::Up),
            b'j' => keys.push(ListKey::Down),
            b'r' => keys.push(ListKey::Refresh),
            0x1b if i + 2 < bytes.len() && matches!(bytes[i + 1], b'[' | b'O') => {
                match bytes[i + 2] {
                    b'A' => keys.push(ListKey::Up),
                    b'B' => keys.push(ListKey::Down),
                    _ => {}
                }
                i += 3;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    keys
}

async fn send_json(ws: &mut Ws, value: serde_json::Value) -> anyhow::Result<()> {
    ws.send(Message::text(value.to_string()))
        .await
        .context("failed to send to the server")?;
    Ok(())
}

async fn next_text(ws: &mut Ws) -> anyhow::Result<Option<String>> {
    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(t) => return Ok(Some(t.to_string())),
            Message::Close(_) => return Ok(None),
            _ => continue,
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_strings_parse() {
        assert_eq!(
            parse_connection_string("g2mirror://tok123@example.com:9000").unwrap(),
            ("tok123".into(), "ws://example.com:9000".into())
        );
        assert_eq!(
            parse_connection_string("g2mirror://tok@example.com").unwrap(),
            ("tok".into(), format!("ws://example.com:{DEFAULT_PORT}"))
        );
        assert_eq!(
            parse_connection_string("g2mirror://tok@[::1]").unwrap(),
            ("tok".into(), format!("ws://[::1]:{DEFAULT_PORT}"))
        );
        assert_eq!(
            parse_connection_string("g2mirror://tok@[::1]:9000").unwrap(),
            ("tok".into(), "ws://[::1]:9000".into())
        );
        // The TLS form, e.g. for a tailscale funnel hostname.
        assert_eq!(
            parse_connection_string("g2mirrors://tok@node.tail.ts.net").unwrap(),
            ("tok".into(), "wss://node.tail.ts.net:443".into())
        );
        assert_eq!(
            parse_connection_string("g2mirrors://tok@node.tail.ts.net:8443").unwrap(),
            ("tok".into(), "wss://node.tail.ts.net:8443".into())
        );
        assert!(parse_connection_string("ws://tok@host").is_err());
        assert!(parse_connection_string("g2mirror://hostonly").is_err());
        assert!(parse_connection_string("g2mirror://@host").is_err());
    }

    #[test]
    fn list_keys_parse() {
        let keys = parse_list_keys(b"\x1b[A\x1b[Bjkr\rq\x03");
        let repr: Vec<&str> = keys
            .iter()
            .map(|k| match k {
                ListKey::Up => "up",
                ListKey::Down => "down",
                ListKey::Enter => "enter",
                ListKey::Refresh => "refresh",
                ListKey::Quit => "quit",
            })
            .collect();
        assert_eq!(
            repr,
            vec!["up", "down", "down", "up", "refresh", "enter", "quit", "quit"]
        );
    }
}
