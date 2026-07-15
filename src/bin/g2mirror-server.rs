//! g2mirror-server: websocket gateway between device drivers (e.g. the
//! smart-glasses driver) and g2mirror session sockets in ~/.g2mirror.
//!
//! Transport security is out of scope: run it on a loopback/tailscale
//! address (from config.json) and tunnel as needed. Devices authenticate
//! with a token whose SHA-256 hash is stored in the config.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _};
use g2mirror::paths;
use g2mirror::protocol::{DeviceInit, ServerToDevice, SessionInfo, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpStream, UnixStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// How often to scan ~/.g2mirror for new session sockets to monitor.
const MONITOR_SCAN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Per-terminal state learned through the monitor connections.
#[derive(Default, Clone)]
struct TerminalState {
    /// Last bell (unix ms), if one has rung since monitoring began.
    last_bell_at: Option<u64>,
    /// Window title, if the app has set one since monitoring began.
    title: Option<String>,
    /// Real working directory, from the session's connect greeting (the
    /// socket name only carries a sanitized, truncated form).
    cwd: Option<String>,
}

/// Terminal tracking, shared between the monitor tasks (one per session
/// socket, connected regardless of whether any device is attached) and the
/// device connections.
struct BellState {
    terminals: std::sync::Mutex<HashMap<String, TerminalState>>,
    /// Socket names that currently have a live monitor task.
    monitored: std::sync::Mutex<HashSet<String>>,
    /// Bell/title events fanned out to every device connection.
    event_tx: tokio::sync::broadcast::Sender<ServerToDevice>,
}

impl BellState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            terminals: std::sync::Mutex::new(HashMap::new()),
            monitored: std::sync::Mutex::new(HashSet::new()),
            event_tx: tokio::sync::broadcast::channel(256).0,
        })
    }

    fn terminal(&self, socket: &str) -> TerminalState {
        self.terminals
            .lock()
            .unwrap()
            .get(socket)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct TokenConfig {
    /// Name identifying this token in `size_precedence` and to humans.
    name: String,
    /// Lowercase hex SHA-256 of the token.
    token_hash: String,
    /// When true (the default), reject all input from connections
    /// authenticated with this token, regardless of what the individual
    /// sessions allow.
    #[serde(default = "default_token_readonly")]
    readonly: bool,
    /// Terminals this token may see and connect to: visible when ANY rule
    /// matches (within one rule, every present field must match). Empty:
    /// every terminal. Enforced on list, connect, and bell/title pushes;
    /// an attached terminal that stops matching (title change) is
    /// force-disconnected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    filter: Vec<FilterRule>,
}

fn default_token_readonly() -> bool {
    true
}

/// One filter rule. Regexes must match the whole value (they are anchored
/// at both ends). Unknown keys are rejected so a typo can't silently turn
/// a rule vacuous.
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct FilterRule {
    /// Matches the session's real working directory (from its `connect`
    /// greeting; until the server has monitored the session, path rules
    /// fail closed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    /// Matches the terminal's current window title; terminals without a
    /// title don't match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    windowtitle: Option<String>,
}

/// A token's filter with its regexes compiled (empty = unrestricted).
struct CompiledRule {
    path: Option<regex::Regex>,
    windowtitle: Option<regex::Regex>,
}

fn anchored(pattern: &str) -> anyhow::Result<regex::Regex> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .with_context(|| format!("invalid filter regex {pattern:?}"))
}

fn compile_filter(token: &TokenConfig) -> anyhow::Result<Vec<CompiledRule>> {
    token
        .filter
        .iter()
        .map(|rule| {
            anyhow::ensure!(
                rule.path.is_some() || rule.windowtitle.is_some(),
                "token {:?} has a filter rule with neither path nor windowtitle",
                token.name
            );
            Ok(CompiledRule {
                path: rule.path.as_deref().map(anchored).transpose()?,
                windowtitle: rule.windowtitle.as_deref().map(anchored).transpose()?,
            })
        })
        .collect()
}

/// Whether a terminal with this cwd/title is visible through a filter.
fn filter_allows(rules: &[CompiledRule], cwd: Option<&str>, title: Option<&str>) -> bool {
    if rules.is_empty() {
        return true;
    }
    rules.iter().any(|rule| {
        rule.path
            .as_ref()
            .is_none_or(|re| cwd.is_some_and(|c| re.is_match(c)))
            && rule
                .windowtitle
                .as_ref()
                .is_none_or(|re| title.is_some_and(|t| re.is_match(t)))
    })
}

/// Whether `socket` is visible through a filter, per the monitored state.
fn session_allowed(rules: &[CompiledRule], state: &BellState, socket: &str) -> bool {
    if rules.is_empty() {
        return true;
    }
    let terminal = state.terminal(socket);
    filter_allows(rules, terminal.cwd.as_deref(), terminal.title.as_deref())
}

#[derive(Serialize, Deserialize)]
struct Config {
    /// Address to listen on. The server trusts this to be non-public
    /// (loopback or a tailscale address); it warns on 0.0.0.0/::.
    listen_addr: String,
    port: u16,
    /// Tokens accepted for authentication. Manage with `--add-token`.
    #[serde(default)]
    auth_tokens: Vec<TokenConfig>,
    /// Ordered size policy: token names and "host". When several clients
    /// view one terminal at once, the wrapped app is sized to whichever
    /// connected viewer's token comes earliest; if "host" comes before all
    /// of them, the app stays at the host terminal's size and viewers get
    /// a host-sized stream. Unlisted tokens rank after every listed entry,
    /// and the host — when unlisted — ranks after unlisted tokens, so with
    /// no list at all any viewer resizes the app (the original behavior).
    #[serde(default)]
    size_precedence: Vec<String>,
    /// Legacy single-token form: equivalent to an `auth_tokens` entry named
    /// "default" whose readonly flag is `readonly` (defaulting to false, as
    /// it did when this was the only form).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_token_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    readonly: Option<bool>,
}

impl Config {
    /// All accepted tokens, with the legacy single-token fields folded in.
    fn tokens(&self) -> Vec<TokenConfig> {
        let mut tokens = self.auth_tokens.clone();
        if let Some(hash) = &self.auth_token_hash {
            tokens.push(TokenConfig {
                name: "default".into(),
                token_hash: hash.clone(),
                readonly: self.readonly.unwrap_or(false),
                filter: Vec::new(),
            });
        }
        tokens
    }

    /// (viewer rank, host rank) in the size-precedence order for a token
    /// name; lower wins. See the `size_precedence` field for the ordering
    /// rules this implements.
    fn size_ranks(&self, token_name: &str) -> (u32, u32) {
        let len = self.size_precedence.len() as u32;
        let position = |name: &str| {
            self.size_precedence
                .iter()
                .position(|e| e == name)
                .map(|p| p as u32)
        };
        let viewer = position(token_name).unwrap_or(len);
        let host = position("host").unwrap_or(len + 1);
        (viewer, host)
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("--init-config") => init_config(),
        Some("--add-token") => add_token(&args[1..]),
        Some(other) => {
            eprintln!("unknown argument: {other}");
            eprintln!("usage: g2mirror-server [--init-config | --add-token <name> [--writable]]");
            std::process::exit(2);
        }
        None => serve(),
    };
    if let Err(e) = result {
        eprintln!("g2mirror-server: {e:#}");
        std::process::exit(1);
    }
}

fn generate_token() -> anyhow::Result<String> {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).context("failed to generate random token")?;
    Ok(hex(&raw))
}

/// Generate a fresh auth token, print it once, and write config.json with
/// its hash and localhost defaults.
fn init_config() -> anyhow::Result<()> {
    let dir = paths::g2mirror_dir()?;
    let path = paths::config_path(&dir);
    anyhow::ensure!(
        !path.exists(),
        "{} already exists; delete it first to regenerate",
        path.display()
    );
    let token = generate_token()?;
    let config = Config {
        listen_addr: "127.0.0.1".into(),
        port: 8737,
        auth_tokens: vec![TokenConfig {
            name: "glasses".into(),
            token_hash: hex(&sha2::Sha256::digest(token.as_bytes())),
            readonly: false,
            filter: Vec::new(),
        }],
        size_precedence: vec!["glasses".into(), "host".into()],
        auth_token_hash: None,
        readonly: None,
    };
    std::fs::write(&path, serde_json::to_string_pretty(&config)? + "\n")?;
    println!("wrote {}", path.display());
    println!("auth token \"glasses\" (save it now; only the hash is stored):");
    println!("{token}");
    println!();
    println!("add tokens for other viewers with: g2mirror-server --add-token <name>");
    println!("start the server by running: g2mirror-server");
    Ok(())
}

/// Generate a token for another viewer (read-only unless --writable), print
/// it once, and append its hash to the config.
fn add_token(args: &[String]) -> anyhow::Result<()> {
    let mut name: Option<&str> = None;
    let mut writable = false;
    for arg in args {
        match arg.as_str() {
            "--writable" => writable = true,
            other if !other.starts_with('-') && name.is_none() => name = Some(other),
            other => anyhow::bail!("unexpected argument: {other}"),
        }
    }
    let Some(name) = name else {
        anyhow::bail!("usage: g2mirror-server --add-token <name> [--writable]");
    };
    anyhow::ensure!(
        name != "host",
        "\"host\" is reserved (it stands for the host terminal in size_precedence)"
    );
    let dir = paths::g2mirror_dir()?;
    let path = paths::config_path(&dir);
    let mut config: Config = serde_json::from_str(
        &std::fs::read_to_string(&path).with_context(|| {
            format!(
                "failed to read {}; run g2mirror-server --init-config to create it",
                path.display()
            )
        })?,
    )
    .with_context(|| format!("failed to parse {}", path.display()))?;
    anyhow::ensure!(
        !config.tokens().iter().any(|t| t.name == name),
        "a token named \"{name}\" already exists"
    );
    let token = generate_token()?;
    config.auth_tokens.push(TokenConfig {
        name: name.into(),
        token_hash: hex(&sha2::Sha256::digest(token.as_bytes())),
        readonly: !writable,
        filter: Vec::new(),
    });
    std::fs::write(&path, serde_json::to_string_pretty(&config)? + "\n")?;
    println!("auth token \"{name}\" (save it now; only the hash is stored):");
    println!("{token}");
    println!();
    println!(
        "this token is {}; it is unlisted in size_precedence, so its viewers",
        if writable { "read/write" } else { "read-only" }
    );
    println!("never resize the wrapped app — edit {} to change either", path.display());
    println!("restart g2mirror-server to pick up the change");
    Ok(())
}

fn serve() -> anyhow::Result<()> {
    let dir = paths::g2mirror_dir()?;
    let config_file = paths::config_path(&dir);
    let config: Config = serde_json::from_str(
        &std::fs::read_to_string(&config_file).with_context(|| {
            format!(
                "failed to read {}; run g2mirror-server --init-config to create it",
                config_file.display()
            )
        })?,
    )
    .with_context(|| format!("failed to parse {}", config_file.display()))?;
    anyhow::ensure!(
        !config.tokens().is_empty(),
        "{} defines no auth tokens; run g2mirror-server --init-config",
        config_file.display()
    );
    // Compile every token's filter up front so a bad regex fails at
    // startup, not at authentication time.
    let filters: HashMap<String, Vec<CompiledRule>> = config
        .tokens()
        .iter()
        .map(|t| Ok((t.name.clone(), compile_filter(t)?)))
        .collect::<anyhow::Result<_>>()?;

    for path in paths::cleanup_stale_sockets(&dir)? {
        eprintln!("removed stale session socket {}", path.display());
    }

    if let Ok(addr) = config.listen_addr.parse::<std::net::IpAddr>()
        && addr.is_unspecified() {
            eprintln!(
                "warning: listening on {} exposes the server on all interfaces; \
                 prefer a loopback or tailscale address",
                addr
            );
        }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind((config.listen_addr.as_str(), config.port))
            .await
            .with_context(|| {
                format!("failed to bind {}:{}", config.listen_addr, config.port)
            })?;
        // Parsed by tooling; keep the format stable.
        println!("g2mirror-server listening on {}", listener.local_addr()?);
        let config = Arc::new(config);
        let dir = Arc::new(dir);
        let filters = Arc::new(filters);
        let state = BellState::new();
        tokio::spawn(monitor_manager((*dir).clone(), state.clone()));
        loop {
            let (stream, peer) = listener.accept().await?;
            let config = config.clone();
            let dir = dir.clone();
            let filters = filters.clone();
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_device(stream, &config, &filters, &dir, &state).await {
                    eprintln!("connection from {peer}: {e:#}");
                }
            });
        }
    })
}

/// Keep a monitor connection open to every live session socket so bells are
/// tracked regardless of device connections. These connections don't count
/// as viewers on the wrapper side.
async fn monitor_manager(dir: PathBuf, state: Arc<BellState>) {
    loop {
        let live = live_session_sockets(&dir);
        {
            let mut terminals = state.terminals.lock().unwrap();
            // Forget terminals whose socket is gone; remember new ones.
            terminals.retain(|name, _| live.contains(name));
            for name in &live {
                terminals.entry(name.clone()).or_default();
            }
        }
        for name in live {
            if state.monitored.lock().unwrap().insert(name.clone()) {
                tokio::spawn(monitor_session(dir.join(&name), name, state.clone()));
            }
        }
        tokio::time::sleep(MONITOR_SCAN_INTERVAL).await;
    }
}

async fn monitor_session(path: PathBuf, name: String, state: Arc<BellState>) {
    let _ = run_monitor(&path, &name, &state).await;
    // On any exit (wrapper gone, I/O error) release the slot; the next scan
    // reconnects if the socket still exists.
    state.monitored.lock().unwrap().remove(&name);
}

async fn run_monitor(path: &Path, name: &str, state: &BellState) -> anyhow::Result<()> {
    let stream = UnixStream::connect(path).await?;
    let mut conn = SessionConn {
        stream,
        buf: Vec::new(),
        name: name.to_string(),
    };
    conn.send_line(
        &serde_json::json!({"type": "monitor", "version": PROTOCOL_VERSION}).to_string(),
    )
    .await?;
    while let Some(line) = conn.next_line().await? {
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let event = match msg.get("type").and_then(|t| t.as_str()) {
            // The connect greeting carries the real working directory,
            // which token filters match against.
            Some("connect") => {
                if let Some(cwd) = msg.get("cwd").and_then(|c| c.as_str()) {
                    let mut terminals = state.terminals.lock().unwrap();
                    terminals.entry(name.to_string()).or_default().cwd = Some(cwd.to_string());
                }
                continue;
            }
            Some("bell") => {
                let Some(at) = msg.get("at").and_then(serde_json::Value::as_u64) else {
                    continue;
                };
                let mut terminals = state.terminals.lock().unwrap();
                terminals.entry(name.to_string()).or_default().last_bell_at = Some(at);
                ServerToDevice::Bell {
                    socket: name.to_string(),
                    last_bell_at: at,
                }
            }
            Some("title") => {
                let Some(title) = msg.get("title").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let mut terminals = state.terminals.lock().unwrap();
                terminals.entry(name.to_string()).or_default().title = Some(title.to_string());
                ServerToDevice::Title {
                    socket: name.to_string(),
                    title: title.to_string(),
                }
            }
            _ => continue,
        };
        // Errors just mean no device is connected right now.
        let _ = state.event_tx.send(event);
    }
    Ok(())
}

async fn handle_device(
    stream: TcpStream,
    config: &Config,
    filters: &HashMap<String, Vec<CompiledRule>>,
    dir: &Path,
    state: &BellState,
) -> anyhow::Result<()> {
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .context("websocket handshake failed")?;

    // First message must be init with a valid auth token.
    let init: DeviceInit = match next_text(&mut ws).await? {
        Some(text) => serde_json::from_str(&text).context("first message must be init")?,
        None => return Ok(()),
    };
    let token = config
        .tokens()
        .into_iter()
        .find(|t| token_matches(&init.auth_token, &t.token_hash));
    let token = match token {
        Some(token) if init.msg_type == "init" && init.version == PROTOCOL_VERSION => token,
        _ => {
            let message = if init.msg_type != "init" {
                "first message must be init".into()
            } else if init.version != PROTOCOL_VERSION {
                format!("unsupported protocol version {}", init.version)
            } else {
                "authentication failed".into()
            };
            send(&mut ws, &ServerToDevice::Error { message }).await?;
            ws.close(None).await?;
            return Ok(());
        }
    };
    send(
        &mut ws,
        &ServerToDevice::Init {
            version: PROTOCOL_VERSION,
            readonly: token.readonly,
        },
    )
    .await?;
    let filter = filters.get(&token.name).map(Vec::as_slice).unwrap_or(&[]);

    let mut session: Option<SessionConn> = None;
    let mut event_rx = state.event_tx.subscribe();
    loop {
        tokio::select! {
            msg = ws.next() => {
                let Some(msg) = msg else { break };
                let Message::Text(text) = msg? else { continue };
                handle_device_message(
                    &text, &mut ws, &mut session, &init, dir, state, config, &token, filter,
                ).await?;
            }
            line = async { session.as_mut().unwrap().next_line().await },
                    if session.is_some() => {
                match line {
                    Ok(Some(line)) => ws.send(Message::text(line)).await?,
                    // Session ended (wrapper exited or I/O error).
                    Ok(None) | Err(_) => {
                        session = None;
                        send(&mut ws, &ServerToDevice::Disconnected {
                            reason: "session closed".into(),
                        }).await?;
                    }
                }
            }
            // A terminal rang its bell or changed its title (viewed or
            // not): notify the device — unless the token's filter hides
            // that terminal.
            ev = event_rx.recv() => {
                if let Ok(event) = ev {
                    let socket = match &event {
                        ServerToDevice::Bell { socket, .. }
                        | ServerToDevice::Title { socket, .. } => Some(socket.clone()),
                        _ => None,
                    };
                    match socket {
                        Some(s) if !session_allowed(filter, state, &s) => {
                            // A title change can also revoke visibility of
                            // the terminal the device is attached to.
                            if session.as_ref().is_some_and(|c| c.name == s) {
                                session = None;
                                send(&mut ws, &ServerToDevice::Disconnected {
                                    reason:
                                        "terminal no longer matches your token's filter".into(),
                                }).await?;
                            }
                        }
                        _ => send(&mut ws, &event).await?,
                    }
                }
                // Lagged receivers just miss old events; `list` resyncs.
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_device_message(
    text: &str,
    ws: &mut WebSocketStream<TcpStream>,
    session: &mut Option<SessionConn>,
    init: &DeviceInit,
    dir: &Path,
    state: &BellState,
    config: &Config,
    token: &TokenConfig,
    filter: &[CompiledRule],
) -> anyhow::Result<()> {
    let parsed: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return send(
                ws,
                &ServerToDevice::Error {
                    message: format!("invalid JSON: {e}"),
                },
            )
            .await;
        }
    };
    match parsed.get("type").and_then(|t| t.as_str()) {
        Some("list") => {
            let sessions = list_sessions(dir, state, filter);
            send(ws, &ServerToDevice::Sessions { sessions }).await
        }
        Some("connect") => {
            let Some(name) = parsed.get("socket").and_then(|s| s.as_str()) else {
                return send(
                    ws,
                    &ServerToDevice::Error {
                        message: "connect requires a socket name".into(),
                    },
                )
                .await;
            };
            if !paths::is_valid_socket_name(name) {
                return send(
                    ws,
                    &ServerToDevice::Error {
                        message: "invalid socket name".into(),
                    },
                )
                .await;
            }
            if !session_allowed(filter, state, name) {
                return send(
                    ws,
                    &ServerToDevice::Error {
                        message: "terminal does not match your token's filter".into(),
                    },
                )
                .await;
            }
            let (size_rank, host_size_rank) = config.size_ranks(&token.name);
            match SessionConn::open(&dir.join(name), name, init, size_rank, host_size_rank).await {
                Ok(conn) => {
                    *session = Some(conn);
                    Ok(())
                }
                Err(e) => {
                    send(
                        ws,
                        &ServerToDevice::Error {
                            message: format!("connect failed: {e:#}"),
                        },
                    )
                    .await
                }
            }
        }
        Some("disconnect") => {
            *session = None;
            send(
                ws,
                &ServerToDevice::Disconnected {
                    reason: "requested".into(),
                },
            )
            .await
        }
        // Input is normally forwarded like any other message, but a
        // read-only token is refused here (sessions can independently
        // refuse via their own --readonly flag).
        Some("input") if token.readonly => {
            send(
                ws,
                &ServerToDevice::Error {
                    message: "server is read-only".into(),
                },
            )
            .await
        }
        // Anything else is forwarded verbatim to the connected session
        // (view, unview, future message types).
        Some(_) => match session {
            Some(conn) => conn.send_line(text).await,
            None => {
                send(
                    ws,
                    &ServerToDevice::Error {
                        message: "not connected to a session".into(),
                    },
                )
                .await
            }
        },
        None => {
            send(
                ws,
                &ServerToDevice::Error {
                    message: "message has no type".into(),
                },
            )
            .await
        }
    }
}

/// Session socket names whose file looks valid and whose wrapper PID is
/// alive.
fn live_session_sockets(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .filter(|name| {
            paths::is_valid_socket_name(name)
                && paths::socket_pid(name).is_some_and(paths::pid_exists)
        })
        .collect()
}

fn list_sessions(dir: &Path, state: &BellState, filter: &[CompiledRule]) -> Vec<SessionInfo> {
    live_session_sockets(dir)
        .into_iter()
        .filter(|name| session_allowed(filter, state, name))
        .map(|name| {
            let terminal = state.terminal(&name);
            SessionInfo {
                pid: paths::socket_pid(&name).unwrap_or(0),
                cwd_hint: name.split_once('-').map(|(_, p)| p).unwrap_or("").to_string(),
                last_bell_at: terminal.last_bell_at,
                title: terminal.title,
                socket: name,
            }
        })
        .collect()
}

/// A connection to a wrapper's session socket, speaking newline-delimited
/// JSON. Lines are relayed verbatim in both directions.
struct SessionConn {
    stream: UnixStream,
    buf: Vec<u8>,
    /// Socket name this connection is attached to (used to re-check the
    /// token's filter when the terminal's title changes).
    name: String,
}

impl SessionConn {
    /// Connect and send the session init derived from the device's init,
    /// annotated with the size-precedence ranks of the device's token and
    /// of the host terminal.
    async fn open(
        path: &PathBuf,
        name: &str,
        init: &DeviceInit,
        size_rank: u32,
        host_size_rank: u32,
    ) -> anyhow::Result<Self> {
        let stream = UnixStream::connect(path)
            .await
            .with_context(|| format!("cannot connect to {}", path.display()))?;
        let mut conn = Self {
            stream,
            buf: Vec::new(),
            name: name.to_string(),
        };
        let init_line = serde_json::to_string(&serde_json::json!({
            "type": "init",
            "version": PROTOCOL_VERSION,
            "device": init.device,
            "width": init.width,
            "height": init.height,
            "size_rank": size_rank,
            "host_size_rank": host_size_rank,
        }))?;
        conn.send_line(&init_line).await?;
        Ok(conn)
    }

    async fn send_line(&mut self, line: &str) -> anyhow::Result<()> {
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.write_all(b"\n").await?;
        Ok(())
    }

    /// Next complete line; Ok(None) on EOF. Cancel-safe (partial lines stay
    /// buffered).
    async fn next_line(&mut self) -> std::io::Result<Option<String>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let line = String::from_utf8_lossy(&line[..line.len() - 1]).into_owned();
                if line.is_empty() {
                    continue;
                }
                return Ok(Some(line));
            }
            let mut chunk = [0u8; 16 * 1024];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

async fn next_text(ws: &mut WebSocketStream<TcpStream>) -> anyhow::Result<Option<String>> {
    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(t) => return Ok(Some(t.to_string())),
            Message::Close(_) => return Ok(None),
            _ => continue,
        }
    }
    Ok(None)
}

async fn send(ws: &mut WebSocketStream<TcpStream>, msg: &ServerToDevice) -> anyhow::Result<()> {
    ws.send(Message::text(serde_json::to_string(msg)?)).await?;
    Ok(())
}

fn token_matches(token: &str, expected_hex: &str) -> bool {
    let digest = sha2::Sha256::digest(token.as_bytes());
    constant_time_eq(
        hex(&digest).as_bytes(),
        expected_hex.trim().to_ascii_lowercase().as_bytes(),
    )
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_single_token_config_still_works() {
        let config: Config = serde_json::from_str(
            r#"{"listen_addr": "127.0.0.1", "port": 8737,
                "auth_token_hash": "abc123", "readonly": true}"#,
        )
        .unwrap();
        let tokens = config.tokens();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].name, "default");
        assert_eq!(tokens[0].token_hash, "abc123");
        assert!(tokens[0].readonly);
        // Without the legacy readonly flag, the legacy token is writable
        // (matching the old default).
        let config: Config = serde_json::from_str(
            r#"{"listen_addr": "127.0.0.1", "port": 8737, "auth_token_hash": "abc123"}"#,
        )
        .unwrap();
        assert!(!config.tokens()[0].readonly);
        // No size_precedence: any viewer outranks the host.
        let (viewer, host) = config.size_ranks("default");
        assert!(viewer < host);
    }

    #[test]
    fn token_readonly_defaults_to_true() {
        let config: Config = serde_json::from_str(
            r#"{"listen_addr": "127.0.0.1", "port": 8737,
                "auth_tokens": [
                  {"name": "glasses", "token_hash": "aa", "readonly": false},
                  {"name": "spectator", "token_hash": "bb"}
                ]}"#,
        )
        .unwrap();
        let tokens = config.tokens();
        assert!(!tokens[0].readonly);
        assert!(tokens[1].readonly, "readonly must default to true");
    }

    #[test]
    fn filters_parse_compile_and_match() {
        // The documented config shape: one key per rule, several rules.
        let config: Config = serde_json::from_str(
            r#"{"listen_addr": "127.0.0.1", "port": 8737,
                "auth_tokens": [
                  {"name": "robert", "token_hash": "aa", "filter": [
                    {"path": "/Users/jb/repositories/lightcone-commons.*"},
                    {"windowtitle": ".*SHARED.*"}
                  ]},
                  {"name": "glasses", "token_hash": "bb"}
                ]}"#,
        )
        .unwrap();
        let rules = compile_filter(&config.tokens()[0]).unwrap();
        assert_eq!(rules.len(), 2);
        assert!(compile_filter(&config.tokens()[1]).unwrap().is_empty());

        // Rules OR together; either the path or the title may admit.
        let allows = |cwd, title| filter_allows(&rules, cwd, title);
        assert!(allows(Some("/Users/jb/repositories/lightcone-commons"), None));
        assert!(allows(Some("/Users/jb/repositories/lightcone-commons/sub"), None));
        assert!(allows(Some("/private"), Some("review SHARED with team")));
        assert!(!allows(Some("/private"), Some("secret notes")));
        // Unknown cwd/title fail closed.
        assert!(!allows(None, None));
        // Regexes are anchored: a partial match is not a match.
        assert!(!allows(Some("/mnt/Users/jb/repositories/lightcone-commonsx"), None));
        assert!(!allows(Some("/Users/jb"), None));

        // No filter at all: everything is visible.
        assert!(filter_allows(&[], None, None));

        // Within one rule, all present fields must match.
        let both: Config = serde_json::from_str(
            r#"{"listen_addr": "a", "port": 1, "auth_tokens": [
                  {"name": "t", "token_hash": "aa",
                   "filter": [{"path": "/shared.*", "windowtitle": ".*SHARED.*"}]}
                ]}"#,
        )
        .unwrap();
        let rules = compile_filter(&both.tokens()[0]).unwrap();
        assert!(filter_allows(&rules, Some("/shared/x"), Some("a SHARED b")));
        assert!(!filter_allows(&rules, Some("/shared/x"), Some("private")));
        assert!(!filter_allows(&rules, Some("/other"), Some("a SHARED b")));
    }

    #[test]
    fn bad_filters_fail_at_parse_or_compile_time() {
        // A typo'd key must not silently produce a match-all rule.
        let typo = serde_json::from_str::<Config>(
            r#"{"listen_addr": "a", "port": 1, "auth_tokens": [
                  {"name": "t", "token_hash": "aa", "filter": [{"windowtitel": "x"}]}
                ]}"#,
        );
        assert!(typo.is_err(), "unknown filter keys must be rejected");

        // An empty rule and a bad regex fail when the filter is compiled.
        for filter in [r#"[{}]"#, r#"[{"path": "("}]"#] {
            let config: Config = serde_json::from_str(&format!(
                r#"{{"listen_addr": "a", "port": 1, "auth_tokens": [
                      {{"name": "t", "token_hash": "aa", "filter": {filter}}}
                    ]}}"#,
            ))
            .unwrap();
            assert!(compile_filter(&config.tokens()[0]).is_err(), "{filter}");
        }
    }

    #[test]
    fn size_ranks_follow_the_precedence_list() {
        let config: Config = serde_json::from_str(
            r#"{"listen_addr": "127.0.0.1", "port": 8737,
                "auth_tokens": [{"name": "glasses", "token_hash": "aa"}],
                "size_precedence": ["glasses", "host", "spectator"]}"#,
        )
        .unwrap();
        assert_eq!(config.size_ranks("glasses"), (0, 1));
        assert_eq!(config.size_ranks("spectator"), (2, 1));
        // Unlisted tokens rank after every listed entry.
        let (viewer, host) = config.size_ranks("other");
        assert_eq!((viewer, host), (3, 1));
        // Host unlisted: ranks after unlisted tokens.
        let config: Config = serde_json::from_str(
            r#"{"listen_addr": "127.0.0.1", "port": 8737,
                "size_precedence": ["glasses"]}"#,
        )
        .unwrap();
        assert_eq!(config.size_ranks("glasses"), (0, 2));
        assert_eq!(config.size_ranks("other"), (1, 2));
    }
}
