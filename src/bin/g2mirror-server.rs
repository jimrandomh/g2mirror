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
use g2mirror::{jsonc, paths};
use g2mirror::protocol::{DeviceInit, ServerToDevice, SessionInfo, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpStream, UnixStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// How often to scan ~/.g2mirror for new session sockets to monitor.
const MONITOR_SCAN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// A connection must complete the websocket handshake and authenticate
/// within this long, so half-open connections to a public (e.g. tailscale
/// funnel) endpoint can't linger.
const AUTH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
/// Cap on connections that have not authenticated yet, across all
/// listeners. Authenticated device connections are not counted.
const MAX_UNAUTHENTICATED: usize = 32;
/// Idle keepalive: websocket pings at this interval stop NAT/proxy
/// middleboxes (e.g. on the funnel path) from reaping quiet connections.
const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

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
    /// Headless session with no host-role client: claimable with
    /// `g2mirror --attach`.
    detached: bool,
    /// Launch preset the session was started from, if any.
    launched: Option<String>,
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
    /// Launch presets this token may start: `true` (all), or a list of
    /// preset names. Absent/`false`: none. A launch grant is remote code
    /// execution by design — give it only to tokens you'd hand a shell —
    /// and requires the token to be writable (checked at startup).
    #[serde(default, skip_serializing_if = "LaunchGrant::is_none")]
    launch: LaunchGrant,
}

fn default_token_readonly() -> bool {
    true
}

/// A token's launch permission: a boolean or a list of preset names.
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum LaunchGrant {
    Flag(bool),
    Named(Vec<String>),
}

impl Default for LaunchGrant {
    fn default() -> Self {
        LaunchGrant::Flag(false)
    }
}

impl LaunchGrant {
    fn allows(&self, preset: &str) -> bool {
        match self {
            LaunchGrant::Flag(all) => *all,
            LaunchGrant::Named(names) => names.iter().any(|n| n == preset),
        }
    }

    fn is_none(&self) -> bool {
        match self {
            LaunchGrant::Flag(all) => !all,
            LaunchGrant::Named(names) => names.is_empty(),
        }
    }
}

/// One entry in the config's `launch` map: what a `launch` request for
/// this preset name starts. The wire can only name presets — argv, env,
/// and (unless `allow_cwd`) the working directory come from this laptop-
/// local config, never from the network.
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct LaunchPreset {
    /// Command and arguments; argv[0] is resolved via PATH.
    argv: Vec<String>,
    /// Working directory (leading `~` expanded); default: home.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    /// Initial window title; default: the preset name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    /// Extra environment, merged over the server's own.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    env: HashMap<String, String>,
    /// Allow the launch request to override `cwd`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    allow_cwd: bool,
    /// History lines retained (wrapper --scrollback).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scrollback: Option<u32>,
    /// Start the wrapper read-only (broadcast-style presets).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    readonly: bool,
    /// Initial pty size as `<cols>x<rows>` before any viewer resizes it;
    /// default 80x24.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size: Option<String>,
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

/// One listen address or several (e.g. loopback for a tailscale funnel
/// proxy plus the tailscale IP for direct tailnet clients).
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum ListenAddrs {
    One(String),
    Many(Vec<String>),
}

impl ListenAddrs {
    fn iter(&self) -> impl Iterator<Item = &str> {
        match self {
            ListenAddrs::One(addr) => std::slice::from_ref(addr),
            ListenAddrs::Many(addrs) => addrs.as_slice(),
        }
        .iter()
        .map(String::as_str)
    }
}

#[derive(Serialize, Deserialize)]
struct Config {
    /// Address(es) to listen on: a string or an array of strings. The
    /// server trusts these to be non-public (loopback or a tailscale
    /// address — public reachability is tailscale funnel's job); it warns
    /// on 0.0.0.0/::.
    listen_addr: ListenAddrs,
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
    /// Named launch presets: commands a suitably-granted token may start
    /// as detached (headless) sessions. A `launch` request without a name
    /// uses the preset called "shell".
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    launch: std::collections::BTreeMap<String, LaunchPreset>,
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
                launch: LaunchGrant::default(),
            });
        }
        tokens
    }

    /// Startup validation of the launch configuration: contradictory or
    /// dangling grants and malformed presets are config errors, not
    /// launch-time surprises.
    fn validate_launch(&self) -> anyhow::Result<()> {
        for (name, preset) in &self.launch {
            anyhow::ensure!(!preset.argv.is_empty(), "launch preset {name:?} has an empty argv");
            if let Some(size) = &preset.size {
                anyhow::ensure!(
                    parse_preset_size(size),
                    "launch preset {name:?} has a bad size {size:?} (want e.g. 80x24)"
                );
            }
        }
        for token in self.tokens() {
            if token.launch.is_none() {
                continue;
            }
            anyhow::ensure!(
                !token.readonly,
                "token {:?} is read-only but has a launch grant; a token that can \
                 start shells it cannot type into is a contradiction \u{2014} make it \
                 writable or drop the grant",
                token.name
            );
            if let LaunchGrant::Named(names) = &token.launch {
                for name in names {
                    anyhow::ensure!(
                        self.launch.contains_key(name),
                        "token {:?} may launch {name:?}, but no such launch preset exists",
                        token.name
                    );
                }
            }
        }
        Ok(())
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

/// Read a config file: JSON with `//` and `/* */` comments allowed.
fn load_config(path: &Path) -> anyhow::Result<Config> {
    parse_config(&read_config_text(path)?).with_context(|| format!("failed to parse {}", path.display()))
}

fn read_config_text(path: &Path) -> anyhow::Result<String> {
    std::fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read {}; run g2mirror-server --init-config to create it",
            path.display()
        )
    })
}

fn parse_config(text: &str) -> anyhow::Result<Config> {
    Ok(serde_json::from_str(&jsonc::strip_comments(text))?)
}

/// `<cols>x<rows>`, both positive.
fn parse_preset_size(s: &str) -> bool {
    s.split_once('x')
        .and_then(|(c, r)| Some((c.parse::<u16>().ok()?, r.parse::<u16>().ok()?)))
        .is_some_and(|(c, r)| c > 0 && r > 0)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("--init-config") => init_config(),
        Some("--add-token") => add_token(&args[1..]),
        Some(other) => {
            eprintln!("unknown argument: {other}");
            eprintln!(
                "usage: g2mirror-server [--init-config | --add-token <name> \
                 [--writable] [--launch <preset>]... [--launch-all]]"
            );
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
    // Seed a "shell" launch preset from the user's login shell and grant
    // it to the glasses token (the owner's own writable device). The
    // template is written with comments documenting every option; the
    // parser accepts // and /* */ comments, and --add-token edits the
    // file without discarding them.
    let hash = hex(&sha2::Sha256::digest(token.as_bytes()));
    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into());
    let shell = serde_json::to_string(&shell)?; // JSON-quoted
    let template = format!(
        r#"{{
  // Address(es) the websocket gateway listens on: a string or an array of
  // strings, e.g. ["127.0.0.1", "100.x.y.z"]. Keep these private (loopback
  // or a tailscale IP); for viewers outside the tailnet, front a loopback
  // listener with `tailscale funnel` rather than listening publicly (see
  // README).
  "listen_addr": "127.0.0.1",
  "port": 8737,

  // Tokens clients authenticate with; only SHA-256 hashes are stored. Add
  // more with `g2mirror-server --add-token <name> [--writable]
  // [--launch <preset>]... [--launch-all]`. Per-token options:
  //   "readonly": reject all input from this token (default true)
  //   "filter":   [{{"path": "<regex>"}}, {{"windowtitle": "<regex>"}}, ...]
  //               only terminals matching some rule are visible to the
  //               token; within one rule every present field must match,
  //               and regexes are anchored at both ends
  //   "launch":   launch presets this token may start: true for all, or a
  //               list of preset names (requires "readonly": false)
  "auth_tokens": [
    {{"name": "glasses", "token_hash": "{hash}",
     "readonly": false, "launch": true}}
  ],

  // Who sizes the wrapped app when several clients view it at once: the
  // earliest listed party that is currently viewing wins ("host" is the
  // terminal the wrapper runs in, always counted as present). Unlisted
  // tokens rank below every listed entry; an unlisted host ranks below
  // those.
  "size_precedence": ["glasses", "host"],

  // Commands that launch-granted tokens may start as detached sessions
  // (claim one into a terminal with `g2mirror -a`). A launch request names
  // a preset; the command line always comes from here, never from the
  // network. Per-preset options:
  //   "argv":       command and arguments (required)
  //   "cwd":        working directory, leading ~ expanded (default "~")
  //   "title":      initial window title (default: the preset name)
  //   "env":        {{"VAR": "value"}} merged over the server's environment
  //   "allow_cwd":  let the launch request override cwd (default false)
  //   "scrollback": history lines kept for viewers (default 10000)
  //   "readonly":   reject input to these sessions (default false)
  //   "size":       initial pty size before a viewer resizes it, e.g.
  //                 "80x24" (the default)
  "launch": {{
    "shell": {{"argv": [{shell}, "-l"], "allow_cwd": true}}
  }}
}}
"#
    );
    // Sanity-check our own template before writing it.
    let config = parse_config(&template).context("generated config template is invalid")?;
    config.validate_launch()?;
    std::fs::write(&path, &template)?;
    println!("wrote {}", path.display());
    println!("auth token \"glasses\" (save it now; only the hash is stored):");
    println!("{token}");
    println!();
    println!("the \"glasses\" token may launch detached login shells (a \"shell\"");
    println!("launch preset was written; edit the launch section to change this)");
    println!("add tokens for other viewers with: g2mirror-server --add-token <name>");
    println!("start the server by running: g2mirror-server");
    Ok(())
}

/// Generate a token for another viewer (read-only unless --writable), print
/// it once, and append its hash to the config.
fn add_token(args: &[String]) -> anyhow::Result<()> {
    let mut name: Option<&str> = None;
    let mut writable = false;
    let mut launch_all = false;
    let mut launch_names: Vec<String> = Vec::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--writable" => writable = true,
            "--launch-all" => launch_all = true,
            "--launch" => match it.next() {
                Some(preset) => launch_names.push(preset.clone()),
                None => anyhow::bail!("--launch requires a preset name"),
            },
            other if !other.starts_with('-') && name.is_none() => name = Some(other),
            other => anyhow::bail!("unexpected argument: {other}"),
        }
    }
    let Some(name) = name else {
        anyhow::bail!(
            "usage: g2mirror-server --add-token <name> [--writable] \
             [--launch <preset>]... [--launch-all]"
        );
    };
    anyhow::ensure!(
        name != "host",
        "\"host\" is reserved (it stands for the host terminal in size_precedence)"
    );
    let launch = if launch_all {
        LaunchGrant::Flag(true)
    } else {
        LaunchGrant::Named(launch_names)
    };
    anyhow::ensure!(
        launch.is_none() || writable,
        "a launch grant requires --writable (a read-only token cannot type \
         into the shells it starts)"
    );
    let dir = paths::g2mirror_dir()?;
    let path = paths::config_path(&dir);
    let text = read_config_text(&path)?;
    let mut config =
        parse_config(&text).with_context(|| format!("failed to parse {}", path.display()))?;
    anyhow::ensure!(
        !config.tokens().iter().any(|t| t.name == name),
        "a token named \"{name}\" already exists"
    );
    if let LaunchGrant::Named(names) = &launch {
        for preset in names {
            anyhow::ensure!(
                config.launch.contains_key(preset),
                "no launch preset named {preset:?} in the config"
            );
        }
    }
    let token = generate_token()?;
    let entry = TokenConfig {
        name: name.into(),
        token_hash: hex(&sha2::Sha256::digest(token.as_bytes())),
        readonly: !writable,
        filter: Vec::new(),
        launch,
    };
    // Splice the new token into the existing text, so comments in the
    // config survive; rewrite from the parsed form only for legacy configs
    // that have no auth_tokens array to splice into.
    let new_text = match insert_token_text(&text, &entry)? {
        Some(t) => t,
        None => {
            config.auth_tokens.push(entry);
            serde_json::to_string_pretty(&config)? + "\n"
        }
    };
    parse_config(&new_text).context("edited config failed to re-parse")?;
    std::fs::write(&path, new_text)?;
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

/// Skip a JSON string starting at `i` (which must point at the opening
/// quote); returns the index just past the closing quote.
fn skip_string(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    i
}

/// Insert `token` into the top-level `auth_tokens` array of the config
/// text by splicing, keeping everything else — comments included — byte
/// for byte. Returns None when the text has no such array (a legacy
/// config); the caller falls back to a full rewrite. `text` must already
/// have parsed successfully as a config.
fn insert_token_text(text: &str, token: &TokenConfig) -> anyhow::Result<Option<String>> {
    // Comment bytes become spaces at the same offsets, so indices found in
    // the stripped text are valid in the original.
    let stripped = jsonc::strip_comments(text);
    let bytes = stripped.as_bytes();

    // Find the "auth_tokens" key at depth 1 of the top-level object.
    let mut i = 0;
    let mut depth = 0i32;
    let mut after_key = None;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let start = i;
                i = skip_string(bytes, i);
                if depth == 1 && &stripped[start..i] == "\"auth_tokens\"" {
                    let mut j = i;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if bytes.get(j) == Some(&b':') {
                        after_key = Some(j + 1);
                        break;
                    }
                }
            }
            b'{' | b'[' => {
                depth += 1;
                i += 1;
            }
            b'}' | b']' => {
                depth -= 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    let Some(mut i) = after_key else {
        return Ok(None);
    };
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if bytes.get(i) != Some(&b'[') {
        return Ok(None); // auth_tokens is not an array; let serde complain
    }

    // Find the matching close bracket and the last element before it.
    let open = i;
    let mut depth = 0i32;
    let close = loop {
        anyhow::ensure!(i < bytes.len(), "unterminated auth_tokens array");
        match bytes[i] {
            b'"' => i = skip_string(bytes, i),
            b'[' | b'{' => {
                depth += 1;
                i += 1;
            }
            b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    break i;
                }
                i += 1;
            }
            _ => i += 1,
        }
    };

    let entry = serde_json::to_string_pretty(token)?.replace('\n', "\n    ");
    let (at, insert) = match stripped[open + 1..close].rfind(|c: char| !c.is_whitespace()) {
        // Empty array: the entry becomes its only element.
        None => (open + 1, format!("\n    {entry}\n  ")),
        // Splice right after the last element (comments between it and the
        // bracket stay where they are).
        Some(off) => (open + 1 + off + 1, format!(",\n    {entry}")),
    };
    let mut out = String::with_capacity(text.len() + insert.len());
    out.push_str(&text[..at]);
    out.push_str(&insert);
    out.push_str(&text[at..]);
    Ok(Some(out))
}

fn serve() -> anyhow::Result<()> {
    let dir = paths::g2mirror_dir()?;
    let config_file = paths::config_path(&dir);
    let config = load_config(&config_file)?;
    anyhow::ensure!(
        !config.tokens().is_empty(),
        "{} defines no auth tokens; run g2mirror-server --init-config",
        config_file.display()
    );
    config.validate_launch()?;
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

    for addr in config.listen_addr.iter() {
        if let Ok(addr) = addr.parse::<std::net::IpAddr>()
            && addr.is_unspecified()
        {
            eprintln!(
                "warning: listening on {} exposes the server on all interfaces; \
                 prefer a loopback or tailscale address",
                addr
            );
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut listeners = Vec::new();
        for addr in config.listen_addr.iter() {
            let listener = tokio::net::TcpListener::bind((addr, config.port))
                .await
                .with_context(|| format!("failed to bind {}:{}", addr, config.port))?;
            // Parsed by tooling; keep the format stable.
            println!("g2mirror-server listening on {}", listener.local_addr()?);
            listeners.push(listener);
        }
        let config = Arc::new(config);
        let dir = Arc::new(dir);
        let filters = Arc::new(filters);
        let state = BellState::new();
        let unauth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for listener in listeners {
            tokio::spawn(accept_loop(
                listener,
                config.clone(),
                filters.clone(),
                dir.clone(),
                state.clone(),
                unauth.clone(),
            ));
        }
        monitor_manager((*dir).clone(), state.clone()).await;
        Ok(())
    })
}

async fn accept_loop(
    listener: tokio::net::TcpListener,
    config: Arc<Config>,
    filters: Arc<HashMap<String, Vec<CompiledRule>>>,
    dir: Arc<PathBuf>,
    state: Arc<BellState>,
    unauth: Arc<std::sync::atomic::AtomicUsize>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                // Transient (e.g. fd exhaustion): back off and keep serving.
                eprintln!("accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        // Cap connections that haven't authenticated yet, so junk traffic
        // to a public endpoint can't accumulate half-open handshakes.
        let Some(guard) = UnauthGuard::acquire(&unauth) else {
            eprintln!("connection from {peer}: dropped (too many unauthenticated connections)");
            continue;
        };
        let config = config.clone();
        let filters = filters.clone();
        let dir = dir.clone();
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_device(stream, guard, &config, &filters, &dir, &state).await {
                eprintln!("connection from {peer}: {e:#}");
            }
        });
    }
}

/// RAII slot in the unauthenticated-connection budget.
struct UnauthGuard(Arc<std::sync::atomic::AtomicUsize>);

impl UnauthGuard {
    fn acquire(counter: &Arc<std::sync::atomic::AtomicUsize>) -> Option<Self> {
        use std::sync::atomic::Ordering;
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current >= MAX_UNAUTHENTICATED {
                return None;
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Self(counter.clone())),
                Err(now) => current = now,
            }
        }
    }
}

impl Drop for UnauthGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
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
            // The connect greeting carries the real working directory
            // (which token filters match against), the current title, and
            // the detached/launched state.
            Some("connect") => {
                let mut terminals = state.terminals.lock().unwrap();
                let terminal = terminals.entry(name.to_string()).or_default();
                if let Some(cwd) = msg.get("cwd").and_then(|c| c.as_str()) {
                    terminal.cwd = Some(cwd.to_string());
                }
                if let Some(title) = msg.get("title").and_then(|t| t.as_str()) {
                    terminal.title = Some(title.to_string());
                }
                terminal.detached = msg
                    .get("detached")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                terminal.launched = msg
                    .get("launched")
                    .and_then(|l| l.as_str())
                    .map(str::to_string);
                continue;
            }
            // A headless session's host role was claimed or released.
            Some("host_changed") => {
                let Some(attached) = msg.get("attached").and_then(serde_json::Value::as_bool)
                else {
                    continue;
                };
                let mut terminals = state.terminals.lock().unwrap();
                terminals.entry(name.to_string()).or_default().detached = !attached;
                ServerToDevice::Detached {
                    socket: name.to_string(),
                    detached: !attached,
                }
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

/// Websocket handshake plus the authenticating init exchange. Returns None
/// on a clean pre-auth close; a rejected connection is an error so the
/// caller logs it with the peer address.
async fn authenticate_device(
    stream: TcpStream,
    config: &Config,
) -> anyhow::Result<Option<(WebSocketStream<TcpStream>, DeviceInit, TokenConfig)>> {
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .context("websocket handshake failed")?;

    // First message must be init with a valid auth token.
    let init: DeviceInit = match next_text(&mut ws).await? {
        Some(text) => serde_json::from_str(&text).context("first message must be init")?,
        None => return Ok(None),
    };
    let token = config
        .tokens()
        .into_iter()
        .find(|t| token_matches(&init.auth_token, &t.token_hash));
    let token = match token {
        Some(token) if init.msg_type == "init" && init.version == PROTOCOL_VERSION => token,
        _ => {
            let message = if init.msg_type != "init" {
                "first message must be init".to_string()
            } else if init.version != PROTOCOL_VERSION {
                format!("unsupported protocol version {}", init.version)
            } else {
                "authentication failed".to_string()
            };
            send(&mut ws, &ServerToDevice::Error { message: message.clone() }).await?;
            ws.close(None).await?;
            anyhow::bail!("{message}");
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
    Ok(Some((ws, init, token)))
}

async fn handle_device(
    stream: TcpStream,
    unauth_guard: UnauthGuard,
    config: &Config,
    filters: &HashMap<String, Vec<CompiledRule>>,
    dir: &Path,
    state: &BellState,
) -> anyhow::Result<()> {
    let (mut ws, init, token) =
        match tokio::time::timeout(AUTH_DEADLINE, authenticate_device(stream, config)).await {
            Ok(Ok(Some(authenticated))) => authenticated,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(e)) => return Err(e),
            Err(_) => anyhow::bail!("authentication deadline expired"),
        };
    drop(unauth_guard); // authenticated: release the pre-auth budget slot
    let filter = filters.get(&token.name).map(Vec::as_slice).unwrap_or(&[]);

    let mut session: Option<SessionConn> = None;
    let mut event_rx = state.event_tx.subscribe();
    let mut ping = tokio::time::interval(PING_INTERVAL);
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
                        | ServerToDevice::Title { socket, .. }
                        | ServerToDevice::Detached { socket, .. } => Some(socket.clone()),
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
            // Keepalive so NAT/proxy middleboxes (e.g. on a tailscale
            // funnel path) don't reap idle connections. Clients' websocket
            // libraries answer pings automatically.
            _ = ping.tick() => {
                ws.send(Message::Ping(Vec::new().into())).await?;
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
        Some("launch") => {
            let preset = parsed
                .get("command")
                .and_then(|c| c.as_str())
                .unwrap_or("shell");
            let cwd_override = parsed.get("cwd").and_then(|c| c.as_str());
            match launch_session(config, token, filter, preset, cwd_override).await {
                Ok(socket) => send(ws, &ServerToDevice::Launched { socket }).await,
                Err(e) => {
                    send(
                        ws,
                        &ServerToDevice::Error {
                            message: format!("launch failed: {e:#}"),
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

/// Expand a leading `~` to $HOME.
fn expand_tilde(path: &str) -> String {
    match (path.strip_prefix("~"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) if rest.is_empty() || rest.starts_with('/') => {
            format!("{home}{rest}")
        }
        _ => path.to_string(),
    }
}

/// The g2mirror wrapper binary: our sibling if present (the normal install
/// layout), else whatever PATH resolves.
fn wrapper_exe() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| Some(p.parent()?.join("g2mirror")))
        .filter(|p| p.exists())
        .unwrap_or_else(|| PathBuf::from("g2mirror"))
}

/// Start a detached session from a launch preset (a `launch` request).
/// Returns the new session's socket name.
///
/// The heavy lifting is `g2mirror --detached`, which double-spawns: the
/// invocation we run here forks the actual headless wrapper into its own
/// session (setsid, so it survives us) and exits immediately, printing the
/// session socket name — so awaiting it never blocks and leaves no zombie.
async fn launch_session(
    config: &Config,
    token: &TokenConfig,
    filter: &[CompiledRule],
    preset_name: &str,
    cwd_override: Option<&str>,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        !token.readonly && token.launch.allows(preset_name),
        "your token may not launch {preset_name:?}"
    );
    let preset = config
        .launch
        .get(preset_name)
        .with_context(|| format!("no launch preset named {preset_name:?}"))?;
    let cwd = match cwd_override {
        Some(cwd) => {
            anyhow::ensure!(
                preset.allow_cwd,
                "preset {preset_name:?} does not allow overriding the working directory"
            );
            cwd.to_string()
        }
        None => preset.cwd.clone().unwrap_or_else(|| "~".into()),
    };
    // Canonical form, as the wrapper's getcwd will report it — what token
    // filters and the socket name are derived from.
    let cwd = std::fs::canonicalize(expand_tilde(&cwd))
        .with_context(|| format!("bad working directory {cwd:?}"))?;
    let title = preset
        .title
        .clone()
        .unwrap_or_else(|| preset_name.to_string());
    anyhow::ensure!(
        filter_allows(filter, Some(&cwd.to_string_lossy()), Some(&title)),
        "your token's filter would hide the launched session"
    );

    let mut cmd = tokio::process::Command::new(wrapper_exe());
    cmd.arg("--detached");
    cmd.arg("--launched").arg(preset_name);
    cmd.arg("--title").arg(&title);
    if preset.readonly {
        cmd.arg("--readonly");
    }
    if let Some(lines) = preset.scrollback {
        cmd.arg("--scrollback").arg(lines.to_string());
    }
    if let Some(size) = &preset.size {
        cmd.arg("--initial-size").arg(size);
    }
    cmd.arg("--").args(&preset.argv);
    cmd.current_dir(&cwd);
    cmd.envs(&preset.env);
    cmd.stdin(std::process::Stdio::null());
    let out = cmd.output().await.context("failed to run g2mirror --detached")?;
    anyhow::ensure!(
        out.status.success(),
        "g2mirror --detached failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    let socket = String::from_utf8_lossy(&out.stdout).trim().to_string();
    anyhow::ensure!(
        paths::is_valid_socket_name(&socket),
        "g2mirror --detached printed an unexpected socket name {socket:?}"
    );
    eprintln!("launched preset {preset_name:?} for token {:?}: {socket}", token.name);
    Ok(socket)
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
                detached: terminal.detached,
                launched: terminal.launched,
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
    fn configs_may_contain_comments() {
        let config: Config = parse_config(
            r#"{
              // gateway address
              "listen_addr": "127.0.0.1", /* and port */ "port": 8737,
              "auth_tokens": [{"name": "t", "token_hash": "aa"}]
            }"#,
        )
        .unwrap();
        assert_eq!(config.port, 8737);
        assert_eq!(config.tokens()[0].name, "t");
    }

    #[test]
    fn add_token_splice_preserves_comments() {
        let token = TokenConfig {
            name: "rob".into(),
            token_hash: "bb".into(),
            readonly: false,
            filter: Vec::new(),
            launch: LaunchGrant::Named(vec!["shell".into()]),
        };

        let text = r#"{
  // gateway
  "listen_addr": "127.0.0.1", "port": 1,
  /* tokens */
  "auth_tokens": [
    {"name": "glasses", "token_hash": "aa"} // mine
  ],
  "launch": {"shell": {"argv": ["sh"]}}
}
"#;
        let out = insert_token_text(text, &token).unwrap().unwrap();
        for comment in ["// gateway", "/* tokens */", "// mine"] {
            assert!(out.contains(comment), "{comment} lost:\n{out}");
        }
        let config = parse_config(&out).unwrap();
        assert_eq!(config.tokens().len(), 2);
        assert_eq!(config.tokens()[1].name, "rob");
        assert!(config.tokens()[1].launch.allows("shell"));
        config.validate_launch().unwrap();

        // An empty array gets the entry as its only element.
        let text = r#"{"listen_addr": "a", "port": 1, "auth_tokens": []}"#;
        let out = insert_token_text(text, &token).unwrap().unwrap();
        let config = parse_config(&out).unwrap();
        assert_eq!(config.tokens()[0].name, "rob");

        // Strings full of brackets, comment markers, and the key's own
        // name must not derail the scan.
        let text = r#"{
  "listen_addr": "a", "port": 1,
  "auth_tokens": [
    {"name": "g", "token_hash": "aa",
     "filter": [{"windowtitle": "x /* ] \" auth_tokens // ["}]}
  ]
}"#;
        let out = insert_token_text(text, &token).unwrap().unwrap();
        assert_eq!(parse_config(&out).unwrap().tokens().len(), 2);

        // Legacy configs without an auth_tokens array have no splice point.
        let text = r#"{"listen_addr": "a", "port": 1, "auth_token_hash": "aa"}"#;
        assert!(insert_token_text(text, &token).unwrap().is_none());
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
