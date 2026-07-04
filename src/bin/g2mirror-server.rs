//! g2mirror-server: websocket gateway between device drivers (e.g. the
//! smart-glasses driver) and g2mirror session sockets in ~/.g2mirror.
//!
//! Transport security is out of scope: run it on a loopback/tailscale
//! address (from config.json) and tunnel as needed. Devices authenticate
//! with a token whose SHA-256 hash is stored in the config.

use std::path::{Path, PathBuf};

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

#[derive(Serialize, Deserialize)]
struct Config {
    /// Address to listen on. The server trusts this to be non-public
    /// (loopback or a tailscale address); it warns on 0.0.0.0/::.
    listen_addr: String,
    port: u16,
    /// Lowercase hex SHA-256 of the auth token.
    auth_token_hash: String,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("--init-config") => init_config(),
        Some(other) => {
            eprintln!("unknown argument: {other}");
            eprintln!("usage: g2mirror-server [--init-config]");
            std::process::exit(2);
        }
        None => serve(),
    };
    if let Err(e) = result {
        eprintln!("g2mirror-server: {e:#}");
        std::process::exit(1);
    }
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
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).context("failed to generate random token")?;
    let token = hex(&raw);
    let config = Config {
        listen_addr: "127.0.0.1".into(),
        port: 8737,
        auth_token_hash: hex(&sha2::Sha256::digest(token.as_bytes())),
    };
    std::fs::write(&path, serde_json::to_string_pretty(&config)? + "\n")?;
    println!("wrote {}", path.display());
    println!("auth token (save it now; only the hash is stored):");
    println!("{token}");
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
        let config = std::sync::Arc::new(config);
        let dir = std::sync::Arc::new(dir);
        loop {
            let (stream, peer) = listener.accept().await?;
            let config = config.clone();
            let dir = dir.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_device(stream, &config, &dir).await {
                    eprintln!("connection from {peer}: {e:#}");
                }
            });
        }
    })
}

async fn handle_device(stream: TcpStream, config: &Config, dir: &Path) -> anyhow::Result<()> {
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .context("websocket handshake failed")?;

    // First message must be init with a valid auth token.
    let init: DeviceInit = match next_text(&mut ws).await? {
        Some(text) => serde_json::from_str(&text).context("first message must be init")?,
        None => return Ok(()),
    };
    if init.msg_type != "init"
        || init.version != PROTOCOL_VERSION
        || !token_matches(&init.auth_token, &config.auth_token_hash)
    {
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
    send(
        &mut ws,
        &ServerToDevice::Init {
            version: PROTOCOL_VERSION,
        },
    )
    .await?;

    let mut session: Option<SessionConn> = None;
    loop {
        tokio::select! {
            msg = ws.next() => {
                let Some(msg) = msg else { break };
                let Message::Text(text) = msg? else { continue };
                handle_device_message(&text, &mut ws, &mut session, &init, dir).await?;
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
        }
    }
    Ok(())
}

async fn handle_device_message(
    text: &str,
    ws: &mut WebSocketStream<TcpStream>,
    session: &mut Option<SessionConn>,
    init: &DeviceInit,
    dir: &Path,
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
            let sessions = list_sessions(dir);
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
            match SessionConn::open(&dir.join(name), init).await {
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

/// Sessions whose socket file looks valid and whose wrapper PID is alive.
fn list_sessions(dir: &Path) -> Vec<SessionInfo> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut sessions = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !paths::is_valid_socket_name(name) {
            continue;
        }
        let Some(pid) = paths::socket_pid(name) else {
            continue;
        };
        if !paths::pid_exists(pid) {
            continue;
        }
        let cwd_hint = name.split_once('-').map(|(_, p)| p).unwrap_or("");
        sessions.push(SessionInfo {
            socket: name.to_string(),
            pid,
            cwd_hint: cwd_hint.to_string(),
        });
    }
    sessions
}

/// A connection to a wrapper's session socket, speaking newline-delimited
/// JSON. Lines are relayed verbatim in both directions.
struct SessionConn {
    stream: UnixStream,
    buf: Vec<u8>,
}

impl SessionConn {
    /// Connect and send the session init derived from the device's init.
    async fn open(path: &PathBuf, init: &DeviceInit) -> anyhow::Result<Self> {
        let stream = UnixStream::connect(path)
            .await
            .with_context(|| format!("cannot connect to {}", path.display()))?;
        let mut conn = Self {
            stream,
            buf: Vec::new(),
        };
        let init_line = serde_json::to_string(&serde_json::json!({
            "type": "init",
            "version": PROTOCOL_VERSION,
            "device": init.device,
            "width": init.width,
            "height": init.height,
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
