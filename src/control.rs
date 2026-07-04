//! The wrapper's session socket: a unix domain socket in ~/.g2mirror that a
//! client (normally g2mirror-server) connects to, speaking newline-delimited
//! JSON. One client at a time.

use std::path::PathBuf;

use anyhow::Context as _;
use g2mirror::protocol::{FromSession, ToSession};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};

/// How long a send may block before we conclude the client is stuck and drop
/// it (a stalled client must not freeze the host terminal's event loop).
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub struct ControlListener {
    listener: UnixListener,
    path: PathBuf,
}

impl ControlListener {
    /// Create ~/.g2mirror (permissions 700) and bind the session socket.
    pub fn bind() -> anyhow::Result<Self> {
        let dir = g2mirror::paths::g2mirror_dir().context("failed to create ~/.g2mirror")?;
        let name = g2mirror::paths::socket_name(
            std::process::id(),
            &std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")),
        );
        let path = dir.join(name);
        // A leftover file from a previous process with our (reused) pid.
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)
            .with_context(|| format!("failed to bind session socket {}", path.display()))?;
        Ok(Self { listener, path })
    }

    pub async fn accept(&self) -> std::io::Result<UnixStream> {
        let (stream, _) = self.listener.accept().await?;
        Ok(stream)
    }
}

impl Drop for ControlListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClientState {
    AwaitingInit,
    Ready,
    Viewing,
}

pub struct Client {
    stream: UnixStream,
    /// Bytes read but not yet consumed as complete lines.
    buf: Vec<u8>,
    pub state: ClientState,
    /// Device description and dimensions from the init message.
    pub device: String,
    pub width: u16,
    pub height: u16,
}

impl Client {
    pub fn new(stream: UnixStream) -> Self {
        Self {
            stream,
            buf: Vec::new(),
            state: ClientState::AwaitingInit,
            device: String::new(),
            width: 0,
            height: 0,
        }
    }

    /// Read the next newline-delimited JSON message. Returns Ok(None) on
    /// clean EOF. Cancel-safe: a partial line stays buffered across
    /// cancelled calls.
    pub async fn next_message(&mut self) -> anyhow::Result<Option<ToSession>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let line = &line[..line.len() - 1];
                if line.is_empty() {
                    continue;
                }
                let msg = serde_json::from_slice(line)
                    .with_context(|| format!("bad message: {}", String::from_utf8_lossy(line)))?;
                return Ok(Some(msg));
            }
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Send a message, giving up (with an error) if the client stalls.
    pub async fn send(&mut self, msg: &FromSession) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(msg)?;
        line.push(b'\n');
        tokio::time::timeout(SEND_TIMEOUT, self.stream.write_all(&line))
            .await
            .context("client stalled")??;
        Ok(())
    }
}
