//! The wrapper's session socket: a unix domain socket in ~/.g2mirror that
//! clients (normally g2mirror-server, relaying for devices) connect to,
//! speaking newline-delimited JSON. Several viewers may be connected — and
//! viewing — at once; the wrapped app is sized to the best-ranked one.

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

/// Debounces bell notifications to at most one per window: the first bell
/// is reported immediately (leading edge); bells during the window are
/// coalesced into one report when the window expires (trailing edge), so
/// the latest bell timestamp is never lost.
pub struct BellDebouncer {
    window: std::time::Duration,
    last_sent: Option<std::time::Instant>,
    pending_at: Option<u64>,
}

impl BellDebouncer {
    pub fn new(window: std::time::Duration) -> Self {
        Self {
            window,
            last_sent: None,
            pending_at: None,
        }
    }

    /// A bell rang at `at` (unix epoch ms). Returns `Some(at)` if it should
    /// be reported now; otherwise it is held for the trailing edge.
    pub fn on_bell(&mut self, now: std::time::Instant, at: u64) -> Option<u64> {
        match self.last_sent {
            Some(sent) if now.duration_since(sent) < self.window => {
                self.pending_at = Some(at);
                None
            }
            _ => {
                self.last_sent = Some(now);
                Some(at)
            }
        }
    }

    /// When (on the monotonic clock) a held bell should be reported, if any.
    pub fn deadline(&self) -> Option<std::time::Instant> {
        self.pending_at
            .and(self.last_sent)
            .map(|sent| sent + self.window)
    }

    /// Report the held bell (call when the deadline expires).
    pub fn fire(&mut self, now: std::time::Instant) -> Option<u64> {
        let at = self.pending_at.take()?;
        self.last_sent = Some(now);
        Some(at)
    }
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
    /// Size-precedence rank from the init message (lower wins the size).
    pub size_rank: u32,
    /// Arrival order, the tie-break between equally ranked viewers.
    pub id: u64,
    /// Marked when a send fails; swept out of the viewer list at safe
    /// points (mid-iteration removal would invalidate indices).
    pub dead: bool,
}

impl Client {
    pub fn new(stream: UnixStream, id: u64) -> Self {
        Self {
            stream,
            buf: Vec::new(),
            state: ClientState::AwaitingInit,
            device: String::new(),
            width: 0,
            height: 0,
            size_rank: 0,
            id,
            dead: false,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn bell_debounce_leading_and_trailing_edge() {
        let mut d = BellDebouncer::new(Duration::from_secs(3));
        let t0 = Instant::now();

        // First bell reports immediately.
        assert_eq!(d.on_bell(t0, 1000), Some(1000));
        assert_eq!(d.deadline(), None);

        // Bells inside the window are held; the latest timestamp wins.
        assert_eq!(d.on_bell(t0 + Duration::from_millis(500), 1500), None);
        assert_eq!(d.on_bell(t0 + Duration::from_millis(900), 1900), None);
        let deadline = d.deadline().expect("trailing edge must be scheduled");
        assert_eq!(deadline, t0 + Duration::from_secs(3));

        // The trailing edge reports the held bell exactly once.
        assert_eq!(d.fire(deadline), Some(1900));
        assert_eq!(d.deadline(), None);
        assert_eq!(d.fire(deadline), None);

        // The trailing report restarts the window...
        assert_eq!(d.on_bell(deadline + Duration::from_secs(1), 5000), None);
        // ...and a bell after a quiet window reports immediately again.
        let mut d = BellDebouncer::new(Duration::from_secs(3));
        assert_eq!(d.on_bell(t0, 1000), Some(1000));
        assert_eq!(d.on_bell(t0 + Duration::from_secs(4), 9000), Some(9000));
    }
}
