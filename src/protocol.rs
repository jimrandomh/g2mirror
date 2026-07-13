//! Message types for the two g2mirror protocols:
//!
//! 1. The **session protocol**: newline-delimited JSON over a unix domain
//!    socket between a command-wrapper process (`g2mirror`) and a client
//!    (normally `g2mirror-server` relaying for a remote device).
//! 2. The **server protocol**: JSON text frames over a websocket between
//!    `g2mirror-server` and a device driver. Server-scoped messages are
//!    defined here; anything else the device sends is forwarded verbatim to
//!    the connected session, and all session messages are forwarded verbatim
//!    to the device.
//!
//! See PROTOCOL.md for the full protocol description.

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

/// Session protocol: client -> wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToSession {
    /// Must be the first message on the connection.
    Init {
        version: u32,
        /// Free-text device description, e.g. "Jim's G2 glasses".
        device: String,
        /// Device terminal size in character cells.
        width: u16,
        height: u16,
    },
    /// Alternative first message: this connection is a monitor (normally
    /// g2mirror-server). It receives bell notifications and does not count
    /// as a viewer — it cannot send view/unview and does not block one.
    Monitor { version: u32 },
    /// Start viewing: the wrapped app is resized to the device dimensions
    /// (SIGWINCH), a snapshot is sent immediately, then output streams.
    View,
    /// Stop viewing: the app is resized back to the host terminal and
    /// output streaming stops.
    Unview,
    /// Keyboard/voice input for the wrapped app: base64 of the bytes to
    /// write to its terminal, exactly as a terminal emulator would encode
    /// them (the device driver is responsible for honoring the input modes
    /// mirrored in the output stream, e.g. bracketed paste). Rejected with
    /// an `error` message — without closing the connection — when the
    /// session is read-only.
    Input { data: String },
    /// Request scrollback history: up to `limit` lines (default 500,
    /// additionally capped by a reply byte budget) ending just before line
    /// index `before`. Paginate backwards by passing the previous reply's
    /// `start` as the next `before`.
    History { before: u64, limit: Option<u32> },
}

/// Session protocol: wrapper -> client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FromSession {
    /// Sent immediately when a connection is accepted.
    Connect {
        version: u32,
        pid: u32,
        command: String,
        cwd: String,
        host_width: u16,
        host_height: u16,
        /// True when the wrapper was started with --readonly; input
        /// messages will be rejected.
        readonly: bool,
        /// Extent of the scrollback history archive.
        history: HistoryExtent,
    },
    /// Full repaint of the device screen; sent on view. `data` is base64 of
    /// terminal bytes (escape sequences) to feed a fresh emulator of the
    /// device's declared width/height. Every history line with index <
    /// `history_next` is fetchable; lines the client witnesses scrolling
    /// off its emulator after this snapshot continue from that index.
    Snapshot { data: String, history_next: u64 },
    /// Incremental terminal output while viewing; base64, same encoding.
    Output { data: String },
    /// Sent to monitor connections when the app rings the terminal bell.
    /// `at` is unix epoch milliseconds; debounced to at most one message
    /// per 3 seconds (a bell suppressed by the debounce window is reported
    /// when the window expires, so the latest timestamp is not lost).
    Bell { at: u64 },
    /// The app set the window title (xterm OSC 0/2). Sent to the monitor
    /// and the viewer on change, and once on attach if a title is set.
    Title { title: String },
    /// The wrapped app exited. The connection closes after this.
    Exit { status: Option<i32> },
    /// Reply to a history request: lines `start..start+lines.len()` in
    /// oldest-to-newest order, plus the current archive extent.
    HistoryLines {
        start: u64,
        oldest: u64,
        next: u64,
        lines: Vec<HistoryLine>,
    },
    Error { message: String },
}

/// Extent of the history archive: line indices `oldest..next` are fetchable.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HistoryExtent {
    pub next: u64,
    pub oldest: u64,
}

/// One archived line. `data` is base64 of self-contained styled text:
/// printable characters and SGR sequences only, starting from default
/// attributes (reset before rendering elsewhere). `width` is the column
/// count the line was laid out at; `wrapped` means the line soft-wraps, so
/// it and the following record form one logical line and may be re-wrapped
/// at a different width.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryLine {
    pub data: String,
    pub width: u16,
    pub wrapped: bool,
}

/// Server protocol: the device's first websocket message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInit {
    #[serde(rename = "type")]
    pub msg_type: String, // must be "init"
    pub version: u32,
    pub auth_token: String,
    pub device: String,
    pub width: u16,
    pub height: u16,
}

/// Server protocol: server-originated messages to the device.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerToDevice {
    /// Reply to a successful init. `readonly` reflects the server's config:
    /// when true the server rejects all input messages. (A session may
    /// additionally be read-only via the wrapper's --readonly flag,
    /// reported in its connect message; input works only when neither is
    /// set.)
    Init { version: u32, readonly: bool },
    /// Reply to `list`.
    Sessions { sessions: Vec<SessionInfo> },
    /// The session connection ended (wrapper exited, `disconnect` requested,
    /// or an I/O error occurred).
    Disconnected { reason: String },
    /// A terminal rang its bell (sent to every connected device, whether or
    /// not it is viewing that terminal). `last_bell_at` is unix epoch ms.
    Bell { socket: String, last_bell_at: u64 },
    /// A terminal's window title changed (sent to every connected device,
    /// whether or not it is viewing that terminal).
    Title { socket: String, title: String },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    /// Socket file name; pass to `connect`.
    pub socket: String,
    pub pid: u32,
    /// Sanitized working directory from the socket name.
    pub cwd_hint: String,
    /// When this terminal's bell last rang (unix epoch ms), if it has rung
    /// since the server started monitoring the terminal.
    pub last_bell_at: Option<u64>,
    /// The terminal's window title, if the app has set one.
    pub title: Option<String>,
}

pub fn encode_terminal_bytes(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn decode_terminal_bytes(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(data)
}
