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
    },
    /// Full repaint of the device screen; sent on view. `data` is base64 of
    /// terminal bytes (escape sequences) to feed a fresh emulator of the
    /// device's declared width/height.
    Snapshot { data: String },
    /// Incremental terminal output while viewing; base64, same encoding.
    Output { data: String },
    /// Sent to monitor connections when the app rings the terminal bell.
    /// `at` is unix epoch milliseconds; debounced to at most one message
    /// per 3 seconds (a bell suppressed by the debounce window is reported
    /// when the window expires, so the latest timestamp is not lost).
    Bell { at: u64 },
    /// The wrapped app exited. The connection closes after this.
    Exit { status: Option<i32> },
    Error { message: String },
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
    /// Reply to a successful init.
    Init { version: u32 },
    /// Reply to `list`.
    Sessions { sessions: Vec<SessionInfo> },
    /// The session connection ended (wrapper exited, `disconnect` requested,
    /// or an I/O error occurred).
    Disconnected { reason: String },
    /// A terminal rang its bell (sent to every connected device, whether or
    /// not it is viewing that terminal). `last_bell_at` is unix epoch ms.
    Bell { socket: String, last_bell_at: u64 },
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
}

pub fn encode_terminal_bytes(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn decode_terminal_bytes(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(data)
}
