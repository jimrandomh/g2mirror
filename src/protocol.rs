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
        /// Size-precedence rank of this viewer (lower wins), computed by
        /// g2mirror-server from the `size_precedence` config list. When
        /// several clients view at once, the wrapped app is sized to the
        /// best-ranked one. Absent (direct connections): rank 0.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size_rank: Option<u32>,
        /// Rank of the host terminal in the same ordering. Absent: the
        /// host ranks below every viewer (the pre-precedence behavior).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_size_rank: Option<u32>,
        /// `host` claims the host role on a headless (detached) session:
        /// this client's size ranks as `"host"` in the size precedence
        /// instead of by token, and the session stops counting as
        /// detached. At most one client holds the role; refused on
        /// sessions with a real host terminal.
        #[serde(default, skip_serializing_if = "Role::is_viewer")]
        role: Role,
        /// With `role: host`: take the role over from a client that
        /// already holds it (which is disconnected) instead of being
        /// refused.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        force: bool,
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
    Input {
        data: String,
        /// Pauses the wrapper inserts while writing the decoded bytes, so
        /// apps that infer pasting from bytes arriving together (and would
        /// e.g. treat a trailing newline as a pasted soft newline rather
        /// than a submit) see distinct keystrokes. Wrapper-side timing is
        /// immune to network jitter, unlike a client sleeping between two
        /// `input` messages. Offsets must be non-decreasing; the wrapper
        /// caps each pause and the entry count (see PROTOCOL.md).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        delays: Vec<InputDelay>,
    },
    /// Request scrollback history: up to `limit` lines (default 500,
    /// additionally capped by a reply byte budget) ending just before line
    /// index `before`. Paginate backwards by passing the previous reply's
    /// `start` as the next `before`.
    History { before: u64, limit: Option<u32> },
    /// This client's terminal was resized: update the dimensions it
    /// declared in `init` (host-role clients forward SIGWINCH this way).
    /// If the client currently controls the view size, the stream restarts
    /// at the new dimensions and every viewer gets a fresh snapshot.
    Resize { width: u16, height: u16 },
}

/// A session client's role, declared in `init`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// A device or human viewer; its size ranks by its token.
    #[default]
    Viewer,
    /// The claimant of a detached session's host slot (`g2mirror --attach`).
    Host,
}

impl Role {
    /// serde skip helper: the default role is omitted on the wire.
    pub fn is_viewer(&self) -> bool {
        *self == Role::Viewer
    }
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
        /// Current window title, if one is set (also pushed as a `title`
        /// message on change; carried here so a one-shot probe of the
        /// socket sees it).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// True for a wrapper running without a host terminal
        /// (`--headless`/`--detached`). Only headless sessions accept
        /// host-role inits.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        headless: bool,
        /// True when the session is headless and no client currently holds
        /// the host role — i.e. it is claimable with `g2mirror --attach`.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        detached: bool,
        /// The server launch preset this session was started from, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        launched: Option<String>,
    },
    /// Full repaint of the mirrored screen; sent on view, and re-sent to
    /// every viewing client whenever the stream's dimensions change (a
    /// better-ranked viewer attached or detached). `data` is base64 of
    /// terminal bytes (escape sequences) to feed a fresh emulator of
    /// exactly `width` x `height` cells — which is the size of the
    /// best-ranked viewer, not necessarily yours. Every history line with
    /// index < `history_next` is fetchable; lines the client witnesses
    /// scrolling off its emulator after this snapshot continue from that
    /// index.
    Snapshot {
        data: String,
        width: u16,
        height: u16,
        history_next: u64,
    },
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
    /// A headless session's host role was claimed or released (sent to the
    /// monitor and viewers, like `title`): its detached state flipped.
    HostChanged { attached: bool },
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

/// A pause inside an `input` message: after writing the first `at` bytes of
/// the decoded data, the wrapper waits `ms` milliseconds before writing the
/// rest. `at` == data length pauses after everything, holding back any
/// subsequently queued input.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct InputDelay {
    /// Byte offset into the decoded `data` (not a character index).
    pub at: u64,
    pub ms: u64,
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
    /// Reply to `launch`: the new session's socket name; follow with a
    /// normal `connect` to attach to it.
    Launched { socket: String },
    /// A terminal's detached state changed (host role claimed or released;
    /// sent to every connected device, like `title`).
    Detached { socket: String, detached: bool },
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
    /// True when the session is headless with no host-role client: it can
    /// be claimed with `g2mirror --attach`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub detached: bool,
    /// The server launch preset the session was started from, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launched: Option<String>,
}

pub fn encode_terminal_bytes(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn decode_terminal_bytes(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(data)
}
