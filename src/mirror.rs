//! Translation between the wrapped app's output and its viewers.
//!
//! When no device is viewing, the child pty is sized to the host terminal
//! and bytes pass through untouched (but are still parsed, so a snapshot of
//! the current screen is always available). When a device is viewing, the
//! child pty is sized to the device screen and its output is parsed into an
//! in-memory vt100 screen model at device dimensions; from that one model we
//! render for two destinations:
//!
//! - the host terminal, truncated if it is smaller than the device screen,
//!   always via per-row escape sequences with explicit cursor positioning
//!   (`rows_diff`) — never vt100's whole-screen `state_diff`, whose output
//!   assumes a terminal of exactly the model's size (it reproduces wrapped
//!   lines by relying on autowrap and can emit scroll operations, which on a
//!   larger host terminal write outside the mirrored region);
//! - the viewing device, which is exactly model-sized, so it gets the
//!   whole-screen `state_diff` byte stream.

use crate::history::{History, HistoryRecord, DEFAULT_MAX_LINES, MAX_LINE_BYTES};

/// Dimensions used for the hotkey-simulated device.
pub const SIM_ROWS: u16 = 24;
pub const SIM_COLS: u16 = 96;

/// vt100's internal scrollback acts as a staging buffer for history capture:
/// rows land there when the (primary) screen scrolls, and we drain new rows
/// into the archive after every processed slice. vt100 offers no way to
/// remove staged rows, so this bounds both the transient capture window and
/// the dead weight a long session carries.
const STAGE_SCROLLBACK: usize = 4096;
/// Output is parsed in slices no larger than this so that a flood of
/// one-byte lines can't overflow the staging buffer between drains.
const PROCESS_SLICE: usize = 2048;

const CLEAR: &[u8] = b"\x1b[0m\x1b[H\x1b[2J";
const SGR_RESET: &[u8] = b"\x1b[0m";
const CLEAR_TO_EOL: &[u8] = b"\x1b[K";
/// Host prelude for a view: reset scroll margins, origin mode, and autowrap,
/// since the bottom-anchored renderer relies on full-screen scrolls and
/// absolute addressing. (An app that set these repaints after SIGWINCH.)
const VIEW_HOST_PRELUDE: &[u8] = b"\x1b[r\x1b[?6l\x1b[?7h";
const HIDE_CURSOR: &[u8] = b"\x1b[?25l";
const SHOW_CURSOR: &[u8] = b"\x1b[?25h";
const SYNC_BEGIN: &[u8] = b"\x1b[?2026h";
const SYNC_END: &[u8] = b"\x1b[?2026l";
/// Turn off input modes the view renderer may have enabled on the host
/// terminal (application cursor keys, bracketed paste, mouse reporting).
const RESET_INPUT_MODES: &[u8] =
    b"\x1b[?1l\x1b>\x1b[?2004l\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l";

#[derive(Debug, Clone, Copy)]
pub struct View {
    pub rows: u16,
    pub cols: u16,
    /// True for the hotkey-simulated device, false for a socket client.
    pub simulated: bool,
}

/// Parser callbacks collecting out-of-band terminal events: audible bells
/// and window-title changes (OSC 0/2, BEL- or ST-terminated). Going through
/// the parser (rather than scanning bytes) avoids false positives — e.g. the
/// BEL that terminates a title sequence is not a bell.
#[derive(Default)]
struct Events {
    bells: usize,
    /// Last title set in this chunk, if any.
    title: Option<String>,
}

impl vt100::Callbacks for Events {
    fn audible_bell(&mut self, _: &mut vt100::Screen) {
        self.bells += 1;
    }

    fn set_window_title(&mut self, _: &mut vt100::Screen, title: &[u8]) {
        self.title = Some(String::from_utf8_lossy(title).into_owned());
    }
}

fn new_parser(rows: u16, cols: u16) -> vt100::Parser<Events> {
    vt100::Parser::new_with_callbacks(rows, cols, STAGE_SCROLLBACK, Events::default())
}

pub struct Mirror {
    host_rows: u16,
    host_cols: u16,
    /// Screen model of the child's output, always sized to the child's
    /// current dimensions (host size normally, view size while viewing).
    parser: vt100::Parser<Events>,
    /// While viewing: a second parser fed the same bytes, holding the state
    /// as of the last render, so diffs don't need to clone the main screen
    /// (whose scrollback staging makes clones expensive).
    shadow: Option<vt100::Parser>,
    view: Option<View>,
    /// Current window title, as last set by the app (survives view
    /// transitions, which rebuild the parser).
    title: Option<String>,
    /// Archive of lines that scrolled off the (primary) screen.
    history: History,
    /// Scroll counter: while parked at offset 1, every row pushed to the
    /// staging scrollback advances the offset by one, giving an exact count
    /// of new rows even after the staging buffer is full. False while the
    /// buffer is empty or a harvest is deferred by the alternate screen.
    parked: bool,
    /// Staging-buffer length after the last harvest, the fallback counter
    /// when parking wasn't possible.
    staged_seen: usize,
    /// While viewing: whether the host's history zone (the rows above the
    /// bottom-anchored live region) holds content that hasn't been pushed
    /// into the host terminal's native scrollback yet.
    zone_dirty: bool,
}

/// Result of processing a chunk of child output.
pub struct Output {
    /// Bytes for the host terminal.
    pub host: Vec<u8>,
    /// Bytes for the viewing device (present only while viewing).
    pub remote: Option<Vec<u8>>,
    /// Number of audible bells in this chunk.
    pub bells: usize,
    /// New window title, if it changed in this chunk.
    pub title: Option<String>,
}

/// What the event loop should do after a state change.
pub struct Transition {
    /// New child pty size (delivers SIGWINCH to the child), if it changed.
    pub child_size: Option<(u16, u16)>,
    /// Bytes to write to the host terminal.
    pub host_output: Vec<u8>,
    /// Snapshot bytes for the viewing device (set by `start_view`).
    pub remote_output: Option<Vec<u8>>,
}

impl Mirror {
    pub fn new(host_rows: u16, host_cols: u16) -> Self {
        Self {
            host_rows,
            host_cols,
            parser: new_parser(host_rows, host_cols),
            shadow: None,
            view: None,
            title: None,
            history: History::new(DEFAULT_MAX_LINES),
            parked: false,
            staged_seen: 0,
            zone_dirty: false,
        }
    }

    /// Host row where the live region starts while viewing: the region is
    /// anchored at the bottom of the host screen, and the rows above it (the
    /// history zone) scroll like a normal terminal.
    fn region_offset(&self) -> u16 {
        let region_rows = self
            .view
            .map_or(self.host_rows, |v| v.rows.min(self.host_rows));
        self.host_rows - region_rows
    }

    /// Number of view-model rows hidden above the host screen when the view
    /// is taller than the host (the host shows the model's bottom): the
    /// region displays model rows `crop_top()..view.rows`.
    fn crop_top(&self) -> u16 {
        self.view
            .map_or(0, |v| v.rows.saturating_sub(self.host_rows))
    }

    /// Bytes that copy the hidden top rows of the view model into the host
    /// terminal's native scrollback, so scrolling up shows the whole
    /// mirrored screen. The copies are point-in-time: the model may edit
    /// those rows afterwards, leaving stale scrollback until the next push
    /// (`refresh_scrollback`, wired to Ctrl+L) — an accepted inaccuracy.
    /// The caller must follow with a full region repaint.
    fn push_crop_to_host_scrollback(&self) -> Vec<u8> {
        let crop = usize::from(self.crop_top());
        if crop == 0 {
            return Vec::new();
        }
        let screen = self.parser.screen();
        let (_, cols) = screen.size();
        let mut out = SYNC_BEGIN.to_vec();
        out.extend_from_slice(HIDE_CURSOR);
        // Autowrap off: a line wider than the host must truncate in place,
        // or the row accounting below breaks (matching the region display,
        // which also truncates at the host width).
        out.extend_from_slice(b"\x1b[?7l");
        // Paint batches of hidden rows over the top of the screen, then
        // scroll exactly that many lines: precisely the painted rows enter
        // the scrollback. The repaint that follows fixes the display.
        let mut pushed = 0;
        while pushed < crop {
            let batch = (crop - pushed).min(usize::from(self.host_rows));
            for i in 0..batch {
                out.extend_from_slice(SGR_RESET);
                out.extend_from_slice(cup(i as u16, 0).as_bytes());
                out.extend_from_slice(CLEAR_TO_EOL);
                out.extend_from_slice(&serialize_row(screen, (pushed + i) as u16, cols).bytes);
            }
            out.extend_from_slice(SGR_RESET);
            out.extend_from_slice(cup(self.host_rows.saturating_sub(1), 0).as_bytes());
            out.extend_from_slice(&b"\r\n".repeat(batch));
            pushed += batch;
        }
        out.extend_from_slice(b"\x1b[?7h");
        out.extend_from_slice(SYNC_END);
        out
    }

    /// Re-push the hidden top rows into the host's native scrollback (fresh
    /// copies healing any stale ones) and repaint the region. Wired to
    /// Ctrl+L on the host and in g2mirror-view; empty when nothing is
    /// being mirrored.
    pub fn refresh_scrollback(&self) -> Vec<u8> {
        if self.view.is_none() {
            return Vec::new();
        }
        let mut out = self.push_crop_to_host_scrollback();
        out.extend_from_slice(&self.render_host_full());
        out
    }

    /// Bytes that push the history zone's contents into the host terminal's
    /// native scrollback (by scrolling the whole host screen up past it).
    /// Used before anything clears the zone.
    fn push_zone_to_host_scrollback(&self) -> Vec<u8> {
        let zone_rows = self.region_offset();
        if !self.zone_dirty || zone_rows == 0 {
            return Vec::new();
        }
        let mut out = SGR_RESET.to_vec();
        out.extend_from_slice(cup(self.host_rows.saturating_sub(1), 0).as_bytes());
        out.extend_from_slice(&b"\r\n".repeat(usize::from(zone_rows)));
        out
    }

    pub fn set_history_limit(&mut self, max_lines: usize) {
        self.history.set_max_lines(max_lines);
    }

    /// (next, oldest) indices of the history archive.
    pub fn history_extent(&self) -> (u64, u64) {
        (self.history.next_index(), self.history.oldest())
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn view(&self) -> Option<View> {
        self.view
    }

    /// The host terminal's current dimensions, as (rows, cols).
    pub fn host_size(&self) -> (u16, u16) {
        (self.host_rows, self.host_cols)
    }

    /// Current window title, as last set by the app.
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Seed the title (from the --title flag) before the app has set one.
    /// An app-set title takes over from here via the usual change tracking.
    pub fn set_title(&mut self, title: String) {
        self.title = Some(title);
    }

    /// Translate a chunk of child output.
    pub fn process(&mut self, bytes: &[u8]) -> Output {
        // Parse in slices, draining freshly scrolled-off rows into the
        // history archive between slices so the staging buffer can't
        // overflow even on a flood of one-byte lines.
        let history_before = self.history.next_index();
        for slice in bytes.chunks(PROCESS_SLICE) {
            self.park();
            self.parser.process(slice);
            self.harvest();
        }
        let scrolled = (self.history.next_index() - history_before) as usize;
        let bells = self.take_bells();
        let title = self.take_title_change();
        match self.view {
            None => Output {
                host: bytes.to_vec(),
                remote: None,
                bells,
                // Pass-through already delivered the title sequence to
                // the host; this is for the session socket only.
                title,
            },
            Some(view) => {
                let offset = self.region_offset();
                let crop = self.crop_top();
                let mut host = Vec::new();
                if scrolled > 0 {
                    // Mirror the scroll for real on the host so its native
                    // scrollback accumulates: delete the live region's rows
                    // (DL affects no scrollback), append the newly archived
                    // lines as flowing text after the zone content, then
                    // scroll just enough to clear the region area for the
                    // repaint. Net effect: exactly `scrolled` rows push into
                    // the host's scrollback, in order — the zone's oldest
                    // rows followed by the oldest new lines — while the
                    // newest lines settle in the zone directly above the
                    // region. (Lines wider than the host wrap and consume
                    // extra rows; order is still preserved.)
                    let region_rows = view.rows.min(self.host_rows);
                    host.extend_from_slice(SYNC_BEGIN);
                    host.extend_from_slice(HIDE_CURSOR);
                    host.extend_from_slice(SGR_RESET);
                    host.extend_from_slice(cup(offset, 0).as_bytes());
                    host.extend_from_slice(format!("\x1b[{region_rows}M").as_bytes());
                    for (i, record) in self.history.tail(scrolled).enumerate() {
                        if i > 0 {
                            host.extend_from_slice(b"\r\n");
                        }
                        host.extend_from_slice(&record.bytes);
                        host.extend_from_slice(SGR_RESET);
                    }
                    host.extend_from_slice(
                        cup(self.host_rows.saturating_sub(1), 0).as_bytes(),
                    );
                    host.extend_from_slice(
                        &b"\r\n".repeat(scrolled.min(usize::from(region_rows))),
                    );
                    host.extend_from_slice(SYNC_END);
                    self.zone_dirty = true;
                }
                let shadow = self.shadow.as_mut().expect("shadow exists while viewing");
                let screen = self.parser.screen();
                if scrolled > 0 {
                    // The scroll shifted the whole host screen, so the
                    // region contents are unknown: clear-and-redraw it.
                    let (rows, cols) = screen.size();
                    let blank = blank_screen(rows, cols);
                    host.extend_from_slice(&render_rows(
                        screen,
                        &blank,
                        self.host_rows,
                        self.host_cols,
                        true,
                        offset,
                        crop,
                    ));
                } else {
                    host.extend_from_slice(&render_rows(
                        screen,
                        shadow.screen(),
                        self.host_rows,
                        self.host_cols,
                        false,
                        offset,
                        crop,
                    ));
                }
                // The render paths only reproduce screen contents, so a
                // title change must be re-emitted to the host explicitly.
                if let Some(t) = &title {
                    host.extend_from_slice(format!("\x1b]2;{t}\x07").as_bytes());
                }
                // The device terminal is exactly view-sized, so the
                // same-size diff stream is correct for it.
                let remote = Some(screen.state_diff(shadow.screen()));
                shadow.process(bytes);
                Output {
                    host,
                    remote,
                    bells,
                    title,
                }
            }
        }
    }

    /// Number of rows currently in the staging scrollback.
    fn staged_len(&mut self) -> usize {
        let screen = self.parser.screen_mut();
        screen.set_scrollback(usize::MAX);
        screen.scrollback()
    }

    /// Arm the scroll counter before parsing a slice. Skipped while the app
    /// is on the alternate screen (which has no scrollback of its own, and
    /// during which the primary screen cannot scroll).
    fn park(&mut self) {
        if self.parser.screen().alternate_screen() {
            return;
        }
        if !self.parked && self.staged_len() > 0 {
            self.parser.screen_mut().set_scrollback(1);
            self.parked = true;
        } else if !self.parked {
            self.parser.screen_mut().set_scrollback(0);
        }
    }

    /// Archive rows that scrolled into the staging buffer since the last
    /// harvest. Deferred (state kept) while the alternate screen is active.
    fn harvest(&mut self) {
        if self.parser.screen().alternate_screen() {
            return;
        }
        let new_rows = if self.parked {
            // Exact even if the staging buffer overflowed meanwhile.
            self.parser.screen().scrollback().saturating_sub(1)
        } else {
            // Growth since last look; exact until the buffer first fills.
            self.staged_len().saturating_sub(self.staged_seen)
        };
        self.parked = false;
        if new_rows > 0 {
            self.archive_staged_rows(new_rows);
        }
        self.staged_seen = self.staged_len();
        self.parser.screen_mut().set_scrollback(0);
    }

    /// Serialize the newest `count` staged rows (oldest first) into history.
    fn archive_staged_rows(&mut self, count: usize) {
        let (rows, cols) = self.parser.screen().size();
        let mut remaining = count.min(self.staged_len());
        self.parser.screen_mut().set_scrollback(0);
        while remaining > 0 {
            // Scrolling back by `remaining` puts the oldest unarchived rows
            // at the top of the visible window.
            self.parser.screen_mut().set_scrollback(remaining);
            let window = remaining.min(usize::from(rows));
            for row in 0..window {
                let record = serialize_row(self.parser.screen(), row as u16, cols);
                self.history.push(record);
            }
            remaining -= window;
        }
    }

    fn take_bells(&mut self) -> usize {
        std::mem::take(&mut self.parser.callbacks_mut().bells)
    }

    /// The title set in the last processed chunk, if it differs from the
    /// current one.
    fn take_title_change(&mut self) -> Option<String> {
        let title = self.parser.callbacks_mut().title.take()?;
        if self.title.as_deref() == Some(title.as_str()) {
            return None;
        }
        self.title = Some(title.clone());
        Some(title)
    }

    /// A device started viewing: resize the child to the device dimensions
    /// and snapshot the current screen so the device has something to show
    /// until the app repaints in response to SIGWINCH.
    ///
    /// The snapshot uses the bottom-left of the current viewport: the last
    /// `rows` rows and first `cols` columns that fit the device screen.
    pub fn start_view(&mut self, view: View) -> Transition {
        self.flush_history_for_rebuild(view.rows);
        let snapshot = render_snapshot(self.parser.screen(), view.rows, view.cols);

        // Rebuild the model at device dimensions, primed with the snapshot,
        // so subsequent diffs (for both destinations) start from what the
        // device and host are actually displaying.
        let mut parser = new_parser(view.rows, view.cols);
        parser.process(&snapshot);
        self.parser = parser;
        let mut shadow = vt100::Parser::new(view.rows, view.cols, 0);
        shadow.process(&snapshot);
        self.shadow = Some(shadow);
        self.view = Some(view);
        self.zone_dirty = false;

        // No clear: the live region redraws the bottom of the host screen
        // in place (it shows the same bottom-anchored content the host was
        // already displaying), and whatever is above it stays visible as
        // the initial history zone. When the view is taller than the host,
        // the hidden top rows go into the host's native scrollback instead.
        let mut host_output = VIEW_HOST_PRELUDE.to_vec();
        host_output.extend_from_slice(&self.push_crop_to_host_scrollback());
        host_output.extend_from_slice(&self.render_host_full());
        Transition {
            child_size: Some((view.rows, view.cols)),
            host_output,
            remote_output: Some(snapshot),
        }
    }

    /// Before a parser rebuild (which discards the staging scrollback and,
    /// for a bottom-anchored crop, the rows above the crop window), preserve
    /// that content in the history archive: drain staged rows, then archive
    /// visible rows that the crop will drop, up to the last non-blank one.
    fn flush_history_for_rebuild(&mut self, keep_rows: u16) {
        self.harvest();
        if !self.parser.screen().alternate_screen() {
            let (src_rows, src_cols) = self.parser.screen().size();
            let dropped = usize::from(src_rows.saturating_sub(keep_rows));
            let records: Vec<HistoryRecord> = (0..dropped)
                .map(|row| serialize_row(self.parser.screen(), row as u16, src_cols))
                .collect();
            let last_nonblank = records.iter().rposition(|r| !r.bytes.is_empty());
            if let Some(last) = last_nonblank {
                for record in records.into_iter().take(last + 1) {
                    self.history.push(record);
                }
            }
        }
        self.parked = false;
        self.staged_seen = 0;
    }

    /// The device stopped viewing (unview message, client disconnect, or
    /// simulated toggle): back to pass-through at host dimensions.
    pub fn end_view(&mut self) -> Transition {
        // Drain staged history; the visible rows are not archived (they stay
        // conceptually on screen — the app repaints them at host size, and a
        // future view-start crop archives whatever ends up above the fold).
        self.harvest();
        self.parked = false;
        self.staged_seen = 0;
        // Preserve the history zone in the host's native scrollback before
        // clearing wipes it.
        let mut host_output = self.push_zone_to_host_scrollback();
        self.zone_dirty = false;
        self.view = None;
        self.shadow = None;
        self.parser = new_parser(self.host_rows, self.host_cols);
        // The child's SIGWINCH repaint will fill the host screen; reset any
        // input modes we enabled on the child's behalf, since from here on
        // the child manages the host terminal directly.
        host_output.extend_from_slice(CLEAR);
        host_output.extend_from_slice(RESET_INPUT_MODES);
        Transition {
            child_size: Some((self.host_rows, self.host_cols)),
            host_output,
            remote_output: None,
        }
    }

    /// The host terminal was resized.
    pub fn host_resized(&mut self, rows: u16, cols: u16) -> Transition {
        self.host_rows = rows;
        self.host_cols = cols;
        match self.view {
            // Pass-through: forward the new size to the child; keep the
            // model in step (preserving contents for future snapshots).
            None => {
                // Resizing clears wrap flags and can drop rows, so drain
                // staged history first. (Staged rows themselves survive
                // set_size.)
                self.harvest();
                self.parser.screen_mut().set_size(rows, cols);
                Transition {
                    child_size: Some((rows, cols)),
                    host_output: Vec::new(),
                    remote_output: None,
                }
            }
            // The device screen stays at its fixed size; only our layout on
            // the host changed, so repaint the whole host frame (after
            // rescuing the history zone — or, when the view no longer fits,
            // the freshly hidden top rows — into the host's scrollback).
            Some(_) => {
                let mut host_output = self.push_zone_to_host_scrollback();
                self.zone_dirty = false;
                host_output.extend_from_slice(&self.push_crop_to_host_scrollback());
                host_output.extend_from_slice(CLEAR);
                host_output.extend_from_slice(&self.render_host_full());
                Transition {
                    child_size: None,
                    host_output,
                    remote_output: None,
                }
            }
        }
    }

    /// Full repaint of the view model onto the host terminal (assumes the
    /// host was just cleared).
    fn render_host_full(&self) -> Vec<u8> {
        let screen = self.parser.screen();
        let (rows, cols) = screen.size();
        let blank = blank_screen(rows, cols);
        render_rows(
            screen,
            &blank,
            self.host_rows,
            self.host_cols,
            true,
            self.region_offset(),
            self.crop_top(),
        )
    }

    /// Bytes to restore the host terminal when exiting while a view is
    /// active.
    pub fn cleanup(&self) -> Vec<u8> {
        match self.view {
            None => Vec::new(),
            Some(_) => {
                // Rescue the history zone first; the final screen contents
                // stay visible above the shell prompt that follows.
                let mut out = self.push_zone_to_host_scrollback();
                out.extend_from_slice(SGR_RESET);
                out.extend_from_slice(SHOW_CURSOR);
                out.extend_from_slice(RESET_INPUT_MODES);
                out.extend_from_slice(cup(self.host_rows.saturating_sub(1), 0).as_bytes());
                out.extend_from_slice(b"\r\n");
                out
            }
        }
    }
}

fn blank_screen(rows: u16, cols: u16) -> vt100::Screen {
    vt100::Parser::new(rows, cols, 0).screen().clone()
}

/// Render the part of `screen` that fits in a `host_rows` x `host_cols`
/// terminal, as a diff from `prev` (same size as `screen`), placed with its
/// top at host row `row_offset` (the live region is anchored at the bottom
/// of the host screen; rows above it are the history zone). `src_off` skips
/// that many rows at the top of the model — when the view is taller than
/// the host, the host shows the model's bottom. Every row is explicitly
/// positioned, so the output is correct on any terminal at least as large
/// as the rendered window. `full` selects full-state emission for a target
/// whose region contents are unknown: every region row is cleared and
/// redrawn rather than diffed.
fn render_rows(
    screen: &vt100::Screen,
    prev: &vt100::Screen,
    host_rows: u16,
    host_cols: u16,
    full: bool,
    row_offset: u16,
    src_off: u16,
) -> Vec<u8> {
    let (screen_rows, screen_cols) = screen.size();
    let rows = host_rows.min(screen_rows.saturating_sub(src_off));
    let cols = host_cols.min(screen_cols);
    let mut out = SYNC_BEGIN.to_vec();
    out.extend_from_slice(HIDE_CURSOR);
    for (i, row_diff) in screen
        .rows_diff(prev, 0, cols)
        .enumerate()
        .skip(src_off.into())
        .take(rows.into())
    {
        let target_row = row_offset + i as u16 - src_off;
        if full {
            out.extend_from_slice(SGR_RESET);
            out.extend_from_slice(cup(target_row, 0).as_bytes());
            out.extend_from_slice(CLEAR_TO_EOL);
            out.extend_from_slice(&row_diff);
        } else if !row_diff.is_empty() {
            out.extend_from_slice(SGR_RESET);
            out.extend_from_slice(cup(target_row, 0).as_bytes());
            out.extend_from_slice(&row_diff);
        }
    }
    if full {
        out.extend_from_slice(&screen.input_mode_formatted());
    } else {
        out.extend_from_slice(&screen.input_mode_diff(prev));
    }
    place_cursor(&mut out, screen, src_off, rows, cols, row_offset);
    out.extend_from_slice(SYNC_END);
    out
}

/// Escape sequences reproducing the bottom-left crop of `src` on a fresh
/// `rows` x `cols` terminal: the last `rows` rows, first `cols` columns.
fn render_snapshot(src: &vt100::Screen, rows: u16, cols: u16) -> Vec<u8> {
    let (src_rows, src_cols) = src.size();
    let row_off = src_rows.saturating_sub(rows);
    let width = cols.min(src_cols);
    let blank = blank_screen(src_rows, src_cols);
    let mut out = CLEAR.to_vec();
    out.extend_from_slice(SYNC_BEGIN);
    out.extend_from_slice(HIDE_CURSOR);
    for (i, row) in src
        .rows_diff(&blank, 0, width)
        .enumerate()
        .skip(row_off.into())
        .take(rows.into())
    {
        if !row.is_empty() {
            out.extend_from_slice(SGR_RESET);
            out.extend_from_slice(cup(i as u16 - row_off, 0).as_bytes());
            out.extend_from_slice(&row);
        }
    }
    out.extend_from_slice(&src.input_mode_formatted());
    place_cursor(&mut out, src, row_off, rows, cols, 0);
    out.extend_from_slice(SYNC_END);
    out
}

/// Position the target cursor at the screen's cursor if it falls inside the
/// rendered window (source rows `src_off..src_off+rows`, cols `0..cols`),
/// drawn at target rows starting at `target_off`; otherwise leave it hidden
/// (render helpers hide it up front).
fn place_cursor(
    out: &mut Vec<u8>,
    screen: &vt100::Screen,
    src_off: u16,
    rows: u16,
    cols: u16,
    target_off: u16,
) {
    let (row, col) = screen.cursor_position();
    if !screen.hide_cursor() && row >= src_off && row - src_off < rows && col < cols {
        out.extend_from_slice(cup(target_off + row - src_off, col).as_bytes());
        out.extend_from_slice(SHOW_CURSOR);
    }
}

fn cup(row: u16, col: u16) -> String {
    format!("\x1b[{};{}H", row + 1, col + 1)
}

#[derive(Clone, Copy, PartialEq)]
struct CellStyle {
    fg: vt100::Color,
    bg: vt100::Color,
    bold: bool,
    dim: bool,
    italic: bool,
    underline: bool,
    inverse: bool,
}

const DEFAULT_STYLE: CellStyle = CellStyle {
    fg: vt100::Color::Default,
    bg: vt100::Color::Default,
    bold: false,
    dim: false,
    italic: false,
    underline: false,
    inverse: false,
};

impl CellStyle {
    fn of(cell: &vt100::Cell) -> Self {
        Self {
            fg: cell.fgcolor(),
            bg: cell.bgcolor(),
            bold: cell.bold(),
            dim: cell.dim(),
            italic: cell.italic(),
            underline: cell.underline(),
            inverse: cell.inverse(),
        }
    }

    /// Whether a cell with no contents in this style is invisible (safe to
    /// trim from the end of a line). A colored or inverted background is
    /// content (back-color-erase paints cells this way).
    fn blank_is_invisible(&self) -> bool {
        self.bg == vt100::Color::Default && !self.inverse
    }
}

/// Append one SGR sequence that switches from any style to `style`,
/// starting with a full reset so the result is self-contained.
fn emit_sgr(out: &mut Vec<u8>, style: &CellStyle) {
    out.extend_from_slice(b"\x1b[0");
    if style.bold {
        out.extend_from_slice(b";1");
    }
    if style.dim {
        out.extend_from_slice(b";2");
    }
    if style.italic {
        out.extend_from_slice(b";3");
    }
    if style.underline {
        out.extend_from_slice(b";4");
    }
    if style.inverse {
        out.extend_from_slice(b";7");
    }
    emit_color(out, style.fg, false);
    emit_color(out, style.bg, true);
    out.push(b'm');
}

fn emit_color(out: &mut Vec<u8>, color: vt100::Color, background: bool) {
    let base = if background { 10 } else { 0 };
    match color {
        vt100::Color::Default => {}
        vt100::Color::Idx(i) if i < 8 => {
            out.extend_from_slice(format!(";{}", 30 + base + u16::from(i)).as_bytes());
        }
        vt100::Color::Idx(i) if i < 16 => {
            out.extend_from_slice(format!(";{}", 82 + base + u16::from(i)).as_bytes());
        }
        vt100::Color::Idx(i) => {
            out.extend_from_slice(format!(";{};5;{i}", 38 + base).as_bytes());
        }
        vt100::Color::Rgb(r, g, b) => {
            out.extend_from_slice(format!(";{};2;{r};{g};{b}", 38 + base).as_bytes());
        }
    }
}

/// Serialize one visible row into a self-contained history record: printable
/// text plus SGR sequences only, starting from default attributes, trailing
/// invisible cells trimmed. `width` is recorded as the layout width.
fn serialize_row(screen: &vt100::Screen, row: u16, width: u16) -> HistoryRecord {
    // Find the last cell that would be visible: contents, or a blank whose
    // style shows (colored/inverted background).
    let mut end = None;
    for col in 0..width {
        let Some(cell) = screen.cell(row, col) else { break };
        let has_visible_contents =
            cell.has_contents() && !(cell.contents() == " " && CellStyle::of(cell) == DEFAULT_STYLE);
        if has_visible_contents || (!cell.has_contents() && !CellStyle::of(cell).blank_is_invisible())
        {
            end = Some(col);
        }
    }
    let mut bytes = Vec::new();
    if let Some(end) = end {
        let mut current = DEFAULT_STYLE;
        for col in 0..=end {
            let Some(cell) = screen.cell(row, col) else { break };
            if cell.is_wide_continuation() {
                continue;
            }
            let style = CellStyle::of(cell);
            if style != current {
                emit_sgr(&mut bytes, &style);
                current = style;
            }
            if cell.has_contents() {
                bytes.extend_from_slice(cell.contents().as_bytes());
            } else {
                bytes.push(b' ');
            }
            if bytes.len() >= MAX_LINE_BYTES {
                break;
            }
        }
    }
    HistoryRecord {
        bytes,
        width,
        wrapped: screen.row_wrapped(row),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIM: View = View {
        rows: SIM_ROWS,
        cols: SIM_COLS,
        simulated: true,
    };

    /// A fake terminal: feed it what Mirror emits, inspect the result.
    fn term(rows: u16, cols: u16) -> vt100::Parser {
        vt100::Parser::new(rows, cols, 0)
    }

    fn start_view(mirror: &mut Mirror, host: &mut vt100::Parser) -> Transition {
        let t = mirror.start_view(SIM);
        assert_eq!(t.child_size, Some((SIM_ROWS, SIM_COLS)));
        host.process(&t.host_output);
        t
    }

    #[test]
    fn local_mode_passes_through() {
        let mut mirror = Mirror::new(24, 80);
        let out = mirror.process(b"hello \x1b[31mred");
        assert_eq!(out.host, b"hello \x1b[31mred");
        assert!(out.remote.is_none());
    }

    #[test]
    fn host_resize_in_local_mode_forwards_size() {
        let mut mirror = Mirror::new(24, 80);
        let t = mirror.host_resized(50, 132);
        assert_eq!(t.child_size, Some((50, 132)));
        assert!(t.host_output.is_empty());
    }

    #[test]
    fn view_content_reproduced_on_larger_host() {
        let mut mirror = Mirror::new(40, 120);
        let mut host = term(40, 120);
        start_view(&mut mirror, &mut host);
        let out = mirror.process(b"hello\r\nworld");
        host.process(&out.host);
        // The live region is anchored at the bottom of the host screen:
        // view row 0 lands at host row 40 - 24 = 16.
        assert_eq!(host.screen().contents_between(16, 0, 16, 5), "hello");
        assert_eq!(host.screen().contents_between(17, 0, 17, 5), "world");
        assert_eq!(host.screen().cursor_position(), (17, 5));
    }

    /// Regression test: a line that wraps in the view model must not spill
    /// into host columns to the right of the view region. (vt100's
    /// whole-screen diff reproduces wraps by relying on autowrap at the
    /// model width, which doesn't fire on a wider host terminal.)
    #[test]
    fn wrapped_lines_stay_inside_view_region_on_wider_host() {
        let mut mirror = Mirror::new(40, 150);
        let mut host = term(40, 150);
        start_view(&mut mirror, &mut host);
        // 150 chars: wraps at col 96 in the model into two rows. Region
        // starts at host row 16 (40 - 24).
        let long: Vec<u8> = (0..150).map(|i| b'a' + (i % 26) as u8).collect();
        let out = mirror.process(&long);
        host.process(&out.host);
        let row0 = host.screen().contents_between(16, 0, 16, 150);
        assert_eq!(row0.len(), 96, "row must stop at the view width");
        // The wrapped remainder appears on the next row, not in cols 97-150.
        assert_eq!(
            host.screen().contents_between(17, 0, 17, 150),
            String::from_utf8_lossy(&long[96..])
        );
    }

    /// Stale host content inside the live region (including right of the
    /// view width) is cleared when the view starts, while content in the
    /// history zone above the region deliberately stays on screen.
    #[test]
    fn view_start_clears_region_but_preserves_zone() {
        let mut mirror = Mirror::new(40, 150);
        let mut host = term(40, 150);
        // Local mode: content in the future zone (row 5), and content in the
        // future region rows painted out to col 150.
        let wide = b"\x1b[5;1Hzone-note\x1b[30;120Hstale-right\x1b[40;1Hbottom";
        host.process(&mirror.process(wide).host);
        assert!(host.screen().contents().contains("stale-right"));

        // Region occupies host rows 16..40; the snapshot crop keeps model
        // rows 16..40, cropped to 96 cols — so "stale-right" (row 30, col
        // 120) must be wiped by the region redraw, while "zone-note" stays.
        start_view(&mut mirror, &mut host);
        // "bottom" (model row 39) is inside the crop and redrawn in place.
        assert_eq!(host.screen().contents_between(39, 0, 39, 6), "bottom");
        let out = mirror.process(b"\x1b[2J\x1b[Hfresh");
        host.process(&out.host);
        assert!(
            !host.screen().contents().contains("stale-right"),
            "region content right of the view width must be cleared"
        );
        assert!(host.screen().contents().contains("fresh"));
        assert!(
            host.screen().contents().contains("zone-note"),
            "pre-view content above the region stays visible"
        );
    }

    #[test]
    fn snapshot_crops_bottom_left_and_primes_remote() {
        let mut mirror = Mirror::new(40, 120);
        // Paint a marker near the bottom (row 39) and one at the top.
        mirror.process(b"\x1b[1;1Htop-marker\x1b[39;5Hbottom-marker");
        let t = mirror.start_view(SIM);

        // The remote device terminal is exactly view-sized.
        let mut remote = term(SIM_ROWS, SIM_COLS);
        remote.process(&t.remote_output.unwrap());
        let contents = remote.screen().contents();
        // Bottom-anchored crop of a 40-row screen onto 24 rows: rows 16..40.
        // Row 39 -> view row 23; the top marker is above the crop.
        assert!(contents.contains("bottom-marker"));
        assert!(!contents.contains("top-marker"));
        assert_eq!(
            remote.screen().contents_between(38 - 16, 4, 38 - 16, 17),
            "bottom-marker"
        );

        // Subsequent output diffs apply cleanly to the primed remote.
        let out = mirror.process(b"\x1b[1;1Hafter");
        remote.process(&out.remote.unwrap());
        assert_eq!(remote.screen().contents_between(0, 0, 0, 5), "after");
        assert!(remote.screen().contents().contains("bottom-marker"));
    }

    #[test]
    fn wide_line_truncated_on_narrow_host() {
        let mut mirror = Mirror::new(24, 80);
        let mut host = term(24, 80);
        start_view(&mut mirror, &mut host);
        let wide: Vec<u8> = std::iter::repeat_n(b'X', usize::from(SIM_COLS))
            .collect();
        host.process(&mirror.process(&wide).host);
        assert_eq!(host.screen().contents_between(0, 0, 0, 80), "X".repeat(80));
        // Nothing wrapped onto the next row.
        assert_eq!(host.screen().contents_between(1, 0, 1, 80).trim(), "");
    }

    #[test]
    fn cursor_outside_truncated_region_is_hidden() {
        let mut mirror = Mirror::new(24, 80);
        let mut host = term(24, 80);
        start_view(&mut mirror, &mut host);
        // Move the child cursor to column 91 (past the host's 80 columns).
        host.process(&mirror.process(b"\x1b[1;91H").host);
        assert!(host.screen().hide_cursor());
        // Bring it back into view.
        host.process(&mirror.process(b"\x1b[1;10H").host);
        assert!(!host.screen().hide_cursor());
        assert_eq!(host.screen().cursor_position(), (0, 9));
    }

    /// A view taller than the host shows the model's *bottom* rows (where
    /// the prompt and fresh output live); the hidden top rows are pushed
    /// into the host's native scrollback, refreshed on demand (Ctrl+L).
    #[test]
    fn tall_view_shows_bottom_and_ctrl_l_pushes_hidden_top_to_scrollback() {
        let mut mirror = Mirror::new(10, 120);
        let mut host = vt100::Parser::new(10, 120, 100);
        // SIM view is 24 rows on a 10-row host: model rows 0..14 hidden.
        host.process(&mirror.start_view(SIM).host_output);
        let out = mirror.process(b"\x1b[15;1Hvisible-bottom\x1b[1;1Htop-hidden");
        host.process(&out.host);
        // Model row 14 (CSI row 15) is the first visible row -> host row 0.
        assert_eq!(host.screen().contents_between(0, 0, 0, 14), "visible-bottom");
        assert!(!host.screen().contents().contains("top-hidden"));
        // The cursor ended on a hidden row: not shown.
        assert!(host.screen().hide_cursor());

        // The hidden row was written after the view started, so the copy
        // pushed at view start is stale (blank); Ctrl+L heals it.
        host.process(&mirror.refresh_scrollback());
        let lines = host_lines(&mut host);
        assert_eq!(
            lines,
            vec!["top-hidden".to_string(), "visible-bottom".to_string()],
            "scrollback + screen show the whole model, in order"
        );
    }

    /// Scroll mirroring and the hidden-top push compose: after a Ctrl+L,
    /// scrollback + screen reproduce every line of a scrolled session in
    /// order, even though the middle rows were never visible on the host.
    #[test]
    fn crop_case_scrollback_is_complete_after_refresh() {
        let mut mirror = Mirror::new(5, 40);
        let mut host = vt100::Parser::new(5, 40, 100);
        let t = mirror.start_view(View {
            rows: 8,
            cols: 40,
            simulated: true,
        });
        host.process(&t.host_output);
        // 13 lines through an 8-row model: L1..L5 scroll into history (and
        // the host's scrollback); the model shows L6..end, of which only
        // the bottom 5 rows (L9..end) fit the host screen.
        let mut bytes = Vec::new();
        for i in 1..=12 {
            bytes.extend_from_slice(format!("L{i}\r\n").as_bytes());
        }
        bytes.extend_from_slice(b"end");
        host.process(&mirror.process(&bytes).host);
        let expect = |nums: &[u32]| {
            let mut v: Vec<String> = nums.iter().map(|i| format!("L{i}")).collect();
            v.push("end".into());
            v
        };
        assert_eq!(
            host_lines(&mut host),
            expect(&[1, 2, 3, 4, 5, 9, 10, 11, 12]),
            "scrolled lines reach scrollback; L6..L8 are hidden model rows"
        );
        // Ctrl+L pushes the hidden rows, completing the sequence in order.
        host.process(&mirror.refresh_scrollback());
        assert_eq!(
            host_lines(&mut host),
            expect(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            "after refresh, nothing is missing and order holds"
        );
    }

    #[test]
    fn end_view_restores_host_size_and_resets_modes() {
        let mut mirror = Mirror::new(24, 80);
        let mut host = term(24, 80);
        start_view(&mut mirror, &mut host);
        host.process(&mirror.process(b"\x1b[?2004h").host); // bracketed paste on
        assert!(host.screen().bracketed_paste());
        let t = mirror.end_view();
        assert_eq!(t.child_size, Some((24, 80)));
        host.process(&t.host_output);
        assert!(!host.screen().bracketed_paste());
        assert!(mirror.view().is_none());
    }

    #[test]
    fn bells_are_counted_in_both_modes() {
        let mut mirror = Mirror::new(24, 80);
        assert_eq!(mirror.process(b"quiet output").bells, 0);
        assert_eq!(mirror.process(b"ding\x07dong\x07").bells, 2);
        // BEL as an OSC string terminator is not a bell.
        assert_eq!(mirror.process(b"\x1b]0;window title\x07").bells, 0);
        // Counter resets between chunks.
        assert_eq!(mirror.process(b"still quiet").bells, 0);

        let mut host = term(24, 80);
        start_view(&mut mirror, &mut host);
        assert_eq!(mirror.process(b"viewing\x07").bells, 1);
    }

    #[test]
    fn title_changes_are_reported_once_per_change() {
        let mut mirror = Mirror::new(24, 80);
        assert_eq!(mirror.process(b"no title yet").title, None);
        assert_eq!(mirror.title(), None);

        // OSC 2, BEL-terminated.
        let out = mirror.process(b"\x1b]2;first title\x07");
        assert_eq!(out.title.as_deref(), Some("first title"));
        assert_eq!(mirror.title(), Some("first title"));

        // Setting the same title again is not a change.
        assert_eq!(mirror.process(b"\x1b]2;first title\x07").title, None);

        // OSC 0 (icon + title), ST-terminated.
        let out = mirror.process(b"\x1b]0;second title\x1b\\");
        assert_eq!(out.title.as_deref(), Some("second title"));

        // OSC 1 sets only the icon name, not the title.
        assert_eq!(mirror.process(b"\x1b]1;icon only\x07").title, None);
        assert_eq!(mirror.title(), Some("second title"));
    }

    #[test]
    fn title_survives_view_transitions_and_reaches_host_while_viewing() {
        let mut mirror = Mirror::new(24, 80);
        mirror.process(b"\x1b]2;steady title\x07");

        let mut host = term(24, 80);
        start_view(&mut mirror, &mut host);
        assert_eq!(mirror.title(), Some("steady title"));

        // While viewing, render paths don't carry OSC sequences, so a title
        // change must be re-emitted to the host explicitly.
        let out = mirror.process(b"\x1b]2;during view\x07");
        assert_eq!(out.title.as_deref(), Some("during view"));
        assert!(
            String::from_utf8_lossy(&out.host).contains("\x1b]2;during view\x07"),
            "host output must include the title sequence"
        );

        mirror.end_view();
        assert_eq!(mirror.title(), Some("during view"));
    }

    /// Render an archived record on a fresh single-row terminal.
    fn render_record(record: &HistoryRecord) -> vt100::Parser {
        let mut p = vt100::Parser::new(1, record.width, 0);
        p.process(&record.bytes);
        p
    }

    fn history_texts(mirror: &Mirror) -> Vec<String> {
        let (next, oldest) = mirror.history_extent();
        let (_, records) = mirror.history().fetch(next, (next - oldest) as u32);
        records
            .iter()
            .map(|r| render_record(r).screen().contents().trim_end().to_string())
            .collect()
    }

    #[test]
    fn scrolled_lines_are_archived_in_order_with_styles() {
        let mut mirror = Mirror::new(3, 40);
        mirror.process(b"plain\r\n\x1b[31mred line\x1b[0m\r\nthird\r\nfourth\r\nfifth");
        // 5 lines through a 3-row screen: the first two scrolled off.
        assert_eq!(history_texts(&mirror), vec!["plain", "red line"]);
        let (_, records) = mirror.history().fetch(2, 10);
        let rendered = render_record(records[1]);
        let cell = rendered.screen().cell(0, 0).unwrap();
        assert_eq!(cell.fgcolor(), vt100::Color::Idx(1), "styles preserved");
        assert_eq!(records[1].width, 40);
        assert!(!records[1].wrapped);
    }

    #[test]
    fn soft_wrapped_lines_carry_wrap_flags() {
        let mut mirror = Mirror::new(3, 10);
        // 25 chars wrap into 3 rows; then enough lines to scroll them off.
        let long: Vec<u8> = (0..25).map(|i| b'a' + (i % 26) as u8).collect();
        mirror.process(&long);
        mirror.process(b"\r\nx\r\ny\r\nz\r\nw");
        let (next, _) = mirror.history_extent();
        let (_, records) = mirror.history().fetch(next, 10);
        let wrapped: Vec<bool> = records.iter().map(|r| r.wrapped).collect();
        // The two full rows of the long line wrap; its final row does not.
        assert_eq!(&wrapped[0..3], &[true, true, false]);
    }

    #[test]
    fn flood_larger_than_staging_buffer_is_archived_completely() {
        let mut mirror = Mirror::new(4, 20);
        let mut bytes = Vec::new();
        let count = STAGE_SCROLLBACK + 1500;
        for i in 0..count {
            bytes.extend_from_slice(format!("L{i}\r\n").as_bytes());
        }
        mirror.process(&bytes);
        let (next, oldest) = mirror.history_extent();
        assert_eq!(next, (count - 3) as u64, "all scrolled lines captured");
        // Retention cap applies (default 10k > count here), continuity holds.
        let (_, records) = mirror.history().fetch(next, 3);
        let texts: Vec<String> = records
            .iter()
            .map(|r| render_record(r).screen().contents().trim_end().to_string())
            .collect();
        assert_eq!(
            texts,
            vec![
                format!("L{}", count - 6),
                format!("L{}", count - 5),
                format!("L{}", count - 4)
            ]
        );
        assert_eq!(oldest, 0);
    }

    #[test]
    fn view_start_crop_flushes_rows_above_the_fold_to_history() {
        let mut mirror = Mirror::new(6, 40);
        let mut host = term(6, 40);
        mirror.process(b"top-line\r\nsecond\r\n\r\n\r\n\r\nprompt");
        // View is 3 rows tall: the bottom-anchored crop keeps rows 3..6;
        // rows 0..3 are flushed to history (trailing blanks skipped).
        let t = mirror.start_view(View {
            rows: 3,
            cols: 40,
            simulated: true,
        });
        host.process(&t.host_output);
        assert_eq!(history_texts(&mirror), vec!["top-line", "second"]);
        // The snapshot itself shows the bottom of the screen.
        let mut remote = term(3, 40);
        remote.process(&t.remote_output.unwrap());
        assert!(remote.screen().contents().contains("prompt"));
    }

    #[test]
    fn alternate_screen_output_is_not_archived() {
        let mut mirror = Mirror::new(3, 40);
        mirror.process(b"before-alt\r\nsecond\r\nthird\r\nfourth");
        let before = mirror.history_extent().0;
        // Enter the alternate screen and scroll a lot inside it.
        let mut alt = b"\x1b[?1049h".to_vec();
        for i in 0..50 {
            alt.extend_from_slice(format!("alt{i}\r\n").as_bytes());
        }
        alt.extend_from_slice(b"\x1b[?1049l");
        mirror.process(&alt);
        assert_eq!(
            mirror.history_extent().0,
            before,
            "alt-screen scrolling must not create history"
        );
        // Scrolling on the primary screen still archives afterwards.
        mirror.process(b"\r\nafter1\r\nafter2\r\nafter3\r\nafter4");
        assert!(mirror.history_extent().0 > before);
    }

    #[test]
    fn trailing_blanks_trim_but_colored_blanks_are_content() {
        let mut mirror = Mirror::new(2, 20);
        // Line 1: text plus trailing spaces. Line 2: a back-color-erased
        // region (colored blanks) after text. Scroll both off.
        mirror.process(b"hi      \r\nab\x1b[41m\x1b[K\x1b[0m\r\n1\r\n2");
        let (_, records) = mirror.history().fetch(2, 10);
        assert_eq!(records[0].bytes, b"hi");
        let rendered = render_record(records[1]);
        assert_eq!(rendered.screen().contents_between(0, 0, 0, 2), "ab");
        let blank = rendered.screen().cell(0, 5).unwrap();
        assert_eq!(
            blank.bgcolor(),
            vt100::Color::Idx(1),
            "BCE-colored blanks survive archiving"
        );
    }

    #[test]
    fn history_width_tracks_the_layout_width_across_views() {
        let mut mirror = Mirror::new(3, 40);
        mirror.process(b"one\r\ntwo\r\nthree\r\nfour"); // "one" scrolls at 40
        // A 2-row view: the crop flushes "two" (still at width 40), and
        // lines scrolling during the view are laid out at 96.
        mirror.start_view(View {
            rows: 2,
            cols: 96,
            simulated: true,
        });
        mirror.process(b"\r\na\r\nb\r\nc"); // scrolls at 96
        let (next, _) = mirror.history_extent();
        let (_, records) = mirror.history().fetch(next, 10);
        assert_eq!(records.first().unwrap().width, 40);
        assert_eq!(records.last().unwrap().width, 96);
        let texts = history_texts(&mirror);
        assert!(texts.contains(&"two".to_string()), "crop flush at old width");
        assert!(texts.contains(&"three".to_string()), "scrolled during view");
    }

    /// All lines (scrollback plus screen) of a host emulator, oldest first,
    /// trimmed, with blank lines dropped.
    fn host_lines(host: &mut vt100::Parser) -> Vec<String> {
        host.screen_mut().set_scrollback(usize::MAX);
        let total = host.screen().scrollback();
        let mut lines = Vec::new();
        for i in 0..total {
            host.screen_mut().set_scrollback(total - i);
            lines.push(host.screen().rows(0, 200).next().unwrap_or_default());
        }
        host.screen_mut().set_scrollback(0);
        lines.extend(host.screen().rows(0, 200));
        lines
            .into_iter()
            .map(|l| l.trim_end().to_string())
            .filter(|l| !l.is_empty())
            .collect()
    }

    #[test]
    fn viewing_scrolls_reach_the_host_terminals_native_scrollback() {
        let mut mirror = Mirror::new(6, 40);
        // Host emulator with real scrollback, like the user's terminal.
        let mut host = vt100::Parser::new(6, 40, 100);
        let t = mirror.start_view(View {
            rows: 3,
            cols: 40,
            simulated: true,
        });
        host.process(&t.host_output);
        // Ten lines through a 3-row view: 7 scroll off (more than the
        // region holds, exercising the flood path too).
        let mut bytes = Vec::new();
        for i in 1..=10 {
            bytes.extend_from_slice(format!("L{i}\r\n").as_bytes());
        }
        bytes.extend_from_slice(b"end");
        host.process(&mirror.process(&bytes).host);

        let lines = host_lines(&mut host);
        let expected: Vec<String> = (1..=10)
            .map(|i| format!("L{i}"))
            .chain(["end".to_string()])
            .collect();
        assert_eq!(
            lines, expected,
            "host scrollback + zone + region must show every line in order"
        );

        // Detaching pushes the zone into the host's scrollback before
        // clearing, so nothing that scrolled during the view is lost.
        let t = mirror.end_view();
        host.process(&t.host_output);
        let lines = host_lines(&mut host);
        // The model ended showing L9/L10/end, which never scrolled and stay
        // the app's to repaint; every line that DID scroll (L1..L8) must be
        // in the host's scrollback after the zone push + clear.
        let scrolled: Vec<String> = (1..=8).map(|i| format!("L{i}")).collect();
        assert_eq!(
            lines, scrolled,
            "after unview, every scrolled line is in scrollback"
        );
    }

    #[test]
    fn host_resize_while_viewing_repaints_without_resizing_child() {
        let mut mirror = Mirror::new(40, 120);
        let mut host = term(40, 120);
        start_view(&mut mirror, &mut host);
        host.process(&mirror.process(b"steady\x1b[24;1Hbottom").host);
        // Host shrinks below the view's 24 rows; child must NOT be resized.
        // The view model's bottom stays visible; "steady" (model row 0) is
        // now above the fold and gets pushed into the host's scrollback.
        let t = mirror.host_resized(20, 60);
        assert_eq!(t.child_size, None);
        let mut small_host = vt100::Parser::new(20, 60, 100);
        small_host.process(&t.host_output);
        // Model row 23 -> host row 19 (crop of 4).
        assert_eq!(small_host.screen().contents_between(19, 0, 19, 6), "bottom");
        assert_eq!(
            host_lines(&mut small_host),
            vec!["steady".to_string(), "bottom".to_string()],
            "the hidden top row is preserved in scrollback"
        );
    }
}
