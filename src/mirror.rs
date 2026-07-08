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

/// Dimensions used for the hotkey-simulated device.
pub const SIM_ROWS: u16 = 24;
pub const SIM_COLS: u16 = 96;

const CLEAR: &[u8] = b"\x1b[0m\x1b[H\x1b[2J";
const SGR_RESET: &[u8] = b"\x1b[0m";
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
    vt100::Parser::new_with_callbacks(rows, cols, 0, Events::default())
}

pub struct Mirror {
    host_rows: u16,
    host_cols: u16,
    /// Screen model of the child's output, always sized to the child's
    /// current dimensions (host size normally, view size while viewing).
    parser: vt100::Parser<Events>,
    view: Option<View>,
    /// Current window title, as last set by the app (survives view
    /// transitions, which rebuild the parser).
    title: Option<String>,
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
            view: None,
            title: None,
        }
    }

    pub fn view(&self) -> Option<View> {
        self.view
    }

    /// Current window title, as last set by the app.
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Translate a chunk of child output.
    pub fn process(&mut self, bytes: &[u8]) -> Output {
        match self.view {
            None => {
                self.parser.process(bytes);
                Output {
                    host: bytes.to_vec(),
                    remote: None,
                    bells: self.take_bells(),
                    // Pass-through already delivered the title sequence to
                    // the host; this is for the session socket only.
                    title: self.take_title_change(),
                }
            }
            Some(_) => {
                let prev = self.parser.screen().clone();
                self.parser.process(bytes);
                let bells = self.take_bells();
                let title = self.take_title_change();
                let screen = self.parser.screen();
                let mut host = render_rows(screen, &prev, self.host_rows, self.host_cols, false);
                // The render paths only reproduce screen contents, so a
                // title change must be re-emitted to the host explicitly.
                if let Some(t) = &title {
                    host.extend_from_slice(format!("\x1b]2;{t}\x07").as_bytes());
                }
                Output {
                    host,
                    // The device terminal is exactly view-sized, so the
                    // same-size diff stream is correct for it.
                    remote: Some(screen.state_diff(&prev)),
                    bells,
                    title,
                }
            }
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
        let snapshot = render_snapshot(self.parser.screen(), view.rows, view.cols);

        // Rebuild the model at device dimensions, primed with the snapshot,
        // so subsequent diffs (for both destinations) start from what the
        // device and host are actually displaying.
        let mut parser = new_parser(view.rows, view.cols);
        parser.process(&snapshot);
        self.parser = parser;
        self.view = Some(view);

        let mut host_output = CLEAR.to_vec();
        host_output.extend_from_slice(&self.render_host_full());
        Transition {
            child_size: Some((view.rows, view.cols)),
            host_output,
            remote_output: Some(snapshot),
        }
    }

    /// The device stopped viewing (unview message, client disconnect, or
    /// simulated toggle): back to pass-through at host dimensions.
    pub fn end_view(&mut self) -> Transition {
        self.view = None;
        self.parser = new_parser(self.host_rows, self.host_cols);
        // The child's SIGWINCH repaint will fill the host screen; reset any
        // input modes we enabled on the child's behalf, since from here on
        // the child manages the host terminal directly.
        let mut host_output = CLEAR.to_vec();
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
                self.parser.screen_mut().set_size(rows, cols);
                Transition {
                    child_size: Some((rows, cols)),
                    host_output: Vec::new(),
                    remote_output: None,
                }
            }
            // The device screen stays at its fixed size; only our truncation
            // window changed, so repaint the whole host frame.
            Some(_) => {
                let mut host_output = CLEAR.to_vec();
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
        render_rows(screen, &blank, self.host_rows, self.host_cols, true)
    }

    /// Bytes to restore the host terminal when exiting while a view is
    /// active.
    pub fn cleanup(&self) -> Vec<u8> {
        match self.view {
            None => Vec::new(),
            Some(view) => {
                let mut out = SGR_RESET.to_vec();
                out.extend_from_slice(SHOW_CURSOR);
                out.extend_from_slice(RESET_INPUT_MODES);
                let last_row = self.host_rows.min(view.rows);
                out.extend_from_slice(cup(last_row.saturating_sub(1), 0).as_bytes());
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
/// terminal, as a diff from `prev` (same size as `screen`). Every row is
/// explicitly positioned, so the output is correct on any terminal at least
/// as large as the rendered window — regions outside the window are never
/// touched. `full` selects full-state emission (fresh target) vs diff.
fn render_rows(
    screen: &vt100::Screen,
    prev: &vt100::Screen,
    host_rows: u16,
    host_cols: u16,
    full: bool,
) -> Vec<u8> {
    let (screen_rows, screen_cols) = screen.size();
    let rows = host_rows.min(screen_rows);
    let cols = host_cols.min(screen_cols);
    let mut out = SYNC_BEGIN.to_vec();
    out.extend_from_slice(HIDE_CURSOR);
    for (i, row_diff) in screen
        .rows_diff(prev, 0, cols)
        .take(rows.into())
        .enumerate()
    {
        if !row_diff.is_empty() {
            out.extend_from_slice(SGR_RESET);
            out.extend_from_slice(cup(i as u16, 0).as_bytes());
            out.extend_from_slice(&row_diff);
        }
    }
    if full {
        out.extend_from_slice(&screen.input_mode_formatted());
    } else {
        out.extend_from_slice(&screen.input_mode_diff(prev));
    }
    place_cursor(&mut out, screen, 0, rows, cols);
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
    place_cursor(&mut out, src, row_off, rows, cols);
    out.extend_from_slice(SYNC_END);
    out
}

/// Position the target cursor at the screen's cursor if it falls inside the
/// rendered window (`row_off..row_off+rows`, `0..cols`); otherwise leave it
/// hidden (render helpers hide it up front).
fn place_cursor(out: &mut Vec<u8>, screen: &vt100::Screen, row_off: u16, rows: u16, cols: u16) {
    let (row, col) = screen.cursor_position();
    if !screen.hide_cursor() && row >= row_off && row - row_off < rows && col < cols {
        out.extend_from_slice(cup(row - row_off, col).as_bytes());
        out.extend_from_slice(SHOW_CURSOR);
    }
}

fn cup(row: u16, col: u16) -> String {
    format!("\x1b[{};{}H", row + 1, col + 1)
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
        assert_eq!(host.screen().contents_between(0, 0, 0, 5), "hello");
        assert_eq!(host.screen().contents_between(1, 0, 1, 5), "world");
        assert_eq!(host.screen().cursor_position(), (1, 5));
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
        // 150 chars: wraps at col 96 in the model into two rows.
        let long: Vec<u8> = (0..150).map(|i| b'a' + (i % 26) as u8).collect();
        let out = mirror.process(&long);
        host.process(&out.host);
        let row0 = host.screen().contents_between(0, 0, 0, 150);
        assert_eq!(row0.len(), 96, "row 0 must stop at the view width");
        // The wrapped remainder appears on row 1, not in cols 97-150 of row 0.
        assert_eq!(
            host.screen().contents_between(1, 0, 1, 150),
            String::from_utf8_lossy(&long[96..])
        );
    }

    /// Regression test: stale host content right of the view region is
    /// cleared at view start and never re-painted by later diffs.
    #[test]
    fn stale_wide_content_cleared_when_view_starts() {
        let mut mirror = Mirror::new(40, 150);
        let mut host = term(40, 150);
        // Local mode: app paints all the way out to col 150.
        let wide = b"\x1b[1;120Hstale-right\x1b[30;1Hbottom";
        host.process(&mirror.process(wide).host);
        assert!(host.screen().contents().contains("stale-right"));

        start_view(&mut mirror, &mut host);
        let out = mirror.process(b"\x1b[2J\x1b[Hfresh");
        host.process(&out.host);
        assert!(
            !host.screen().contents().contains("stale-right"),
            "content right of the view region must be cleared"
        );
        assert!(host.screen().contents().contains("fresh"));
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

    #[test]
    fn tall_content_truncated_on_short_host() {
        let mut mirror = Mirror::new(10, 120);
        let mut host = term(10, 120);
        start_view(&mut mirror, &mut host);
        // Write a marker on view row 15, below the 10-row host window.
        host.process(
            &mirror
                .process(b"\x1b[15;1Hbelow-the-fold\x1b[1;1Htop")
                .host,
        );
        assert_eq!(host.screen().contents_between(0, 0, 0, 3), "top");
        assert!(!host.screen().contents().contains("below-the-fold"));
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

    #[test]
    fn host_resize_while_viewing_repaints_without_resizing_child() {
        let mut mirror = Mirror::new(40, 120);
        let mut host = term(40, 120);
        start_view(&mut mirror, &mut host);
        host.process(&mirror.process(b"steady").host);
        // Host shrinks below the view size; child must NOT be resized.
        let t = mirror.host_resized(20, 60);
        assert_eq!(t.child_size, None);
        let mut small_host = term(20, 60);
        small_host.process(&t.host_output);
        assert_eq!(small_host.screen().contents_between(0, 0, 0, 6), "steady");
    }
}
