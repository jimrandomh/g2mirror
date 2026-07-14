//! The scrollback history archive: an append-only sequence of lines with
//! stable monotonic indices, retained up to a line cap. Each record is a
//! self-contained styled line — printable text plus SGR sequences only,
//! starting from default attributes — annotated with the width it was laid
//! out at and whether it soft-wraps onto the next record.

use std::collections::VecDeque;

pub const DEFAULT_MAX_LINES: usize = 10_000;
/// Cap on one line's stored bytes. A line is at most a terminal width of
/// glyphs, but styling and multi-byte characters make the byte count
/// technically unbounded, so cap it during capture.
pub const MAX_LINE_BYTES: usize = 64 * 1024;
/// Byte budget (pre-base64) for one history reply, so a fetch can't blow up
/// a websocket frame. At least one line is always returned if available.
pub const REPLY_BYTE_BUDGET: usize = 192 * 1024;
/// Lines per fetch when the client doesn't say.
pub const DEFAULT_FETCH_LIMIT: u32 = 500;

pub struct HistoryRecord {
    /// SGR-styled text, self-contained (starts from default attributes).
    pub bytes: Vec<u8>,
    /// Width (columns) the line was laid out at.
    pub width: u16,
    /// True if the line soft-wraps: it and the next record form one logical
    /// line, so a client may re-wrap them at its own width.
    pub wrapped: bool,
}

pub struct History {
    lines: VecDeque<HistoryRecord>,
    /// Index that the next appended line will get.
    next: u64,
    max_lines: usize,
}

impl History {
    pub fn new(max_lines: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            next: 0,
            max_lines,
        }
    }

    pub fn set_max_lines(&mut self, max_lines: usize) {
        self.max_lines = max_lines;
        while self.lines.len() > self.max_lines {
            self.lines.pop_front();
        }
    }

    pub fn push(&mut self, record: HistoryRecord) {
        self.lines.push_back(record);
        self.next += 1;
        while self.lines.len() > self.max_lines {
            self.lines.pop_front();
        }
    }

    /// Index one past the newest line (the next line to be appended).
    pub fn next_index(&self) -> u64 {
        self.next
    }

    /// Index of the oldest line still retained.
    pub fn oldest(&self) -> u64 {
        self.next - self.lines.len() as u64
    }

    /// The newest `count` retained lines, oldest first, with no byte budget
    /// (used for host-side scroll mirroring, where truncation would lose
    /// lines from the host terminal's scrollback).
    pub fn tail(&self, count: usize) -> impl Iterator<Item = &HistoryRecord> {
        self.lines.iter().skip(self.lines.len().saturating_sub(count))
    }

    /// Lines ending just before `before`, newest-bounded by `limit` and the
    /// reply byte budget, in oldest-to-newest order. Returns the index of
    /// the first returned line.
    pub fn fetch(&self, before: u64, limit: u32) -> (u64, Vec<&HistoryRecord>) {
        let end = before.min(self.next).max(self.oldest());
        let mut collected: Vec<&HistoryRecord> = Vec::new();
        let mut budget = REPLY_BYTE_BUDGET;
        let mut index = end;
        while index > self.oldest() && (collected.len() as u32) < limit {
            let record = &self.lines[(index - 1 - self.oldest()) as usize];
            if !collected.is_empty() && record.bytes.len() > budget {
                break;
            }
            budget = budget.saturating_sub(record.bytes.len());
            collected.push(record);
            index -= 1;
        }
        collected.reverse();
        (index, collected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(text: &str) -> HistoryRecord {
        HistoryRecord {
            bytes: text.as_bytes().to_vec(),
            width: 80,
            wrapped: false,
        }
    }

    #[test]
    fn indices_are_stable_across_eviction() {
        let mut h = History::new(3);
        for i in 0..5 {
            h.push(record(&format!("line{i}")));
        }
        assert_eq!(h.next_index(), 5);
        assert_eq!(h.oldest(), 2); // lines 0 and 1 evicted
        let (start, lines) = h.fetch(5, 10);
        assert_eq!(start, 2);
        assert_eq!(
            lines.iter().map(|r| r.bytes.clone()).collect::<Vec<_>>(),
            vec![b"line2".to_vec(), b"line3".to_vec(), b"line4".to_vec()]
        );
    }

    #[test]
    fn fetch_pages_backwards() {
        let mut h = History::new(100);
        for i in 0..10 {
            h.push(record(&format!("line{i}")));
        }
        let (start, lines) = h.fetch(10, 4);
        assert_eq!(start, 6);
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0].bytes, b"line6");
        // Next page uses the previous start as `before`.
        let (start, lines) = h.fetch(start, 4);
        assert_eq!(start, 2);
        assert_eq!(lines[0].bytes, b"line2");
        // Clamped at the oldest retained line.
        let (start, lines) = h.fetch(start, 4);
        assert_eq!(start, 0);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn fetch_respects_byte_budget_but_returns_at_least_one_line() {
        let mut h = History::new(100);
        let big = "x".repeat(REPLY_BYTE_BUDGET);
        h.push(record(&big));
        h.push(record(&big));
        let (start, lines) = h.fetch(2, 10);
        assert_eq!(lines.len(), 1, "budget limits to one huge line");
        assert_eq!(start, 1);
        let (start, lines) = h.fetch(start, 10);
        assert_eq!(lines.len(), 1);
        assert_eq!(start, 0);
    }

    #[test]
    fn fetch_beyond_extent_is_clamped() {
        let mut h = History::new(100);
        h.push(record("only"));
        let (start, lines) = h.fetch(999, 10);
        assert_eq!((start, lines.len()), (0, 1));
        let (start, lines) = h.fetch(0, 10);
        assert_eq!((start, lines.len()), (0, 0));
    }
}
