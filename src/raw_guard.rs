//! RAII guard that puts the host terminal into raw mode and restores the
//! saved settings on drop.

use rustix::termios::{self, OptionalActions, Termios};

pub struct RawGuard {
    saved: Termios,
}

impl RawGuard {
    pub fn new() -> rustix::io::Result<Self> {
        let stdin = rustix::stdio::stdin();
        let saved = termios::tcgetattr(stdin)?;
        let mut raw = saved.clone();
        raw.make_raw();
        termios::tcsetattr(stdin, OptionalActions::Flush, &raw)?;
        Ok(Self { saved })
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        let _ = termios::tcsetattr(rustix::stdio::stdin(), OptionalActions::Flush, &self.saved);
    }
}
