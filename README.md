# g2mirror

Mirror terminal programs to Even Realities G2 smart glasses.

`g2mirror <command> [args...]` runs the command in a pseudoterminal. While
the glasses are connected and viewing the terminal, the wrapped app is
resized (via SIGWINCH) to the glasses' screen dimensions, and its output is
parsed into an in-memory [vt100](https://crates.io/crates/vt100) screen
model and re-emitted so it renders correctly on both the host terminal and
the glasses, despite the dimension mismatch — the same approach tmux uses.
If the host terminal is smaller than the glasses screen, the host view is
truncated.

## Components

- **`g2mirror`** — the command wrapper. Exposes a session socket at
  `~/.g2mirror/<pid>-<cwd>` speaking newline-delimited JSON; a client can
  ask to `view` (resize the app to the device size, get a snapshot, then a
  live output stream) and `unview`. **Ctrl+G** simulates a glasses
  connect/disconnect at 96×24 without a real client.
- **`g2mirror-server`** — a websocket gateway for device drivers. Reads
  `~/.g2mirror/config.json` (create it with `g2mirror-server
  --init-config`, which prints the auth token once), cleans up stale
  session sockets, authenticates devices, lists sessions, and relays
  messages. It listens on a private address (loopback by default);
  encryption is delegated to tailscale or an ssh tunnel. It also keeps a
  monitor connection to every session and tracks each terminal's last
  bell, pushing debounced bell notifications to connected devices — useful
  for watching AI agents and other long-running programs that ring the
  terminal bell (`printf '\a'`) when they want attention.

See [PROTOCOL.md](PROTOCOL.md) for the full protocol (aimed at glasses-
driver implementers). Devices can send input (e.g. voice-to-text) unless
the wrapper was started with `--readonly` or the server config sets
`"readonly": true`.

## Build & run

```sh
cargo build
./target/debug/g2mirror htop           # press Ctrl+G to toggle the simulated view
./target/debug/g2mirror --title "build watcher" -- make watch

./target/debug/g2mirror-server --init-config   # once; prints the auth token
./target/debug/g2mirror-server                 # ws://127.0.0.1:8737
```

`--title` sets the initial window title (shown in session lists and pushed
to devices) for programs that never set one themselves; a program-set title
takes over from there. The wrapped command's exit status is propagated, and
a client watching when the program quits receives an `exit` message carrying
that status. `cargo test` runs unit
tests plus end-to-end tests of the session socket, the websocket server,
and the full device→server→wrapper chain.

## Layout

- `src/main.rs` — wrapper: pty + child spawn, raw mode, event loop
- `src/mirror.rs` — view state machine and vt100-based output translation
- `src/control.rs` — session socket listener/client framing
- `src/raw_guard.rs` — RAII raw-mode guard for the host terminal
- `src/protocol.rs`, `src/paths.rs` — shared library (message types,
  ~/.g2mirror handling)
- `src/bin/g2mirror-server.rs` — websocket gateway
