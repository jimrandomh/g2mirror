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
  --init-config`, which prints the first auth token once), cleans up stale
  session sockets, authenticates devices, lists sessions, and relays
  messages. It listens on a private address (loopback by default);
  encryption is delegated to tailscale or an ssh tunnel. It also keeps a
  monitor connection to every session and tracks each terminal's last
  bell, pushing debounced bell notifications to connected devices — useful
  for watching AI agents and other long-running programs that ring the
  terminal bell (`printf '\a'`) when they want attention.
- **`g2mirror-view`** — a terminal client for humans without glasses
  (e.g. a coworker following a shared project). `g2mirror-view
  g2mirror://<token>@<host>[:port]` shows the list of live terminals
  (arrows + enter to attach, `q` to quit); attaching prints recent
  scrollback history into your terminal's own scrollback, then mirrors the
  live viewport. **Ctrl+D** detaches back to the list; every other key is
  forwarded to the wrapped app unless the token or session is read-only.

See [PROTOCOL.md](PROTOCOL.md) for the full protocol (aimed at glasses-
driver implementers).

## Tokens, permissions, and sizing

The server config holds an `auth_tokens` array; each entry has a `name`, a
`token_hash`, and a `readonly` flag (**default true** — add tokens with
`g2mirror-server --add-token <name>`, or `--add-token <name> --writable`
for one that may send input). A viewer can send input (e.g. voice-to-text)
only if its token is writable *and* the wrapper wasn't started with
`--readonly`. Legacy configs with a single `auth_token_hash` still work
(as a writable token named `default`).

A token can also be restricted to a subset of terminals with a `filter`
array:

```json
{"name": "robert", "token_hash": "…", "readonly": true,
 "filter": [
   {"path": "/Users/jim/repositories/shared-project.*"},
   {"windowtitle": ".*SHARED.*"}
 ]}
```

A terminal is visible when **any** rule matches; within one rule every
present field must match. `path` is matched against the session's real
working directory and `windowtitle` against its current title; both are
regexes anchored at both ends. Filters govern everything: hidden terminals
are absent from `list`, refuse `connect`, and produce no bell/title
notifications — and since a title change can toggle visibility (handy as
an on/off switch: have the program or your prompt set a title containing a
marker like `SHARED`), a viewer attached to a terminal that stops matching
is disconnected on the spot. Unknown rule keys and invalid regexes are
config errors, so a typo fails at startup instead of silently widening
access.

Several viewers can view one terminal at the same time; they all receive
the same stream. Which of them sets the wrapped app's *size* is the
`size_precedence` config list — an ordered list of token names plus
`"host"` (the host terminal), e.g. `["glasses", "host", "spectator"]`:
the app is sized to the earliest listed party that is currently viewing
(`"host"` always counts as present). Everyone ranked lower gets a stream
at that size, cropped bottom-left to fit their screen — g2mirror-view
tolerates this mismatch natively, and re-synchronizes automatically when
the stream size changes. Unlisted tokens rank below everything listed;
with no list at all, any viewer resizes the app (the original behavior).

## Build & run

```sh
cargo build
./target/debug/g2mirror htop           # press Ctrl+G to toggle the simulated view
./target/debug/g2mirror --title "build watcher" -- make watch

./target/debug/g2mirror-server --init-config   # once; prints the "glasses" token
./target/debug/g2mirror-server --add-token spectator   # a read-only coworker token
./target/debug/g2mirror-server                 # ws://127.0.0.1:8737

./target/debug/g2mirror-view "g2mirror://<token>@127.0.0.1:8737"
```

## Exposing to coworkers with tailscale funnel

For viewers outside your tailnet, terminate TLS with [tailscale
funnel](https://tailscale.com/kb/1223/funnel) instead of building it into
the server. Add loopback to `listen_addr` (it takes a string or an array):

```json
"listen_addr": ["127.0.0.1", "100.68.94.67"]
```

then `tailscale funnel --bg 8737`. Direct tailnet clients (the glasses)
keep using the tailscale address; funnel proxies public
`wss://<node>.<tailnet>.ts.net` traffic — websocket upgrades pass through
its HTTPS proxy — to the loopback listener. A coworker then needs nothing
installed beyond g2mirror-view:

```sh
g2mirror-view "g2mirrors://<token>@<node>.<tailnet>.ts.net"
```

(`g2mirrors://` is the TLS form, default port 443.) Treat the funnel
hostname as public knowledge — TLS certificates land in Certificate
Transparency logs — so security rests on the tokens: keep coworker tokens
read-only and filtered. The server hardens the public surface by capping
concurrent unauthenticated connections (32), enforcing a 10s
handshake+auth deadline, logging failed authentications with the peer
address, and sending websocket keepalive pings every 30s so idle
connections survive the proxy path. If funnel's HTTP proxy ever misbehaves
for websockets, its TLS-terminated-TCP mode forwards the raw byte stream
and works identically.

`--title` sets the initial window title (shown in session lists and pushed
to devices) for programs that never set one themselves; a program-set title
takes over from there. Lines that scroll off screen (including before any
device connects) are archived — 10,000 lines by default, `--scrollback`
to change — and devices fetch them lazily in pages.

While a device is viewing, the host terminal shows the live view anchored
at the bottom of the screen, with scrolled lines flowing up above it and
into the host terminal's own native scrollback in real time — so
scrolling up in your terminal works during and after a view, and
detaching preserves everything that scrolled. The wrapped command's exit status is propagated, and
a client watching when the program quits receives an `exit` message carrying
that status. `cargo test` runs unit
tests plus end-to-end tests of the session socket, the websocket server,
and the full device→server→wrapper chain.

## Layout

- `src/main.rs` — wrapper: pty + child spawn, raw mode, event loop
- `src/control.rs` — session socket listener/client framing
- `src/lib.rs` etc. — shared library: `mirror` (view state machine and
  vt100-based output translation, also used by the viewer for its local
  rendering), `history` (scrollback archive), `protocol` (message types),
  `paths` (~/.g2mirror handling), `raw_guard` (RAII raw-mode guard)
- `src/bin/g2mirror-server.rs` — websocket gateway
- `src/bin/g2mirror-view.rs` — terminal viewer client
