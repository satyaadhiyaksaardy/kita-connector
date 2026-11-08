# kita-connector — design

Go program that links one WhatsApp account as a companion device and talks
to the kita-finance brain over [PROTOCOL.md](PROTOCOL.md). One binary, two
modes: **hosted** (on the brain's server, one process per couple) and **self**
(on the couple's own always-on machine). Built on
[whatsmeow](https://github.com/tulir/whatsmeow).

The code is open source so anyone can check the privacy promise: **only `$`
messages from one 1:1 chat ever leave this program.**

## Packages

```
cmd/kita-connector/   CLI: run · login · status · logout · install-service · uninstall-service · version
internal/config/      flags, env, per-OS defaults, key file
internal/wa/          whatsmeow wrapper: connect, pair (QR / code), events → domain, send text/reaction, download media
internal/filter/      Decide(msg, cfg) → forward or drop. Pure function, table-tested
internal/outbox/      SQLite queue of forwarded messages until `received`
internal/link/        WebSocket client: hello/config, frames, ping, reconnect, resend timer
internal/app/         wires wa ↔ filter ↔ outbox ↔ link; the state machine below
internal/service/     install as an OS service
```

Dependencies (pin exact versions; all MIT/BSD/MPL-2.0, compatible with the
connector's own license):

- `go.mau.fi/whatsmeow` — WhatsApp.
- A SQLite driver for whatsmeow's session store and the outbox. Prefer a
  pure-Go driver (e.g. `modernc.org/sqlite`) so cross-compiles need no CGO;
  verify whatsmeow's `sqlstore` accepts it, else build per-OS with CGO.
- A WebSocket client (e.g. `github.com/coder/websocket`).
- `github.com/mdp/qrterminal` — terminal QR.
- `github.com/kardianos/service` — Windows service / launchd / systemd.

## Configuration

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--brain` | `KITA_BRAIN` | `wss://<app domain>/connector` (baked in at build) | `ws://` refused unless the host is loopback |
| `--key` | `KITA_KEY` | read from `<data>/key` | self mode asks once and saves it (mode 0600) |
| `--data` | `KITA_DATA` | Windows `%APPDATA%\kita-connector` · macOS `~/Library/Application Support/kita-connector` · Linux `~/.local/share/kita-connector` | hosted: set by the operator, one per couple |
| `--mode` | `KITA_MODE` | `self` | `hosted` sends QR / pairing code to the brain instead of showing them |
| `--log-level` | `KITA_LOG` | `info` | |

Data directory (mode 0700): `session.db` (whatsmeow store), `outbox.db`,
`key`, `connector.log` (rotated, 3 × 5 MB).

## Commands (self mode)

- **First run** (double-click or `kita-connector`): asks for the key from the
  dashboard → shows the WhatsApp QR in the terminal (or
  `login --code +62…` for a pairing code) → once linked, offers to
  **install as a service** so it starts with the computer. This step is
  required for the "computer stays on" promise; a connector that dies with its
  terminal window is useless.
- `run` — what the service runs. `status` — linked? connected to the brain?
  queue length? `logout` — unlink from WhatsApp and wipe the data directory.
  `version`.

## State machine

```
            no session                 QR scanned / code entered
   start ──────────────► PAIRING ─────────────────────────────► OPEN
     │                     ▲  self: terminal QR right away          │  ▲
     │ has session         │  hosted: wait for brain `pair`         │  │ whatsmeow reconnects
     └─────────────────────┼────────────────────────────────────► CLOSED
                           │                                        │
                 LoggedOut event (phone removed the device,        │
                 or offline ~14 days) / brain `logout` frame ──► LOGGED_OUT
                           │   delete session.db, keep running      │
                           └────────────────────────────────────────┘
```

Every change sends `status{state, …}`. On `LoggedOut` the connector stays up
and returns to PAIRING (hosted waits for `pair`; self prints how to re-link).

## The filter (privacy promise)

`Decide` gets the unwrapped message plus `config.partnerPhone` and `prefix`.

| Case | Forward? |
|---|---|
| 1:1 chat with the partner, text starts with `$` (after trimming leading spaces), sent by the partner | ✅ `from: partner` |
| same, sent by the linked account from the phone | ✅ `from: linked` |
| partner seen by LID instead of phone number | ✅ (resolved with whatsmeow's LID ↔ phone mapping) |
| disappearing-message wrapper around a `$` text | ✅ (unwrapped) |
| reply that quotes another message, own text starts with `$` | ✅ (text only, quote ignored) |
| image with caption starting `$` | ✅ with `image` if the frame with base64 data fits in 6 MB (≈ 4.5 MB of image), else `imageTooBig: true` with the caption as `text` |
| `$` alone | ✅ (the brain answers with help / a parse error) |
| image without `$` caption · video · document · voice note · sticker | ❌ |
| view-once media | ❌ (the sender meant it to vanish) |
| edited message · deleted message · reaction | ❌ (v1: fix mistakes in the sheet) |
| any group, including one with the partner | ❌ |
| any other 1:1 chat · status updates · channels · broadcast lists · calls | ❌ |
| text starting `🤖` / sent by this connector (the bot's own replies) | ❌ (loop guard) |
| history-sync message older than 14 days | ❌ |

Dropped messages are never stored, forwarded or logged — not even their ids.

## Outbox

```sql
CREATE TABLE outbox (
  msg_id      TEXT PRIMARY KEY,
  frame       BLOB NOT NULL,      -- the full `message` frame, image included
  created_at  INTEGER NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 0,
  next_at     INTEGER NOT NULL,   -- next resend time
  media       BLOB,               -- image still to download: its media reference; never sent while set
  chat        TEXT,               -- where the brain's react/reply go (JIDs), never sent
  sender      TEXT
);
```

- `put` before sending; `received` → delete; resend on reconnect and when
  `next_at` passes (60 s, doubling to 30 min); older than 24 h → delete and log
  the id. (PROTOCOL.md → Resending.)
- A message must be in the outbox before whatsmeow acks it to WhatsApp: a
  redelivery can't be decrypted a second time, so a crash in between loses it.
  For text that window is a few milliseconds. An image is not downloaded in
  that window: its row is **held** with only the media reference (direct path,
  media key, hashes — no caption, thumbnail or quoted message), then
  downloaded, completed and sent; held rows left by a crash are downloaded at
  the next OPEN.
- In hosted mode a receipt photo sits here on our server only until the brain
  acks it (normally seconds).

## Replies and reactions

The brain sends `reply` / `react`; the connector posts them into the couple
chat with whatsmeow (`reply` quotes the original when `id` is given). Sends are
serialised and rate-limited locally (≤ 1 per second) as a second guard against
ban-like bursts. The chat and sender a react/reply needs are kept in memory
for the last 1000 forwarded ids and in the outbox row, so messages still
queued at a restart get their ✅ and quoted reply too.

## Catch-up after re-linking

On a fresh link WhatsApp sends a partial history sync. The connector passes
history messages through the same filter (≤ 14 days old) and forwards them;
the brain's dedup drops any already logged. Best-effort by design — the UI
says so.

## Logging

JSON lines: `ts, level, event, msg_id, state, attempts`. Never message text,
captions, names, or full phone numbers (last 4 digits only). Self mode writes
`connector.log` in the data directory; hosted mode writes to stdout.

## Versions and updates

- `hello` carries the protocol version (`"1"`) and the connector's semver.
  The brain closes with `4003` if the protocol is unsupported; the connector
  stops and says "please update".
- `config.latestConnector` tells the connector the newest release; self mode
  logs and shows it in `status`; the couple's dashboard shows "pembaruan
  tersedia". No auto-update in v1 (an unsigned self-updater is a security
  hole).
- Hosted: the operator replaces the binary and restarts each connector.

## Hosted mode (run by the brain's operator)

One process per couple, `kita-connector run --mode hosted --data <dir>
--brain ws://127.0.0.1:<port>/connector`, kept running by a process manager
independently of the brain, so a brain restart doesn't drop the WhatsApp
link. The key is read from `<data>/key` (owner-only) rather than the
environment, which process managers tend to print and persist.

No terminal: logs go to stdout, and the connector never pairs by itself.
The dashboard sends `pair` (`qr`, or `code` with the phone to link); every new
QR or pairing code goes to the brain as `status`, and an attempt that expires
reports `pairing` without one. A `pair` while linked is ignored; one while
pairing replaces that attempt (QR → code when the dashboard is open on the bot
phone itself).
`login`, `install-service` and the first-run flow refuse `--mode hosted`.

## Security

- Key file and data directory owner-only. TLS with system roots for `wss://`.
- The only network peers are WhatsApp and the configured brain.
- `logout` (frame or command) deletes the session, outbox and key.

## Build and release

GoReleaser → GitHub Releases: `windows/amd64`, `darwin/amd64`,
`darwin/arm64`, `linux/amd64`, `linux/arm64`, plus checksums.
Unsigned in v1: Windows SmartScreen and macOS Gatekeeper will warn; the
self-host guide must show how to allow it. Code signing is a later step.

`.goreleaser.yaml`: raw binaries (no archives) named
`kita-connector_<os>_<arch>[.exe]` with no version, so
`releases/latest/download/<name>` is a stable link for the dashboard;
`KITA_BRAIN_URL` is baked in as the default brain and a build without it
fails. `.github/workflows/release.yml`: a `v*` tag → `go test` → GoReleaser
→ a **draft** release, published by hand after a check (the brain's
`KITA_CONNECTOR_LATEST` is bumped with it). Local dry run:
`KITA_BRAIN_URL=… goreleaser release --snapshot --clean` (needs `go` on PATH).

## Tests

- `filter`: one table row per case above, incl. LID vs phone and the loop guard.
- `outbox`: put / ack / due / backoff / 24 h prune; survives a restart.
- `link`: against a fake brain — hello → config, ack flow, resend timer,
  reconnect backoff, close codes `4001` / `4003` / `4004` stop instead of retrying.
- `config`: per-OS defaults, refusing `ws://` to non-loopback.
- End-to-end (M3 done-when): two spare SIMs; `$` text, receipt, other chats
  never reach the fake brain's frame log; kill either side mid-stream → no loss,
  no duplicates; measure memory per connector.
