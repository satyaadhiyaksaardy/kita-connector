# Connector ↔ brain protocol, v1 (draft)

The connector links one WhatsApp account and talks to the brain over a single
WebSocket. The same protocol is used whether the connector runs on the brain's
server (hosted) or on the couple's own machine (self-hosted).

## Connection

- The connector dials **out**: `wss://<brain-host>/connector`. Hosted
  connectors (one process per couple, run by the brain's operator) dial the
  brain over loopback, `ws://127.0.0.1:<port>/connector` — same frames, same
  key auth.
- Auth header: `Authorization: Bearer <connector-key>` (one key per couple).
- One JSON object per text frame, always with a `"type"` field. Frames over
  6 MB are rejected. An image is forwarded only if its `message` frame,
  base64 data included, fits in 6 MB (about 4.5 MB of image); otherwise it
  is sent as `imageTooBig`.
- WebSocket ping every 30 s. On disconnect, reconnect with backoff
  (1 s doubling, max 60 s), forever — except after close codes `4001`/`4003`,
  which need a human (new key / upgrade).
- First frame from the connector is `hello`. The brain answers with `config`.
- One live connection per key: a new one replaces the old (closed with `4004`).
- Unknown frame types are ignored by both sides (forward compatibility).

### Close codes

| code | meaning | connector does |
|---|---|---|
| `4001` | bad or revoked key | stop, show "key invalid" |
| `4003` | unsupported `hello.version` | stop, show "please update" |
| `4004` | replaced by a newer connection with the same key | stop (another copy is running) |
| other | network/server | reconnect with backoff |

## Connector → brain

| type | fields | when |
|---|---|---|
| `hello` | `version: "1"`, `mode: "hosted"\|"self"`, `connector: "<semver>"` | first frame |
| `status` | `state: "pairing"\|"open"\|"closed"\|"logged_out"`, `qr?`, `pairCode?`, `self?: {phone}` | on every WhatsApp state change; hosted: also on each new QR, and `pairing` without `qr`/`pairCode` when a pairing attempt ended unlinked (QR expired) |
| `message` | `id`, `from: "linked"\|"partner"`, `sentAt` (unix s), `text?`, `image?: {mime, data (base64), caption?}`, `imageTooBig?: true` | a qualifying message (see Filtering); an image whose frame would exceed 6 MB with the base64 data (about 4.5 MB of image) is sent as `imageTooBig` with its caption in `text` and no `image`, so the brain can reply |

- `qr` / `pairCode` are sent **only in hosted mode**, so the brain can show them
  on the couple's dashboard. A self-hosted connector shows them locally only.
- `message.id` is the WhatsApp message ID. The connector sends each message
  **at least once**: it keeps it in a local queue until the brain answers
  `received` with that `id`.
- **Resending:** an un-`received` message is sent again after every reconnect
  **and** on a timer while connected — after 60 s, then doubling to at most
  30 min. After 24 h without `received` the connector drops it and logs the id
  locally. (Without the timer, a temporary brain failure on a stable
  connection would leave a message stuck until the next reconnect.)

## Brain → connector

| type | fields | effect |
|---|---|---|
| `config` | `partnerPhone`, `prefix: "$"`, `latestConnector?` | who the couple chat is with; what qualifies; newest release (for "update available") |
| `received` | `id` | drop `id` from the local queue |
| `react` | `id`, `emoji` | react to message `id` in the couple chat |
| `reply` | `id?`, `text` | send `text` in the couple chat (quoting `id` if given) |
| `pair` | `method: "qr"\|"code"`, `phone?` | start linking; for `code`, `phone` (digits, with country code) is the number of the WhatsApp to link. Hosted only — a self-hosted connector with no session starts a terminal QR by itself. Ignored while linked; one sent while pairing replaces that attempt (e.g. QR → code) |
| `logout` | — | unlink from WhatsApp and delete the local session |

## Filtering (the privacy guarantee)

The connector forwards a message **only if all** hold:

1. it is in the 1:1 chat between the linked account and `config.partnerPhone`
   (the connector resolves the partner's phone ↔ LID itself);
2. its text, or an image's caption, starts with `config.prefix`; leading
   whitespace before the prefix is allowed.

`text` (and `image.caption`) is forwarded unchanged, leading whitespace
included; the brain trims it.

Everything else (other chats, groups, statuses, calls) is dropped on arrival:
not forwarded, not stored, not logged. The connector never sends a message
anywhere except in reply to a brain `reply`/`react` inside the couple chat.

## Security model (why this can be open source)

- **The key is the identity.** The connector doesn't know which couple it
  serves; the brain hashes the presented key and looks up the couple. Keys are
  32 random bytes (base64url), shown to the couple once, stored only as sha256,
  one live key per couple, revocable by the couple ("Buat kunci baru") or the
  admin.
- **One endpoint, one capability.** A valid key lets a connector act for
  exactly one couple: send that couple's `$` messages and status, receive its
  `config`, `reply`, `react`, `pair`, `logout`. **No frame reads data from the
  brain** — transactions, budgets and reports never go to a connector.
- **The brain enforces, the connector is untrusted.** Frame shape and size
  (≤ 6 MB) are validated; per-key limits: 60 `message` frames per minute and
  10 connections per minute; unknown types ignored; the couple comes only from
  the key, never from frame content.
- **Publishing this code reveals no secret.** It contains no keys or brain
  secrets; the brain's code and data stay private, and its public surface (the
  website, `/health`, `/connector`) is the same with or without this repo.
- **A modified connector only affects its owner's own WhatsApp and own couple.**
- **If a key leaks**, its holder could add fake entries to that couple's sheet,
  keep replacing their real connector (`4004`), and learn `partnerPhone` — but
  read nothing. The brain logs every replacement with the source IP and warns
  the couple on their dashboard; the fix is a new key.
- **Releases** are built by CI from a tagged commit with published checksums,
  so anyone can rebuild and compare.

## Brain duties (for reference)

- Answer `received` once the brain has taken responsibility: row written, known
  duplicate, or rejected for good with a reply (unparseable, paused couple).
  A temporary failure (rate limit, network) gets no `received` — the brain
  reacts `⏳` once — so the connector's resend timer retries it; that is what
  makes the outbox safe. A sheet that can't be opened is **not** temporary: the
  brain replies ❌ and answers `received` (rejected).
- A resend of a message still being processed (dedup row `pending`) is
  ignored without `received`; one already finished gets `received` again.
- Dedup by (couple, `message.id`) before parsing — replays are expected.
- Reject unpaid/paused couples by replying once with a notice, logging nothing.
- `from` maps to tenant members: `linked` = the partner whose WhatsApp is linked.
