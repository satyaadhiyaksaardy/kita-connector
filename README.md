# kita-connector

Links one WhatsApp account as a companion device and forwards **only** the
`$`-prefixed messages from one 1:1 chat (a couple's chat) to a kita-finance
brain. Everything else is dropped on arrival. See [PROTOCOL.md](PROTOCOL.md).

Runs hosted (on the brain's server) or self-hosted (on your own always-on
machine) — the same binary either way.

Status: self mode implemented (Go + [whatsmeow](https://github.com/tulir/whatsmeow),
pure-Go SQLite, no CGO); hosted mode is run by the brain's operator, one
process per couple. See [DESIGN.md](DESIGN.md).

```sh
go test ./...
CGO_ENABLED=0 go build -ldflags "-X main.version=0.1.0 \
  -X github.com/satyaadhiyaksaardy/kita-connector/internal/config.DefaultBrain=wss://<app domain>/connector" \
  ./cmd/kita-connector
```

Releases: push a `v*` tag; CI runs the tests and GoReleaser and leaves a
draft release with the five binaries and `checksums.txt` (DESIGN.md → Build
and release).

Run `kita-connector` (first run: key → WhatsApp QR → install as a service),
or `kita-connector help`.

> Unofficial WhatsApp client. Using it may violate WhatsApp's Terms of Service
> and the linked number can be restricted or banned.

## License

Apache License 2.0, see [LICENSE](LICENSE). Copyright 2026 the kita-connector
authors.

The binaries also contain third-party modules under their own licenses (see
`go.mod`), notably [whatsmeow](https://github.com/tulir/whatsmeow) under the
Mozilla Public License 2.0, whose source is available there.
