package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/config"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/fakebrain"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/filter"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/link"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/wa"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func newApp(t *testing.T, b *fakebrain.Brain, logs *syncBuf) *App {
	return &App{
		Cfg:      config.Config{Brain: b.URL(), Data: t.TempDir(), Mode: "self", LogLevel: "debug"},
		Key:      b.Key,
		Version:  "0.1.0-test",
		Log:      slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		download: func(context.Context, []byte) ([]byte, error) { return []byte{1, 2, 3}, nil },
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// No WhatsApp session and Pair=false: nothing touches the network except
// the brain. Forwarded messages become the right frames; logs carry ids only.
func TestRunForwardsAndNeverLogsText(t *testing.T) {
	b := fakebrain.New(t)
	logs := &syncBuf{}
	a := newApp(t, b, logs)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	if h := b.Next("hello").In; h["mode"] != "self" || h["connector"] != "0.1.0-test" {
		t.Fatalf("hello %v", h)
	}
	if st := b.Next("status").In; st["state"] != "pairing" || st["qr"] != nil {
		t.Fatalf("status %v", st)
	}
	eventually(t, "status.json", func() bool {
		s, err := ReadStatus(a.Cfg)
		return err == nil && s.WhatsApp == "pairing" && s.Brain == "connected" && s.Latest == "9.9.9"
	})

	now := time.Now()
	a.onMessage(filter.Out{ID: "T", From: "partner", SentAt: now, Text: "$ 25000 SECRET-TEXT"})
	if m := b.Next("message").In; m["text"] != "$ 25000 SECRET-TEXT" || m["from"] != "partner" || m["sentAt"] != float64(now.Unix()) {
		t.Fatalf("text frame %v", m)
	}
	img := &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}
	a.onMessage(filter.Out{ID: "I", From: "linked", SentAt: now, Text: "$ SECRET-CAPTION", Image: img})
	if m := b.Next("message").In; m["text"] != nil || m["image"].(map[string]any)["caption"] != "$ SECRET-CAPTION" || m["image"].(map[string]any)["data"] != "AQID" {
		t.Fatalf("image frame %v", m)
	}
	a.onMessage(filter.Out{ID: "B", From: "partner", SentAt: now, Text: "$ SECRET-BIG", Image: img, TooBig: true})
	if m := b.Next("message").In; m["imageTooBig"] != true || m["text"] != "$ SECRET-BIG" || m["image"] != nil {
		t.Fatalf("too-big frame %v", m)
	}

	b.Send(`{"type":"future-frame"}`)                           // ignored
	b.Send(`{"type":"pair","method":"qr"}`)                     // hosted only: ignored
	b.Send(`{"type":"reply","id":"T","text":"🤖 SECRET-REPLY"}`) // not connected to WhatsApp: logged as failed
	eventually(t, "reply attempt", func() bool { return strings.Contains(logs.String(), "reply_failed") })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	out := logs.String()
	if strings.Contains(out, "SECRET") {
		t.Fatalf("message text in logs:\n%s", out)
	}
	for _, id := range []string{`"msg_id":"T"`, `"msg_id":"I"`, `"msg_id":"B"`} {
		if !strings.Contains(out, id) {
			t.Fatalf("missing %s in logs:\n%s", id, out)
		}
	}
}

func TestRunStopsOnBadKey(t *testing.T) {
	b := fakebrain.New(t)
	a := newApp(t, b, &syncBuf{})
	a.Key = "wrong"
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, link.ErrKey) {
			t.Fatalf("Run = %v, want ErrKey", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept going after 4001")
	}
	if s, _ := ReadStatus(a.Cfg); s.Brain != link.ErrKey.Error() || s.WhatsApp != "closed" {
		t.Fatalf("status.json %+v", s)
	}
}

// An image is held in the outbox (with its media reference) before WhatsApp
// is acked and downloaded after, so stopping mid-download can't lose it: the
// next start downloads and sends it. M3: whatsmeow can't decrypt a redelivered
// message twice, so an image dropped mid-download used to be lost.
func TestImageHeldUntilDownloaded(t *testing.T) {
	b := fakebrain.New(t)
	a := newApp(t, b, &syncBuf{})
	started := make(chan []byte, 1)
	a.download = func(ctx context.Context, ref []byte) ([]byte, error) {
		started <- ref
		<-ctx.Done() // still downloading when the connector stops
		return nil, ctx.Err()
	}
	connected := func(a *App) func() bool {
		return func() bool { s, err := ReadStatus(a.Cfg); return err == nil && s.Brain == "connected" }
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	eventually(t, "connected", connected(a))
	img := &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/p")}
	a.onMessage(filter.Out{ID: "I", From: "partner", SentAt: time.Now(), Text: "$ caption", Image: img})
	ref := <-started
	b.None("message", 300*time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}

	a2 := newApp(t, b, &syncBuf{})
	a2.Cfg = a.Cfg
	a2.download = func(_ context.Context, got []byte) ([]byte, error) {
		if !bytes.Equal(got, ref) {
			t.Errorf("ref %q, want %q", got, ref)
		}
		return []byte{1, 2, 3}, nil
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = a2.Run(ctx2) }()
	eventually(t, "connected again", connected(a2))
	a2.onState(wa.State{State: "open"})
	m := b.Next("message").In
	if img, _ := m["image"].(map[string]any); m["id"] != "I" || img["data"] != "AQID" || img["caption"] != "$ caption" || img["mime"] != "image/jpeg" {
		t.Fatalf("frame after restart %v", m)
	}
	b.None("message", 300*time.Millisecond) // once
}

// A stopped connector isn't connected to WhatsApp any more: `status` must not
// keep showing "open" (M3: it did after 4004 / 4001 stopped it).
func TestStatusNotOpenAfterStop(t *testing.T) {
	b := fakebrain.New(t)
	a := newApp(t, b, &syncBuf{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	// startWhatsApp (its own goroutine) reports pairing first: wait, then "link"
	eventually(t, "pairing", func() bool { s, err := ReadStatus(a.Cfg); return err == nil && s.WhatsApp == "pairing" })
	a.onState(wa.State{State: "open"})
	cancel()
	<-done
	if s, _ := ReadStatus(a.Cfg); s.WhatsApp != "closed" {
		t.Fatalf("status.json after stop %+v", s)
	}
}

// A key revoked (4001) while the terminal shows a QR stops the connector at
// once with "key invalid" (M3: it waited for the QR to expire, then reported
// "QR code expired" instead).
func TestKeyRevokedWhilePairing(t *testing.T) {
	b := fakebrain.New(t)
	a := newApp(t, b, &syncBuf{})
	a.Pair = true
	pairing := make(chan struct{})
	a.pairQR = func(ctx context.Context) error { close(pairing); <-ctx.Done(); return ctx.Err() }
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	select {
	case <-pairing:
	case <-time.After(5 * time.Second):
		t.Fatal("pairing never started")
	}
	b.CloseWith(4001)
	select {
	case err := <-done:
		if !errors.Is(err, link.ErrKey) {
			t.Fatalf("Run = %v, want ErrKey", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept pairing after 4001")
	}
	if s, _ := ReadStatus(a.Cfg); s.Brain != link.ErrKey.Error() {
		t.Fatalf("status.json %+v", s)
	}
}

// The chat and sender a react/reply needs survive a restart for messages
// still in the outbox (M3 #7: after a kill, the resent messages got no ✅,
// "react skipped: message id not known").
func TestReactContextSurvivesRestart(t *testing.T) {
	b := fakebrain.New(t)
	b.Ack = false // stays in the outbox
	a := newApp(t, b, &syncBuf{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	eventually(t, "connected", func() bool { s, err := ReadStatus(a.Cfg); return err == nil && s.Brain == "connected" })
	chat := types.NewJID("6281111111111", types.DefaultUserServer)
	a.onMessage(filter.Out{ID: "R", From: "partner", SentAt: time.Now(), Text: "$ 1", Chat: chat, Sender: chat})
	b.Next("message")
	cancel()
	<-done

	logs := &syncBuf{}
	a2 := newApp(t, b, logs)
	a2.Cfg = a.Cfg
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = a2.Run(ctx2) }()
	b.Next("message") // resent after the restart
	b.Send(`{"type":"react","id":"R","emoji":"✅"}`)
	eventually(t, "react attempted", func() bool { return strings.Contains(logs.String(), "react_failed") }) // not connected to WhatsApp here
	if strings.Contains(logs.String(), "react_skipped") {
		t.Fatalf("react skipped after restart:\n%s", logs.String())
	}
}

// Hosted (M4): no terminal; the brain's `pair` starts linking and every new
// QR / code goes to the brain for the dashboard. A `pair` while pairing
// replaces that attempt (QR → code); one that ends unlinked says so without a QR.
func TestHostedPairing(t *testing.T) {
	b := fakebrain.New(t)
	a := newApp(t, b, &syncBuf{})
	a.Cfg.Mode = "hosted"
	var qrStopped atomic.Bool
	a.pairQR = func(ctx context.Context) error {
		a.onState(wa.State{State: "pairing", QR: "QR-1"})
		a.onState(wa.State{State: "pairing", QR: "QR-2"})
		<-ctx.Done() // replaced by the code request
		qrStopped.Store(true)
		return ctx.Err()
	}
	expire := make(chan struct{})
	phones := make(chan string, 1)
	a.pairCode = func(_ context.Context, phone string) error {
		phones <- phone
		a.onState(wa.State{State: "pairing", Code: "ABCD-EFGH"})
		<-expire
		return wa.ErrQRTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	if h := b.Next("hello").In; h["mode"] != "hosted" {
		t.Fatalf("hello %v", h)
	}
	if st := b.Next("status").In; st["state"] != "pairing" || st["qr"] != nil {
		t.Fatalf("waiting for pair: %v", st)
	}
	b.Send(`{"type":"pair","method":"qr"}`)
	for _, want := range []string{"QR-1", "QR-2"} {
		if st := b.Next("status").In; st["state"] != "pairing" || st["qr"] != want {
			t.Fatalf("status %v, want qr %s", st, want)
		}
	}
	b.Send(`{"type":"pair","method":"code","phone":"6282222222222"}`) // while the QR shows
	if p := <-phones; p != "6282222222222" || !qrStopped.Load() {
		t.Fatalf("pairing code for %q, QR attempt stopped first: %v", p, qrStopped.Load())
	}
	if st := b.Next("status").In; st["pairCode"] != "ABCD-EFGH" || st["qr"] != nil {
		t.Fatalf("code status %v", st)
	}
	close(expire)
	if st := b.Next("status").In; st["state"] != "pairing" || st["pairCode"] != nil {
		t.Fatalf("after expiry %v", st)
	}
}
