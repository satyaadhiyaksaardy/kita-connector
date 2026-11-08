package link

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/fakebrain"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/outbox"
)

func newClient(t *testing.T, b *fakebrain.Brain) (*Client, chan In) {
	t.Helper()
	ob, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ob.Close() })
	in := make(chan In, 16)
	return &Client{
		URL: b.URL(), Key: b.Key, Mode: "self", Version: "0.1.0-test",
		Outbox: ob, Log: slog.New(slog.DiscardHandler),
		Handle: func(f In) { in <- f },
	}, in
}

// run starts c.Run; the returned func waits for it to return.
func run(t *testing.T, c *Client) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var err error
	go func() { err = c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return func() error {
		t.Helper()
		select {
		case <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return")
			return nil
		}
	}
}

func recv(t *testing.T, in chan In, typ string) In {
	t.Helper()
	select {
	case f := <-in:
		if f.Type != typ {
			t.Fatalf("handled %q, want %q", f.Type, typ)
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatalf("no %q frame handled", typ)
		return In{}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func queued(c *Client) int { n, _ := c.Outbox.Len(); return n }

func timings(t *testing.T, ping, tk, bmin, bmax time.Duration) {
	p, k, a, z := pingEvery, tick, backoffMin, backoffMax
	pingEvery, tick, backoffMin, backoffMax = ping, tk, bmin, bmax
	t.Cleanup(func() { pingEvery, tick, backoffMin, backoffMax = p, k, a, z })
}

func TestHelloConfigStatusAck(t *testing.T) {
	timings(t, 20*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, 100*time.Millisecond) // ping often: must not stall reads
	b := fakebrain.New(t)
	c, in := newClient(t, b)
	// pending from a previous run, and a known WhatsApp state
	pending, _ := MessageFrame(Message{ID: "P", From: "partner", SentAt: 1, Text: "$1"})
	if err := c.Outbox.Put("P", pending, "", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	c.SetStatus(context.Background(), Status{State: "open", Self: &Self{Phone: "6282222222222"}})
	run(t, c)

	hello := b.Next("hello").In
	if hello["version"] != "1" || hello["mode"] != "self" || hello["connector"] != "0.1.0-test" {
		t.Fatalf("hello = %v", hello)
	}
	cfg := recv(t, in, "config")
	if cfg.PartnerPhone != "6281111111111" || cfg.Prefix != "$" || cfg.LatestConnector != "9.9.9" {
		t.Fatalf("config = %+v", cfg)
	}
	st := b.Next("status").In
	if st["state"] != "open" || st["self"].(map[string]any)["phone"] != "6282222222222" || st["qr"] != nil {
		t.Fatalf("status = %v", st)
	}
	if id := b.Next("message").In["id"]; id != "P" {
		t.Fatalf("pending not resent after connect, got %v", id)
	}
	eventually(t, "P acked", func() bool { return queued(c) == 0 })

	if err := c.SendMessage(context.Background(), Message{ID: "IMG", From: "linked", SentAt: 2,
		Image: &Image{Mime: "image/jpeg", Data: []byte{1, 2, 3}, Caption: "$ struk"}}); err != nil {
		t.Fatal(err)
	}
	img := b.Next("message").In["image"].(map[string]any)
	if img["data"] != "AQID" || img["mime"] != "image/jpeg" || img["caption"] != "$ struk" {
		t.Fatalf("image = %v", img)
	}
	eventually(t, "IMG acked", func() bool { return queued(c) == 0 })

	// 4.8 MB passes the 5 MB image limit but not the 6 MB frame limit once base64'd
	if err := c.SendMessage(context.Background(), Message{ID: "BIG", From: "partner", SentAt: 3,
		Image: &Image{Mime: "image/jpeg", Data: make([]byte, 4_800_000), Caption: "$ struk besar"}}); err != nil {
		t.Fatal(err)
	}
	if big := b.Next("message"); big.In["imageTooBig"] != true || big.In["text"] != "$ struk besar" || big.In["image"] != nil {
		t.Fatalf("oversized frame not downgraded: %.200s", big.Raw)
	}

	b.Send(`{"type":"something-new","x":1}`)
	recv(t, in, "something-new") // passed through; the app ignores it
	b.Send(`{"type":"reply","id":"IMG","text":"🤖 ok"}`)
	if r := recv(t, in, "reply"); r.ID != "IMG" || r.Text != "🤖 ok" {
		t.Fatalf("reply = %+v", r)
	}
	b.Send(`{"type":"react","id":"IMG","emoji":"✅"}`)
	if r := recv(t, in, "react"); r.Emoji != "✅" {
		t.Fatalf("react = %+v", r)
	}
	time.Sleep(100 * time.Millisecond) // several pings
	if len(b.Conns()) != 1 {
		t.Fatalf("connections = %d, want 1", len(b.Conns()))
	}
}

func TestResendTimer(t *testing.T) {
	timings(t, time.Minute, 10*time.Millisecond, time.Second, time.Second)
	b := fakebrain.New(t)
	b.Ack = false
	c, in := newClient(t, b)
	c.Outbox.Base, c.Outbox.Max = 100*time.Millisecond, 200*time.Millisecond
	run(t, c)
	recv(t, in, "config")

	if err := c.SendMessage(context.Background(), Message{ID: "M", From: "partner", SentAt: 1, Text: "$1"}); err != nil {
		t.Fatal(err)
	}
	b.Next("message")
	t1 := time.Now()
	b.Next("message")
	if d := time.Since(t1); d < 80*time.Millisecond {
		t.Fatalf("first resend after %v, want ~100ms", d)
	}
	t2 := time.Now()
	b.Next("message")
	if d := time.Since(t2); d < 180*time.Millisecond {
		t.Fatalf("second resend after %v, want ~200ms (doubled)", d)
	}
	b.Send(`{"type":"received","id":"M"}`)
	eventually(t, "M acked", func() bool { return queued(c) == 0 })
	b.None("message", 500*time.Millisecond)
}

func TestReconnectResendsPendingAndStatus(t *testing.T) {
	timings(t, time.Minute, 10*time.Millisecond, 20*time.Millisecond, 100*time.Millisecond)
	b := fakebrain.New(t)
	b.Ack = false
	c, in := newClient(t, b)
	c.Outbox.Base = time.Hour // only the reconnect path may resend
	run(t, c)
	recv(t, in, "config")
	c.SetStatus(context.Background(), Status{State: "closed"})
	b.Next("status")
	if err := c.SendMessage(context.Background(), Message{ID: "M", From: "partner", SentAt: 1, Text: "$1"}); err != nil {
		t.Fatal(err)
	}
	b.Next("message")

	b.CloseWith(websocket.StatusGoingAway)
	recv(t, in, "config")
	if g := b.Next("status"); g.Conn != 1 || g.In["state"] != "closed" {
		t.Fatalf("status after reconnect: %+v", g)
	}
	if g := b.Next("message"); g.Conn != 1 || g.In["id"] != "M" {
		t.Fatalf("message after reconnect: %+v", g)
	}
}

func TestReconnectBackoff(t *testing.T) {
	timings(t, time.Minute, 10*time.Millisecond, 40*time.Millisecond, 160*time.Millisecond)
	b := fakebrain.New(t)
	b.Reject = 5
	c, in := newClient(t, b)
	run(t, c)
	recv(t, in, "config")

	conns := b.Conns()
	for i, want := range []time.Duration{40, 80, 160, 160, 160} {
		want *= time.Millisecond
		if d := conns[i+1].Sub(conns[i]); d < want-5*time.Millisecond || d > 3*want+100*time.Millisecond {
			t.Errorf("retry %d after %v, want ~%v", i+1, d, want)
		}
	}

	// a connection that got config resets the backoff
	t0 := time.Now()
	b.CloseWith(websocket.StatusInternalError)
	recv(t, in, "config")
	if d := time.Since(t0); d > 150*time.Millisecond {
		t.Fatalf("reconnect after a good session took %v, want ~40ms", d)
	}
}

func TestCloseCodesStop(t *testing.T) {
	timings(t, time.Minute, 10*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond)
	cases := []struct {
		name  string
		setup func(b *fakebrain.Brain, c *Client)
		want  error
	}{
		{"4001 bad key", func(b *fakebrain.Brain, c *Client) { c.Key = "wrong" }, ErrKey},
		{"4003 unsupported version", func(b *fakebrain.Brain, c *Client) { b.Version = "2" }, ErrVersion},
		{"4004 replaced", nil, ErrReplaced},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := fakebrain.New(t)
			c, in := newClient(t, b)
			if tc.setup != nil {
				tc.setup(b, c)
			}
			wait := run(t, c)
			conns := 1
			if tc.want == ErrReplaced {
				recv(t, in, "config")
				c2, in2 := newClient(t, b) // another copy with the same key
				run(t, c2)
				recv(t, in2, "config")
				conns = 2
			}
			if err := wait(); !errors.Is(err, tc.want) {
				t.Fatalf("Run = %v, want %v", err, tc.want)
			}
			time.Sleep(100 * time.Millisecond) // > several backoff periods
			if n := len(b.Conns()); n != conns {
				t.Fatalf("connections = %d, want %d (no retry)", n, conns)
			}
		})
	}
}
