// Package fakebrain is a brain that speaks PROTOCOL.md, for tests: Bearer
// key auth, hello → config, one live connection per key (the older one is
// closed 4004), frame-shape checks, optional `received`, and a frame log.
package fakebrain

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const maxFrame = 6 << 20

type Brain struct {
	Key     string
	Version string // supported hello.version
	Ack     bool   // answer `received` to every message
	Reject  int    // close the first n connections (1011) before hello

	t testing.TB

	srv    *httptest.Server
	mu     sync.Mutex
	cur    *websocket.Conn
	conns  []time.Time
	frames chan Frame
}

// Frame is one frame the brain received, on connection number Conn.
type Frame struct {
	Conn int
	In   map[string]any
	Raw  []byte
}

// New starts a brain; it accepts Key, and stops when the test ends.
func New(t testing.TB) *Brain {
	b := &Brain{t: t, Key: "k3y", Version: "1", Ack: true, frames: make(chan Frame, 256)}
	b.srv = httptest.NewServer(http.HandlerFunc(b.serve))
	t.Cleanup(b.srv.Close)
	return b
}

// URL is the ws:// URL of the /connector endpoint.
func (b *Brain) URL() string { return "ws" + strings.TrimPrefix(b.srv.URL, "http") + "/connector" }

func (b *Brain) serve(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(maxFrame)
	b.mu.Lock()
	n := len(b.conns)
	b.conns = append(b.conns, time.Now())
	if n < b.Reject {
		b.mu.Unlock()
		c.Close(websocket.StatusInternalError, "not now")
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+b.Key {
		b.mu.Unlock()
		c.Close(4001, "bad key")
		return
	}
	if b.cur != nil {
		b.cur.Close(4004, "replaced")
	}
	b.cur = c
	b.mu.Unlock()

	ctx := context.Background()
	first := true
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var in map[string]any
		if err := json.Unmarshal(data, &in); err != nil {
			b.t.Errorf("brain: not JSON: %v", err)
			return
		}
		b.frames <- Frame{n, in, data}
		if first {
			first = false
			if in["type"] != "hello" {
				b.t.Errorf("brain: first frame %v, want hello", in["type"])
			}
			if in["version"] != b.Version {
				c.Close(4003, "update")
				return
			}
			b.write(c, `{"type":"config","partnerPhone":"6281111111111","prefix":"$","latestConnector":"9.9.9"}`)
			continue
		}
		if in["type"] == "message" {
			b.checkMessage(in)
			if b.Ack {
				b.write(c, `{"type":"received","id":"`+in["id"].(string)+`"}`)
			}
		}
	}
}

// checkMessage validates a message frame's shape like the brain does.
func (b *Brain) checkMessage(in map[string]any) {
	id, _ := in["id"].(string)
	from, _ := in["from"].(string)
	sentAt, _ := in["sentAt"].(float64)
	if id == "" || (from != "linked" && from != "partner") || sentAt <= 0 {
		b.t.Errorf("brain: bad message frame %v", in)
	}
	if in["text"] == nil && in["image"] == nil && in["imageTooBig"] == nil {
		b.t.Errorf("brain: message without text/image: %v", in)
	}
}

func (b *Brain) write(c *websocket.Conn, frame string) {
	_ = c.Write(context.Background(), websocket.MessageText, []byte(frame))
}

// Send writes a frame on the live connection.
func (b *Brain) Send(frame string) {
	b.mu.Lock()
	c := b.cur
	b.mu.Unlock()
	b.write(c, frame)
}

// CloseWith closes the live connection with code.
func (b *Brain) CloseWith(code websocket.StatusCode) {
	b.mu.Lock()
	c := b.cur
	b.mu.Unlock()
	c.Close(code, "test")
}

// Conns lists when each connection arrived.
func (b *Brain) Conns() []time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]time.Time(nil), b.conns...)
}

// Next waits for the next frame of type typ, skipping others.
func (b *Brain) Next(typ string) Frame {
	b.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case g := <-b.frames:
			if g.In["type"] == typ {
				return g
			}
		case <-deadline:
			b.t.Fatalf("brain: no %q frame", typ)
		}
	}
}

// None asserts no frame of type typ arrives within d.
func (b *Brain) None(typ string, d time.Duration) {
	b.t.Helper()
	deadline := time.After(d)
	for {
		select {
		case g := <-b.frames:
			if g.In["type"] == typ {
				b.t.Fatalf("brain: unexpected %s frame %s", typ, g.Raw)
			}
		case <-deadline:
			return
		}
	}
}
