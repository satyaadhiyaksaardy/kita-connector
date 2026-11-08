// Package link is the connector's WebSocket client to the brain
// (PROTOCOL.md): hello/config, frames, ping, reconnect, resend timer.
package link

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/outbox"
)

const (
	Protocol = "1"
	MaxFrame = 6 << 20 // frames over this are rejected by both sides
)

// Timings from PROTOCOL.md; vars only so tests can shrink them.
var (
	pingEvery  = 30 * time.Second
	tick       = 5 * time.Second // how often the outbox is checked for due resends
	backoffMin = time.Second
	backoffMax = time.Minute
)

// Errors that stop Run for good: they need a human.
var (
	ErrKey      = errors.New("key invalid")                                      // 4001
	ErrVersion  = errors.New("protocol unsupported, please update")              // 4003
	ErrReplaced = errors.New("replaced by another connector using the same key") // 4004
)

// In is any brain → connector frame.
type In struct {
	Type            string `json:"type"`
	ID              string `json:"id,omitempty"`
	Emoji           string `json:"emoji,omitempty"`
	Text            string `json:"text,omitempty"`
	Method          string `json:"method,omitempty"`
	Phone           string `json:"phone,omitempty"` // pair, method "code": the number to link
	PartnerPhone    string `json:"partnerPhone,omitempty"`
	Prefix          string `json:"prefix,omitempty"`
	LatestConnector string `json:"latestConnector,omitempty"`
}

type Status struct {
	State    string `json:"state"` // pairing | open | closed | logged_out
	QR       string `json:"qr,omitempty"`
	PairCode string `json:"pairCode,omitempty"`
	Self     *Self  `json:"self,omitempty"`
}

type Self struct {
	Phone string `json:"phone"`
}

type Message struct {
	ID          string `json:"id"`
	From        string `json:"from"` // linked | partner
	SentAt      int64  `json:"sentAt"`
	Text        string `json:"text,omitempty"`
	Image       *Image `json:"image,omitempty"`
	ImageTooBig bool   `json:"imageTooBig,omitempty"`
	// Where the brain's react/reply go (JIDs): kept in the outbox, never sent.
	Chat   string `json:"-"`
	Sender string `json:"-"`
}

type Image struct {
	Mime    string `json:"mime"`
	Data    []byte `json:"data"` // encoding/json writes []byte as base64
	Caption string `json:"caption,omitempty"`
}

type Client struct {
	URL, Key, Mode, Version string
	Outbox                  *outbox.Outbox
	Log                     *slog.Logger
	// Handle gets every frame except `received` (handled here), from the
	// read loop: it must not block.
	Handle func(In)
	// OnLink reports the brain connection going up (config received) or down.
	OnLink func(up bool)

	mu     sync.Mutex
	conn   *websocket.Conn
	status []byte // last status frame, resent after every hello
}

// Run connects and keeps reconnecting with backoff until ctx ends or the
// brain closes with 4001/4003/4004.
func (c *Client) Run(ctx context.Context) error {
	delay := backoffMin
	for {
		up, err := c.session(ctx)
		if up && c.OnLink != nil {
			c.OnLink(false)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch websocket.CloseStatus(err) {
		case 4001:
			return ErrKey
		case 4003:
			return ErrVersion
		case 4004:
			return ErrReplaced
		}
		if up {
			delay = backoffMin
		}
		c.Log.Warn("brain disconnected", "event", "link_down", "error", err.Error(), "retry_in", delay.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, backoffMax)
	}
}

// session runs one connection; up is true once config arrived.
func (c *Client) session(ctx context.Context) (up bool, err error) {
	conn, _, err := websocket.Dial(ctx, c.URL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + c.Key}},
	})
	if err != nil {
		return false, err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(MaxFrame)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	hello, _ := json.Marshal(struct {
		Type      string `json:"type"`
		Version   string `json:"version"`
		Mode      string `json:"mode"`
		Connector string `json:"connector"`
	}{"hello", Protocol, c.Mode, c.Version})
	if err := write(ctx, conn, hello); err != nil {
		return false, err
	}

	// Reading and pinging run on their own goroutines: a pong is only
	// processed while Read is running.
	frames := make(chan In)
	connErr := make(chan error, 2)
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				connErr <- err
				return
			}
			var in In
			if json.Unmarshal(data, &in) != nil {
				continue // not a frame: ignore, like an unknown type
			}
			select {
			case frames <- in:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		ping := time.NewTicker(pingEvery)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ping.C:
				pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					connErr <- err
					return
				}
			}
		}
	}()

	resend := time.NewTicker(tick)
	defer resend.Stop()
	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "")
			return up, ctx.Err()
		case err := <-connErr:
			return up, err
		case <-resend.C:
			if up {
				if err := c.flush(ctx, conn, time.Now()); err != nil {
					return up, err
				}
			}
		case in := <-frames:
			switch {
			case in.Type == "received":
				if err := c.Outbox.Ack(in.ID); err != nil {
					c.Log.Error("outbox ack failed", "event", "outbox_error", "msg_id", in.ID, "error", err.Error())
				}
			case in.Type == "config" && !up:
				up = true
				c.mu.Lock()
				c.conn = conn
				status := c.status
				c.mu.Unlock()
				defer func() {
					c.mu.Lock()
					c.conn = nil
					c.mu.Unlock()
				}()
				if status != nil {
					if err := write(ctx, conn, status); err != nil {
						return up, err
					}
				}
				// after every reconnect: everything pending goes again
				if err := c.flush(ctx, conn, time.Now().Add(100*365*24*time.Hour)); err != nil {
					return up, err
				}
				c.Log.Info("brain connected", "event", "link_up")
				if c.OnLink != nil {
					c.OnLink(true)
				}
				c.Handle(in)
			default:
				c.Handle(in)
			}
		}
	}
}

// flush prunes expired messages and sends those due by t.
func (c *Client) flush(ctx context.Context, conn *websocket.Conn, t time.Time) error {
	now := time.Now()
	dropped, err := c.Outbox.Prune(now)
	for _, id := range dropped {
		c.Log.Warn("message dropped after 24 h without received", "event", "outbox_expired", "msg_id", id)
	}
	if err != nil {
		return err
	}
	items, err := c.Outbox.Due(t)
	if err != nil {
		return err
	}
	for _, it := range items {
		if err := write(ctx, conn, it.Frame); err != nil {
			return err
		}
		if err := c.Outbox.Sent(it.ID, now); err != nil {
			return err
		}
		c.Log.Info("message resent", "event", "message_resent", "msg_id", it.ID, "attempts", it.Attempts+1)
	}
	return nil
}

// MessageFrame encodes m as a `message` frame (the app checks its size).
func MessageFrame(m Message) ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Message
	}{"message", m})
}

// frameFor encodes m; an image whose frame would exceed MaxFrame is sent as
// imageTooBig (base64 grows an image by 4/3: one near 5 MB doesn't fit in 6 MB).
func frameFor(m Message) ([]byte, error) {
	frame, err := MessageFrame(m)
	if err == nil && len(frame) > MaxFrame && m.Image != nil {
		m.Text, m.Image, m.ImageTooBig = m.Image.Caption, nil, true
		frame, err = MessageFrame(m)
	}
	return frame, err
}

// SendMessage queues m in the outbox and sends it now if connected;
// otherwise the reconnect/resend path delivers it.
func (c *Client) SendMessage(ctx context.Context, m Message) error {
	frame, err := frameFor(m)
	if err != nil {
		return err
	}
	if err := c.Outbox.Put(m.ID, frame, m.Chat, m.Sender, time.Now()); err != nil {
		return err
	}
	c.send(ctx, frame)
	return nil
}

// HoldImage queues an image message whose image isn't downloaded yet (no
// Data), with the media reference to download it. Nothing is sent until
// ReleaseImage.
func (c *Client) HoldImage(m Message, media []byte) error {
	frame, err := MessageFrame(m)
	if err != nil {
		return err
	}
	return c.Outbox.Hold(m.ID, frame, media, m.Chat, m.Sender, time.Now())
}

// ReleaseImage replaces a held message with m, now complete, and sends it.
func (c *Client) ReleaseImage(ctx context.Context, m Message) error {
	frame, err := frameFor(m)
	if err != nil {
		return err
	}
	if err := c.Outbox.Release(m.ID, frame, time.Now()); err != nil {
		return err
	}
	c.send(ctx, frame)
	return nil
}

// Held is a held image message and its media reference.
type Held struct {
	Message Message
	Media   []byte
}

// Held lists the image messages still to be downloaded (after a restart).
func (c *Client) Held() ([]Held, error) { return messages(c.Outbox.Held) }

// Queued lists every message still waiting for `received` (after a
// restart: for their react/reply context).
func (c *Client) Queued() ([]Held, error) { return messages(c.Outbox.Queued) }

func messages(list func() ([]outbox.Item, error)) ([]Held, error) {
	items, err := list()
	if err != nil {
		return nil, err
	}
	out := make([]Held, 0, len(items))
	for _, it := range items {
		var f struct{ Message }
		if err := json.Unmarshal(it.Frame, &f); err != nil {
			return nil, err
		}
		f.Message.Chat, f.Message.Sender = it.Chat, it.Sender
		out = append(out, Held{f.Message, it.Media})
	}
	return out, nil
}

// SetStatus sends s now if connected and again after every reconnect.
func (c *Client) SetStatus(ctx context.Context, s Status) {
	frame, _ := json.Marshal(struct {
		Type string `json:"type"`
		Status
	}{"status", s})
	c.mu.Lock()
	c.status = frame
	c.mu.Unlock()
	c.send(ctx, frame)
}

// send writes if connected. A failed write breaks the connection, and the
// reconnect path resends from the outbox.
func (c *Client) send(ctx context.Context, frame []byte) {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = write(ctx, conn, frame)
	}
}

func write(ctx context.Context, conn *websocket.Conn, frame []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, frame)
}
