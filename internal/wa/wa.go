// Package wa wraps whatsmeow: session store, pairing, connection state,
// incoming messages through the filter, and the rate-limited reply/react path.
//
// Raw messages never leave this package except through filter.Decide.
// Nothing about a dropped message is stored or logged.
package wa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/filter"
)

var ErrQRTimeout = errors.New("QR code expired")

// State is a WhatsApp connection state change (PROTOCOL.md `status`).
type State struct {
	State string // pairing | open | closed | logged_out
	QR    string // pairing: current QR code
	Code  string // pairing: pairing code
	Phone string // open: the linked account's phone
}

type Client struct {
	Log *slog.Logger
	// OnState and OnMessage are called from whatsmeow's event goroutine.
	// OnMessage gets a message that passed the filter (an image is not
	// downloaded yet: see MediaRef, Download). When it returns, whatsmeow
	// acks the message to WhatsApp (SynchronousAck), so it must have been
	// persisted by then: a redelivery can't be decrypted a second time.
	OnState   func(State)
	OnMessage func(filter.Out)

	db    *sqlstore.Container
	sends chan func()

	mu         sync.Mutex
	cli        *whatsmeow.Client
	partner    types.JID
	partnerLID types.JID
	prefix     string
	recent     map[string]msgKey // forwarded ids → what reply/react need
	order      []string
}

type msgKey struct {
	chat, sender types.JID
	text         string // for the quote preview; memory only
}

// ponytail: in memory, last 1000 forwarded ids, plus whatever is still in the
// outbox at start (Remember). A react/reply to an older id from before a
// restart sends unquoted (reply) or is skipped (react).
const recentMax = 1000

func Open(ctx context.Context, path string, log *slog.Logger) (*Client, error) {
	store.SetOSInfo("kita-connector", [3]uint32{0, 1, 0})
	db, err := sqlstore.New(ctx, "sqlite", path+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", waLogger{log})
	if err != nil {
		return nil, err
	}
	dev, err := db.GetFirstDevice(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	c := &Client{Log: log, db: db, sends: make(chan func(), 64), recent: map[string]msgKey{}}
	c.use(dev)
	go func() {
		for send := range c.sends {
			send()
			time.Sleep(time.Second) // ≤ 1 send per second
		}
	}()
	return c, nil
}

func (c *Client) use(dev *store.Device) {
	cli := whatsmeow.NewClient(dev, waLogger{c.Log})
	cli.SynchronousAck = true
	cli.InitialAutoReconnect = true // network not up yet at boot: keep trying
	cli.AddEventHandler(c.handle)
	c.mu.Lock()
	c.cli = cli
	c.mu.Unlock()
}

func (c *Client) client() *whatsmeow.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cli
}

func (c *Client) Linked() bool { return c.client().Store.ID != nil }

// SetPartner applies config.partnerPhone and config.prefix.
func (c *Client) SetPartner(phone, prefix string) {
	digits := nonDigit.ReplaceAllString(phone, "")
	c.mu.Lock()
	defer c.mu.Unlock()
	if digits != c.partner.User {
		c.partnerLID = types.JID{}
	}
	c.partner = types.NewJID(digits, types.DefaultUserServer)
	c.prefix = prefix
}

// Connect connects an already linked session; whatsmeow reconnects by itself.
func (c *Client) Connect() error { return c.client().Connect() }

func (c *Client) Close() {
	c.client().Disconnect()
	c.db.Close()
}

// PairQR links by QR code, reported through OnState. Returns once linked.
func (c *Client) PairQR(ctx context.Context) error {
	return c.pair(ctx, "")
}

// PairCode links with a pairing code entered on the phone with this number.
func (c *Client) PairCode(ctx context.Context, phone string) error {
	return c.pair(ctx, phone)
}

func (c *Client) pair(ctx context.Context, phone string) error {
	cli := c.client()
	qr, err := cli.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := cli.Connect(); err != nil {
		return err
	}
	coded := false
	for item := range qr {
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			if phone == "" {
				c.state(State{State: "pairing", QR: item.Code})
			} else if !coded {
				code, err := cli.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
				if err != nil {
					cli.Disconnect()
					return err
				}
				coded = true
				c.state(State{State: "pairing", Code: code})
			}
		case whatsmeow.QRChannelSuccess.Event:
			return nil
		case whatsmeow.QRChannelTimeout.Event:
			cli.Disconnect()
			return ErrQRTimeout
		case whatsmeow.QRChannelEventError:
			cli.Disconnect()
			return item.Error
		case whatsmeow.QRChannelEventPasskeyRequest, whatsmeow.QRChannelEventPasskeyResponse:
		default:
			cli.Disconnect()
			return fmt.Errorf("pairing failed: %s", item.Event)
		}
	}
	// ctx ended (stopping, or a new pairing replaces this one): a still
	// connected socket would make the next GetQRChannel fail
	cli.Disconnect()
	return ctx.Err()
}

// Logout unlinks from WhatsApp, deletes the session and starts a fresh,
// unlinked one (PAIRING).
func (c *Client) Logout(ctx context.Context) error {
	cli := c.client()
	if cli.Store.ID == nil {
		return nil
	}
	err := cli.Logout(ctx)
	if err != nil { // offline or already gone: still drop the local session
		cli.Disconnect()
		if derr := cli.Store.Delete(ctx); derr != nil {
			return errors.Join(err, derr)
		}
	}
	c.reset()
	return nil
}

func (c *Client) reset() {
	c.state(State{State: "logged_out"})
	c.use(c.db.NewDevice())
	c.state(State{State: "pairing"})
}

func (c *Client) handle(evt any) {
	switch e := evt.(type) {
	case *events.Message:
		c.message(e)
	case *events.HistorySync:
		cli := c.client()
		for _, conv := range e.Data.GetConversations() {
			chat, err := types.ParseJID(conv.GetID())
			if err != nil {
				continue
			}
			for _, hm := range conv.GetMessages() {
				if m, err := cli.ParseWebMessage(chat, hm.GetMessage()); err == nil {
					c.message(m)
				}
			}
		}
	case *events.Connected:
		var phone string
		if id := c.client().Store.ID; id != nil {
			phone = id.User
		}
		c.state(State{State: "open", Phone: phone})
	case *events.Disconnected:
		c.state(State{State: "closed"})
	case *events.StreamReplaced:
		c.Log.Error("another copy of this connector took over the WhatsApp session", "event", "wa_replaced")
		c.state(State{State: "closed"})
	case *events.TemporaryBan:
		c.Log.Error("WhatsApp temporarily banned this account", "event", "wa_banned", "detail", e.String())
		c.state(State{State: "closed"})
	case *events.ClientOutdated:
		c.Log.Error("WhatsApp says this client is outdated, please update", "event", "wa_outdated")
		c.state(State{State: "closed"})
	case *events.ConnectFailure:
		c.Log.Warn("WhatsApp connect failure", "event", "wa_connect_failure", "reason", int(e.Reason))
	case *events.PairSuccess:
		c.Log.Info("linked to WhatsApp", "event", "wa_paired")
	case *events.LoggedOut:
		// whatsmeow already deleted the session (phone removed the device,
		// or offline too long)
		c.Log.Warn("logged out of WhatsApp", "event", "wa_logged_out", "reason", int(e.Reason))
		c.reset()
	}
}

func (c *Client) state(s State) {
	if c.OnState != nil {
		c.OnState(s)
	}
}

// message runs the filter; only a forwarded message gets past the first return.
func (c *Client) message(m *events.Message) {
	out, ok := filter.Decide(m, c.rule(m))
	if !ok {
		return
	}
	c.remember(out.ID, msgKey{out.Chat, out.Sender, out.Text})
	c.OnMessage(out)
}

// MediaRef is what Download needs to fetch img later, and nothing else: no
// caption, thumbnail or context (a quoted message's text lives there).
func MediaRef(img *waE2E.ImageMessage) []byte {
	b, _ := proto.Marshal(&waE2E.ImageMessage{
		DirectPath: img.DirectPath, MediaKey: img.MediaKey,
		FileSHA256: img.FileSHA256, FileEncSHA256: img.FileEncSHA256,
	})
	return b
}

// Download fetches the image a MediaRef points to.
func (c *Client) Download(ctx context.Context, ref []byte) ([]byte, error) {
	var img waE2E.ImageMessage
	if err := proto.Unmarshal(ref, &img); err != nil {
		return nil, err
	}
	return c.client().Download(ctx, &img)
}

func (c *Client) rule(m *events.Message) filter.Rule {
	c.mu.Lock()
	cli, partner, lid, prefix := c.cli, c.partner, c.partnerLID, c.prefix
	c.mu.Unlock()
	if lid.IsEmpty() && !partner.IsEmpty() && m.Info.Chat.Server == types.HiddenUserServer {
		if l, err := cli.Store.LIDs.GetLIDForPN(context.Background(), partner); err == nil && !l.IsEmpty() {
			lid = l
			c.mu.Lock()
			c.partnerLID = l
			c.mu.Unlock()
		}
	}
	var self uint16
	if cli.Store.ID != nil {
		self = cli.Store.ID.Device
	}
	return filter.Rule{Partner: partner, PartnerLID: lid, Prefix: prefix, SelfDevice: self, Now: time.Now()}
}

func (c *Client) remember(id string, k msgKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.recent[id]; !ok {
		c.order = append(c.order, id)
	}
	c.recent[id] = k
	if len(c.order) > recentMax {
		delete(c.recent, c.order[0])
		c.order = c.order[1:]
	}
}

// Remember restores what a react/reply to id needs, for a message forwarded
// before a restart and still in the outbox.
func (c *Client) Remember(id, chat, sender, text string) {
	ch, err1 := types.ParseJID(chat)
	se, err2 := types.ParseJID(sender)
	if err1 == nil && err2 == nil && !ch.IsEmpty() {
		c.remember(id, msgKey{ch, se, text})
	}
}

func (c *Client) lookup(id string) (msgKey, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k, ok := c.recent[id]
	return k, ok
}

// Reply sends text in the couple chat, quoting id when it is known.
func (c *Client) Reply(id, text string) {
	c.enqueue("reply", id, func(cli *whatsmeow.Client, partner types.JID) (types.JID, *waE2E.Message) {
		k, ok := c.lookup(id)
		if id == "" || !ok {
			return partner, &waE2E.Message{Conversation: proto.String(text)}
		}
		return k.chat, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String(text),
			ContextInfo: &waE2E.ContextInfo{
				StanzaID:      proto.String(id),
				Participant:   proto.String(k.sender.String()),
				QuotedMessage: &waE2E.Message{Conversation: proto.String(k.text)},
			},
		}}
	})
}

// React reacts to a forwarded message id in the couple chat.
func (c *Client) React(id, emoji string) {
	c.enqueue("react", id, func(cli *whatsmeow.Client, _ types.JID) (types.JID, *waE2E.Message) {
		k, ok := c.lookup(id)
		if !ok {
			return types.JID{}, nil
		}
		return k.chat, cli.BuildReaction(k.chat, k.sender, id, emoji)
	})
}

// enqueue serialises sends (≤ 1/s). build returns the chat and message, or
// nil to skip. Sends only ever go to the partner chat.
func (c *Client) enqueue(kind, id string, build func(*whatsmeow.Client, types.JID) (types.JID, *waE2E.Message)) {
	send := func() {
		c.mu.Lock()
		cli, partner := c.cli, c.partner
		c.mu.Unlock()
		if partner.IsEmpty() {
			return
		}
		to, msg := build(cli, partner)
		if msg == nil {
			c.Log.Warn(kind+" skipped: message id not known", "event", kind+"_skipped", "msg_id", id)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := cli.SendMessage(ctx, to, msg); err != nil {
			c.Log.Warn(kind+" failed", "event", kind+"_failed", "msg_id", id, "error", err.Error())
		}
	}
	select {
	case c.sends <- send:
	default:
		c.Log.Warn("send queue full, dropped "+kind, "event", "send_dropped", "msg_id", id)
	}
}

// Redact keeps the last 4 digits of every run of 5+ digits (phone numbers, LIDs).
func Redact(s string) string {
	return digitRun.ReplaceAllStringFunc(s, func(d string) string { return "…" + d[len(d)-4:] })
}

var (
	digitRun = regexp.MustCompile(`\d{5,}`)
	msgID    = regexp.MustCompile(`\b[0-9A-F]{16,}\b`) // WhatsApp message ids: 3EB0…, 3A…, 32-hex
	nonDigit = regexp.MustCompile(`\D`)
)

// waLogger passes whatsmeow's warnings and errors to slog with phone numbers
// redacted. Its info/debug output (JIDs, names, raw nodes) is dropped, and so
// is every line naming a message id: those are per-message (undecryptable,
// unavailable, view-once, receipts…) and can be about any chat, while dropped
// messages are never logged, not even their ids.
type waLogger struct{ l *slog.Logger }

func (w waLogger) Errorf(f string, a ...any) { w.log(slog.LevelError, f, a) }
func (w waLogger) Warnf(f string, a ...any)  { w.log(slog.LevelWarn, f, a) }

func (w waLogger) log(level slog.Level, f string, a []any) {
	s := fmt.Sprintf(f, a...)
	if msgID.MatchString(s) {
		return
	}
	w.l.Log(context.Background(), level, Redact(s), "event", "whatsmeow")
}
func (waLogger) Infof(string, ...any)  {}
func (waLogger) Debugf(string, ...any) {}
func (w waLogger) Sub(module string) waLog.Logger {
	return waLogger{w.l.With("module", strings.ToLower(module))}
}
