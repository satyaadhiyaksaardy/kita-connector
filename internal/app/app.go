// Package app wires wa ↔ filter ↔ outbox ↔ link and runs the state machine
// in DESIGN.md: self mode (terminal QR) and hosted mode (the brain asks to
// pair and shows the QR / code on the couple's dashboard).
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/mdp/qrterminal/v3"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/config"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/filter"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/link"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/outbox"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/wa"
)

const (
	relinkHint       = "Not linked to WhatsApp. Run `kita-connector login` to link."
	hostedRelinkHint = "Not linked to WhatsApp: waiting for the dashboard to link it."
)

type App struct {
	Cfg     config.Config
	Key     string
	Version string
	Log     *slog.Logger
	// Term is the user's terminal (QR, pairing code, hints), nil when
	// running as a service.
	Term io.Writer
	// Pair: link WhatsApp now if there is no session. PairPhone uses a
	// pairing code for that number instead of a QR.
	Pair      bool
	PairPhone string
	// OnOpen is called each time WhatsApp reaches OPEN.
	OnOpen func()

	// download fetches an image from its wa.MediaRef (wa.Client.Download;
	// tests replace it).
	download func(context.Context, []byte) ([]byte, error)
	pairQR   func(context.Context) error         // wa.Client.PairQR; tests replace it
	pairCode func(context.Context, string) error // wa.Client.PairCode; tests replace it
	pairMu   sync.Mutex                          // hosted: the pairing the brain asked for, if running
	pairStop context.CancelFunc
	pairDone chan struct{}
	fetching sync.Map  // ids of images being downloaded
	heldOnce sync.Once // held images of a previous run: fetched at the first OPEN

	ctx  context.Context
	wa   *wa.Client
	link *link.Client
	cfg  chan struct{} // closed on the first config
	once sync.Once

	mu   sync.Mutex
	st   Status
	last link.Status
}

// Status is what `kita-connector status` reads from <data>/status.json.
type Status struct {
	WhatsApp string    `json:"whatsapp"` // pairing | open | closed | logged_out
	Brain    string    `json:"brain"`    // connected | disconnected | <why it stopped>
	Latest   string    `json:"latestConnector,omitempty"`
	Updated  time.Time `json:"updated"`
}

// Run runs until ctx ends or the brain refuses this connector for good
// (link.ErrKey / ErrVersion / ErrReplaced).
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	a.ctx, a.cfg = ctx, make(chan struct{})
	if err := a.Cfg.EnsureData(); err != nil {
		return err
	}
	ob, err := outbox.Open(a.Cfg.Path("outbox.db"))
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	defer ob.Close()
	a.wa, err = wa.Open(ctx, a.Cfg.Path("session.db"), a.Log)
	if err != nil {
		return fmt.Errorf("whatsapp session: %w", err)
	}
	defer a.wa.Close()
	defer a.setStatus(func(s *Status) {
		if s.WhatsApp == "open" { // stopped: no longer connected (Disconnect sends no event)
			s.WhatsApp = "closed"
		}
	})
	a.wa.OnState = a.onState
	a.wa.OnMessage = a.onMessage
	if a.download == nil {
		a.download = a.wa.Download
	}
	if a.pairQR == nil {
		a.pairQR = a.wa.PairQR
	}
	if a.pairCode == nil {
		a.pairCode = a.wa.PairCode
	}
	a.link = &link.Client{
		URL: a.Cfg.Brain, Key: a.Key, Mode: a.Cfg.Mode, Version: a.Version,
		Outbox: ob, Log: a.Log, Handle: a.handle, OnLink: a.onLink,
	}
	a.setStatus(func(s *Status) { s.WhatsApp, s.Brain = "closed", "disconnected" })
	a.rememberQueued() // before the brain can answer them

	linkErr := make(chan error, 1)
	go func() { linkErr <- a.link.Run(ctx) }()

	// WhatsApp waits for the first config: without partnerPhone the filter
	// would drop everything, including the history sync after linking.
	select {
	case <-a.cfg:
	case err := <-linkErr:
		return a.stopped(err)
	}
	// Pairing blocks until linked; a refusal from the brain meanwhile
	// (4001/4003/4004) must stop it, not wait for the QR to expire.
	waErr := make(chan error, 1)
	go func() { waErr <- a.startWhatsApp(ctx) }()
	select {
	case err := <-linkErr:
		err = a.stopped(err) // before cancel: stopped ignores errors after it
		cancel()
		select {
		case <-waErr: // pairing ends with ctx
		case <-time.After(10 * time.Second):
		}
		return err
	case err := <-waErr:
		if err != nil {
			return err
		}
	}
	return a.stopped(<-linkErr)
}

func (a *App) startWhatsApp(ctx context.Context) error {
	if a.wa.Linked() {
		return a.wa.Connect()
	}
	a.onState(wa.State{State: "pairing"})
	if !a.Pair { // self: `login` pairs; hosted: the brain's `pair`
		a.Log.Warn(a.relinkHint(), "event", "not_linked")
		a.say(a.relinkHint())
		return nil
	}
	var err error
	if a.PairPhone != "" {
		err = a.wa.PairCode(ctx, a.PairPhone)
	} else {
		a.say("Scan this QR code in WhatsApp → Linked devices → Link a device:")
		err = a.pairQR(ctx)
	}
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("linking WhatsApp: %w", err)
	}
	return nil
}

// stopped records why the brain link ended.
func (a *App) stopped(err error) error {
	if a.ctx.Err() != nil {
		return nil
	}
	a.Log.Error("brain refused this connector, stopped", "event", "link_stopped", "error", err.Error())
	a.setStatus(func(s *Status) { s.Brain = err.Error() })
	return err
}

func (a *App) handle(in link.In) {
	switch in.Type {
	case "config":
		a.wa.SetPartner(in.PartnerPhone, in.Prefix)
		if in.LatestConnector != "" && in.LatestConnector != a.Version {
			a.Log.Info("update available", "event", "update_available", "latest", in.LatestConnector)
		}
		a.setStatus(func(s *Status) { s.Latest = in.LatestConnector })
		a.once.Do(func() { close(a.cfg) })
	case "reply":
		a.wa.Reply(in.ID, in.Text)
	case "react":
		a.wa.React(in.ID, in.Emoji)
	case "logout":
		a.Log.Info("brain asked to unlink WhatsApp", "event", "logout")
		go func() {
			if err := a.wa.Logout(a.ctx); err != nil {
				a.Log.Error("logout failed", "event", "logout_failed", "error", err.Error())
			}
		}()
	case "pair":
		if a.hosted() { // a self-hosted connector pairs in its own terminal
			go a.pairForBrain(in.Method, in.Phone)
		}
	}
}

func (a *App) hosted() bool { return a.Cfg.Mode == "hosted" }

func (a *App) relinkHint() string {
	if a.hosted() {
		return hostedRelinkHint
	}
	return relinkHint
}

// pairForBrain links WhatsApp when the dashboard asks (hosted); the QR or
// code reaches it through status frames. Ignored while linked; a request
// while pairing replaces that attempt (say QR → code on the bot phone itself).
func (a *App) pairForBrain(method, phone string) {
	if a.wa.Linked() {
		a.Log.Info("pair ignored: already linked", "event", "pair_ignored")
		return
	}
	if method != "qr" && (method != "code" || phone == "") {
		a.Log.Warn("pair ignored: unknown method or no phone", "event", "pair_ignored")
		return
	}
	ctx, stop := context.WithCancel(a.ctx)
	done := make(chan struct{})
	a.pairMu.Lock()
	prevStop, prevDone := a.pairStop, a.pairDone
	a.pairStop, a.pairDone = stop, done
	a.pairMu.Unlock()
	defer func() {
		stop()
		close(done)
		a.pairMu.Lock()
		if a.pairDone == done {
			a.pairStop, a.pairDone = nil, nil
		}
		a.pairMu.Unlock()
	}()
	if prevStop != nil {
		a.Log.Info("new pair request replaces the running one", "event", "pair_replaced")
		prevStop()
		<-prevDone
	}
	a.Log.Info("brain asked to link WhatsApp", "event", "pair", "method", method)
	var err error
	if method == "code" {
		err = a.pairCode(ctx, phone)
	} else {
		err = a.pairQR(ctx)
	}
	if err != nil && ctx.Err() == nil {
		a.Log.Warn("pairing ended without linking", "event", "pair_failed", "error", err.Error())
		a.onState(wa.State{State: "pairing"}) // no QR / code: the dashboard drops the dead one
	}
}

func (a *App) onLink(up bool) {
	a.setStatus(func(s *Status) {
		s.Brain = "disconnected"
		if up {
			s.Brain = "connected"
		}
	})
}

func (a *App) onState(s wa.State) {
	// self mode: QR and pairing code are shown here only, never sent;
	// hosted: they go to the brain, for the couple's dashboard
	if s.QR != "" && a.Term != nil {
		qrterminal.GenerateHalfBlock(s.QR, qrterminal.L, a.Term)
	}
	if s.Code != "" {
		a.say("On your phone: WhatsApp → Linked devices → Link with phone number, enter: " + s.Code)
	}
	st := link.Status{State: s.State}
	if s.Phone != "" {
		st.Self = &link.Self{Phone: s.Phone}
	}
	if a.hosted() {
		st.QR, st.PairCode = s.QR, s.Code
	}
	a.mu.Lock()
	newState := st.State != a.last.State || (st.Self != nil) != (a.last.Self != nil)
	newCode := st.QR != a.last.QR || st.PairCode != a.last.PairCode // hosted only
	a.last = st
	a.mu.Unlock()
	if newState || newCode {
		a.link.SetStatus(a.ctx, st)
	}
	if !newState {
		return // a QR refresh is not a state change
	}
	a.Log.Info("whatsapp "+s.State, "event", "wa_state", "state", s.State)
	a.setStatus(func(x *Status) { x.WhatsApp = s.State })
	switch s.State {
	case "open":
		a.heldOnce.Do(func() { go a.fetchHeld() })
		if s.Phone != "" {
			a.say("Linked to WhatsApp …" + last4(s.Phone) + ".")
		}
		if a.OnOpen != nil {
			a.OnOpen()
		}
	case "logged_out":
		a.Log.Warn(a.relinkHint(), "event", "not_linked")
		a.say("Unlinked from WhatsApp. " + a.relinkHint())
	}
}

// onMessage queues a forwarded message before whatsmeow acks it. An image
// is held with its media reference and downloaded afterwards: a redelivery
// can't be decrypted twice, so a download inside the ack window could lose it.
func (a *App) onMessage(out filter.Out) {
	m := link.Message{ID: out.ID, From: out.From, SentAt: out.SentAt.Unix(), Chat: out.Chat.String(), Sender: out.Sender.String()}
	var err error
	switch {
	case out.Image != nil && !out.TooBig:
		m.Image = &link.Image{Mime: out.Image.GetMimetype(), Caption: out.Text}
		ref := wa.MediaRef(out.Image)
		if err = a.link.HoldImage(m, ref); err == nil {
			go a.fetch(m, ref)
		}
	case out.Image != nil:
		m.Text, m.ImageTooBig = out.Text, true
		err = a.link.SendMessage(a.ctx, m)
	default:
		m.Text = out.Text
		err = a.link.SendMessage(a.ctx, m)
	}
	if err != nil {
		a.Log.Error("could not queue message", "event", "outbox_error", "msg_id", out.ID, "error", err.Error())
		return
	}
	a.Log.Info("message forwarded", "event", "message_forwarded", "msg_id", out.ID)
}

// fetch downloads a held image and sends the message. If the download fails
// the caption goes as text rather than losing the entry; if the connector is
// stopping it stays held for the next start.
func (a *App) fetch(m link.Message, ref []byte) {
	if _, busy := a.fetching.LoadOrStore(m.ID, true); busy {
		return
	}
	defer a.fetching.Delete(m.ID)
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Minute)
	data, err := a.download(ctx, ref)
	cancel()
	switch {
	case a.ctx.Err() != nil:
		return
	case err != nil:
		a.Log.Warn("image download failed, forwarding caption only", "event", "image_download_failed", "msg_id", m.ID, "error", err.Error())
		m.Text, m.Image = m.Image.Caption, nil
	case len(data) > filter.MaxImage:
		m.Text, m.Image, m.ImageTooBig = m.Image.Caption, nil, true
	default:
		m.Image.Data = data
	}
	if err := a.link.ReleaseImage(a.ctx, m); err != nil {
		a.Log.Error("could not queue message", "event", "outbox_error", "msg_id", m.ID, "error", err.Error())
	}
}

// rememberQueued gives wa the react/reply context of the messages a previous
// run queued: the brain answers them after the restart.
func (a *App) rememberQueued() {
	queued, err := a.link.Queued()
	if err != nil {
		a.Log.Error("could not read the outbox", "event", "outbox_error", "error", err.Error())
		return
	}
	for _, q := range queued {
		text := q.Message.Text
		if q.Message.Image != nil {
			text = q.Message.Image.Caption
		}
		a.wa.Remember(q.Message.ID, q.Message.Chat, q.Message.Sender, text)
	}
}

// fetchHeld downloads the images a previous run held but didn't send.
func (a *App) fetchHeld() {
	held, err := a.link.Held()
	if err != nil {
		a.Log.Error("could not read held images", "event", "outbox_error", "error", err.Error())
		return
	}
	for _, h := range held {
		a.fetch(h.Message, h.Media)
	}
}

func (a *App) setStatus(f func(*Status)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f(&a.st)
	a.st.Updated = time.Now().UTC()
	b, _ := json.MarshalIndent(a.st, "", "  ")
	path, tmp := a.Cfg.Path("status.json"), a.Cfg.Path("status.json.tmp")
	if os.WriteFile(tmp, b, 0o600) != nil {
		return
	}
	// Windows refuses to replace a file someone is reading (`status`)
	for range 20 {
		if os.Rename(tmp, path) == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = os.WriteFile(path, b, 0o600)
}

// ReadStatus reads what a running connector last wrote.
func ReadStatus(cfg config.Config) (Status, error) {
	var s Status
	b, err := os.ReadFile(cfg.Path("status.json"))
	if err == nil {
		err = json.Unmarshal(b, &s)
	}
	return s, err
}

func (a *App) say(s string) {
	if a.Term != nil {
		fmt.Fprintln(a.Term, s)
	}
}

func last4(s string) string {
	if len(s) <= 4 {
		return s
	}
	return s[len(s)-4:]
}
