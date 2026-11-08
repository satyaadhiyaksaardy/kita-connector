package wa

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/filter"
)

// A real session store (pure-Go SQLite) and the real handler: forwarded
// messages reach OnMessage, dropped ones vanish, and no text reaches the log.
func TestMessagesNeverLogText(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "session.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Linked() {
		t.Fatal("fresh store is linked")
	}
	var got []filter.Out
	c.OnMessage = func(o filter.Out) { got = append(got, o) }
	c.SetPartner("+62 811-1111-1111", "$")

	partner := types.NewJID("6281111111111", types.DefaultUserServer)
	other := types.NewJID("6283333333333", types.DefaultUserServer)
	send := func(chat types.JID, id, text string) {
		c.handle((&events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{Chat: chat, Sender: chat},
				ID:            id, Timestamp: time.Now(),
			},
			RawMessage: &waE2E.Message{Conversation: proto.String(text)},
		}).UnwrapRaw())
	}
	send(partner, "KEEP", "$ 25000 SECRET-FORWARDED")
	send(partner, "DROP1", "SECRET-NO-PREFIX")
	send(other, "DROP2", "$ SECRET-OTHER-CHAT")
	c.handle(&events.ConnectFailure{Reason: events.ConnectFailureLoggedOut})
	waLogger{log}.Errorf("decrypt failed for %s", "6281111111111@s.whatsapp.net")

	if len(got) != 1 || got[0].ID != "KEEP" || got[0].From != "partner" {
		t.Fatalf("forwarded %+v", got)
	}
	if _, ok := c.lookup("KEEP"); !ok {
		t.Fatal("forwarded id not remembered for react/reply")
	}
	if _, ok := c.lookup("DROP1"); ok {
		t.Fatal("dropped id remembered")
	}
	out := logs.String()
	for _, bad := range []string{"SECRET", "DROP1", "DROP2", "6281111111111", "6283333333333"} {
		if strings.Contains(out, bad) {
			t.Fatalf("log contains %q:\n%s", bad, out)
		}
	}
	if !strings.Contains(out, "…1111@s.whatsapp.net") {
		t.Fatalf("whatsmeow error not logged redacted:\n%s", out)
	}
}

// whatsmeow's per-message warnings can be about any chat, so they are
// dropped: they name a message id (a view-once from the partner was logged
// as "Unavailable message <id> from …<lid>").
func TestWhatsmeowPerMessageLinesDropped(t *testing.T) {
	var logs bytes.Buffer
	w := waLogger{slog.New(slog.NewJSONHandler(&logs, nil))}
	w.Warnf("Unavailable message %s from %s (type: %q)", "3A0000000000000000AA", "100000000009999@lid", "view_once")
	w.Errorf("Failed to send retry receipt for %s: %v", "3EB0000000000000000BB", "timeout")
	w.Warnf("Handler for %s failed", "A1B2C3D4E5F60718293A4B5C6D7E8F90")
	w.Warnf("Error sending close to websocket: %v", "EOF")
	out := logs.String()
	for _, bad := range []string{"3A0000", "3EB000", "A1B2C3", "9999"} {
		if strings.Contains(out, bad) {
			t.Fatalf("log contains %q:\n%s", bad, out)
		}
	}
	if !strings.Contains(out, "Error sending close") {
		t.Fatalf("connection warning dropped:\n%s", out)
	}
}

// The outbox holds a forwarded image's MediaRef until it is downloaded:
// only the download fields, never the caption, thumbnail or a quoted message.
func TestMediaRefKeepsOnlyDownloadFields(t *testing.T) {
	img := &waE2E.ImageMessage{
		DirectPath: proto.String("/v/t62/x"), MediaKey: []byte{1}, FileSHA256: []byte{2}, FileEncSHA256: []byte{3},
		Caption: proto.String("$ SECRET-CAPTION"), Mimetype: proto.String("image/jpeg"), JPEGThumbnail: []byte("SECRET-THUMB"),
		ContextInfo: &waE2E.ContextInfo{QuotedMessage: &waE2E.Message{Conversation: proto.String("SECRET-QUOTED")}},
	}
	ref := MediaRef(img)
	if strings.Contains(string(ref), "SECRET") || strings.Contains(string(ref), "image/jpeg") {
		t.Fatalf("ref carries more than the download fields: %q", ref)
	}
	var back waE2E.ImageMessage
	if err := proto.Unmarshal(ref, &back); err != nil || back.GetDirectPath() != "/v/t62/x" || !bytes.Equal(back.GetMediaKey(), []byte{1}) ||
		!bytes.Equal(back.GetFileSHA256(), []byte{2}) || !bytes.Equal(back.GetFileEncSHA256(), []byte{3}) {
		t.Fatalf("ref lost a download field: %v %+v", err, &back)
	}
}

func TestRedact(t *testing.T) {
	if got := Redact("6281234567890@s.whatsapp.net and 1234 and 123456789012345@lid"); got != "…7890@s.whatsapp.net and 1234 and …2345@lid" {
		t.Fatal(got)
	}
}
