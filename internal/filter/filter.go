// Package filter decides which WhatsApp messages leave the connector.
// This is the privacy promise in code: only `$` messages from the one 1:1
// chat with the partner are forwarded; everything else is dropped.
package filter

import (
	"strings"
	"time"
	"unicode"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	MaxImage = 5 << 20             // larger images are forwarded as imageTooBig
	MaxAge   = 14 * 24 * time.Hour // older (history-sync) messages are dropped
	botMark  = "🤖"                 // the brain's replies start with this
)

// Rule is what Decide needs besides the message.
type Rule struct {
	Partner    types.JID // partner's phone JID, from config.partnerPhone
	PartnerLID types.JID // partner's LID if whatsmeow knows the mapping; may be empty
	Prefix     string    // config.prefix
	SelfDevice uint16    // this connector's device number (loop guard)
	Now        time.Time
}

// Out is a message that may be forwarded.
type Out struct {
	ID     string
	From   string // "linked" | "partner"
	SentAt time.Time
	Text   string              // the text, or the image caption
	Image  *waE2E.ImageMessage // set for an image; Text is its caption
	TooBig bool                // image over MaxImage: forward without data
	// Where the brain's react/reply go: kept in memory and the outbox, never sent.
	Chat, Sender types.JID
}

// Decide reports whether m qualifies. m must already be unwrapped
// (events.Message.UnwrapRaw, which whatsmeow does before dispatching).
func Decide(m *events.Message, r Rule) (Out, bool) {
	if m == nil || m.Message == nil || r.Prefix == "" || r.Partner.User == "" {
		return Out{}, false
	}
	info := m.Info
	if info.IsGroup || !r.inPartnerChat(info.MessageSource) {
		return Out{}, false
	}
	from := "partner"
	if info.IsFromMe {
		if r.SelfDevice != 0 && info.Sender.Device == r.SelfDevice {
			return Out{}, false // sent by this connector
		}
		from = "linked"
	} else if !r.isPartner(info.Sender) && !r.isPartner(info.SenderAlt) {
		return Out{}, false
	}
	if m.IsViewOnce || m.IsEdit || info.Edit != "" || r.Now.Sub(info.Timestamp) > MaxAge {
		return Out{}, false
	}

	msg := m.Message
	var text string
	var img *waE2E.ImageMessage
	switch {
	case msg.Conversation != nil:
		text = msg.GetConversation()
	case msg.ExtendedTextMessage != nil:
		text = msg.GetExtendedTextMessage().GetText() // a quoted message is ignored
	case msg.ImageMessage != nil:
		img = msg.GetImageMessage()
		if img.GetViewOnce() {
			return Out{}, false
		}
		text = img.GetCaption()
	default:
		return Out{}, false // video, document, voice, sticker, reaction, protocol, call log, …
	}
	t := strings.TrimLeftFunc(text, unicode.IsSpace)
	if !strings.HasPrefix(t, r.Prefix) || strings.HasPrefix(t, botMark) {
		return Out{}, false
	}
	return Out{
		ID:     info.ID,
		From:   from,
		SentAt: info.Timestamp,
		Text:   text,
		Image:  img,
		TooBig: img != nil && img.GetFileLength() > MaxImage,
		Chat:   info.Chat,
		Sender: info.Sender.ToNonAD(),
	}, true
}

// inPartnerChat: the chat is the partner, by phone or by LID. A LID chat
// whatsmeow hasn't mapped yet is accepted when the message's alternate
// address is the partner's phone.
func (r Rule) inPartnerChat(s types.MessageSource) bool {
	if r.isPartner(s.Chat) {
		return true
	}
	if s.Chat.Server != types.HiddenUserServer {
		return false
	}
	if s.IsFromMe {
		return r.isPartner(s.RecipientAlt)
	}
	return r.isPartner(s.SenderAlt)
}

func (r Rule) isPartner(j types.JID) bool {
	switch j.Server {
	case types.DefaultUserServer:
		return j.User == r.Partner.User
	case types.HiddenUserServer:
		return !r.PartnerLID.IsEmpty() && j.User == r.PartnerLID.User
	}
	return false
}
