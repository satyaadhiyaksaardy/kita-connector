package filter

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

var (
	now        = time.Unix(1_800_000_000, 0)
	partnerPN  = types.NewJID("6281111111111", types.DefaultUserServer)
	partnerLID = types.NewJID("111111111111111", types.HiddenUserServer)
	myPN       = types.NewJID("6282222222222", types.DefaultUserServer)
	myLID      = types.NewJID("222222222222222", types.HiddenUserServer)
	rule       = Rule{Partner: partnerPN, PartnerLID: partnerLID, Prefix: "$", SelfDevice: 23, Now: now}
)

func text(s string) *waE2E.Message { return &waE2E.Message{Conversation: proto.String(s)} }

func image(caption string, size uint64) *waE2E.Message {
	return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Caption: proto.String(caption), Mimetype: proto.String("image/jpeg"), FileLength: proto.Uint64(size),
	}}
}

func wrap(m *waE2E.Message) *waE2E.FutureProofMessage { return &waE2E.FutureProofMessage{Message: m} }

// fromPartner builds an incoming event the way whatsmeow dispatches it.
func fromPartner(raw *waE2E.Message, mods ...func(*types.MessageInfo)) *events.Message {
	info := types.MessageInfo{
		MessageSource: types.MessageSource{Chat: partnerPN, Sender: partnerPN},
		ID:            "3EB0AAAA", Timestamp: now.Add(-time.Minute),
	}
	for _, mod := range mods {
		mod(&info)
	}
	return (&events.Message{Info: info, RawMessage: raw}).UnwrapRaw()
}

// fromMe builds a message sent by the linked account from the given device;
// device 0 is the phone, which wraps it in DeviceSentMessage.
func fromMe(device uint16, raw *waE2E.Message, mods ...func(*types.MessageInfo)) *events.Message {
	sender := myPN
	sender.Device = device
	return fromPartner(&waE2E.Message{DeviceSentMessage: &waE2E.DeviceSentMessage{
		DestinationJID: proto.String(partnerPN.String()), Message: raw,
	}}, append([]func(*types.MessageInfo){func(i *types.MessageInfo) {
		i.Sender, i.IsFromMe = sender, true
	}}, mods...)...)
}

func chat(j types.JID) func(*types.MessageInfo) {
	return func(i *types.MessageInfo) { i.Chat = j }
}

func TestDecide(t *testing.T) {
	type want struct {
		forward bool
		from    string
		text    string
		image   bool
		tooBig  bool
	}
	noLID := rule
	noLID.PartnerLID = types.JID{}
	other := types.NewJID("6283333333333", types.DefaultUserServer)
	group := types.NewJID("120363000000000000", types.GroupServer)

	tests := []struct {
		name string
		m    *events.Message
		r    Rule
		want want
	}{
		// ✅ forwarded
		{"partner $ text", fromPartner(text("$25000 makan")), rule, want{true, "partner", "$25000 makan", false, false}},
		{"partner $ after leading spaces", fromPartner(text("  \t$ 10k parkir")), rule, want{true, "partner", "  \t$ 10k parkir", false, false}},
		{"partner $ extended text", fromPartner(&waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("$5k https://x.y")}}), rule, want{true, "partner", "$5k https://x.y", false, false}},
		{"linked account from the phone", fromMe(0, text("$ 7000 kopi")), rule, want{true, "linked", "$ 7000 kopi", false, false}},
		{"linked account from another companion", fromMe(5, text("$ 7000 kopi")), rule, want{true, "linked", "$ 7000 kopi", false, false}},
		{"partner seen by LID", fromPartner(text("$1"), chat(partnerLID), func(i *types.MessageInfo) { i.Sender = partnerLID }), rule, want{true, "partner", "$1", false, false}},
		{"partner LID not mapped yet, phone in SenderAlt", fromPartner(text("$1"), chat(partnerLID), func(i *types.MessageInfo) { i.Sender, i.SenderAlt = partnerLID, partnerPN }), noLID, want{true, "partner", "$1", false, false}},
		{"linked in LID chat", fromMe(0, text("$1"), chat(partnerLID), func(i *types.MessageInfo) { i.Sender = myLID }), rule, want{true, "linked", "$1", false, false}},
		{"linked in unmapped LID chat, phone in RecipientAlt", fromMe(0, text("$1"), chat(partnerLID), func(i *types.MessageInfo) { i.RecipientAlt = partnerPN }), noLID, want{true, "linked", "$1", false, false}},
		{"disappearing-message wrapper", fromPartner(&waE2E.Message{EphemeralMessage: wrap(text("$ 3k"))}), rule, want{true, "partner", "$ 3k", false, false}},
		{"reply quoting another message", fromPartner(&waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String("$ 12k"),
			ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("OLD"), QuotedMessage: text("secret quoted text")},
		}}), rule, want{true, "partner", "$ 12k", false, false}},
		{"image with $ caption", fromPartner(image("$ struk", 200_000)), rule, want{true, "partner", "$ struk", true, false}},
		{"image with $ caption at exactly 5 MB", fromPartner(image("$", MaxImage)), rule, want{true, "partner", "$", true, false}},
		{"image with $ caption over 5 MB", fromPartner(image("$ struk", MaxImage+1)), rule, want{true, "partner", "$ struk", true, true}},
		{"$ alone", fromPartner(text("$")), rule, want{true, "partner", "$", false, false}},
		{"history message 13 days old", fromPartner(text("$1"), func(i *types.MessageInfo) { i.Timestamp = now.Add(-13 * 24 * time.Hour) }), rule, want{true, "partner", "$1", false, false}},

		// ❌ dropped
		{"text without $", fromPartner(text("makan 25000")), rule, want{}},
		{"$ not at the start", fromPartner(text("makan $25000")), rule, want{}},
		{"reply quoting a $ message, own text without $", fromPartner(&waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String("ok"), ContextInfo: &waE2E.ContextInfo{QuotedMessage: text("$ 12k")},
		}}), rule, want{}},
		{"image without caption", fromPartner(image("", 100)), rule, want{}},
		{"image with non-$ caption", fromPartner(image("struk", 100)), rule, want{}},
		{"video with $ caption", fromPartner(&waE2E.Message{VideoMessage: &waE2E.VideoMessage{Caption: proto.String("$1")}}), rule, want{}},
		{"document with $ caption", fromPartner(&waE2E.Message{DocumentWithCaptionMessage: wrap(&waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{Caption: proto.String("$1")}})}), rule, want{}},
		{"voice note", fromPartner(&waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true)}}), rule, want{}},
		{"sticker", fromPartner(&waE2E.Message{StickerMessage: &waE2E.StickerMessage{}}), rule, want{}},
		{"view-once image with $ caption", fromPartner(&waE2E.Message{ViewOnceMessageV2: wrap(image("$ rahasia", 100))}), rule, want{}},
		{"view-once v1 wrapper", fromPartner(&waE2E.Message{ViewOnceMessage: wrap(image("$ rahasia", 100))}), rule, want{}},
		{"image flagged viewOnce", fromPartner(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("$1"), ViewOnce: proto.Bool(true)}}), rule, want{}},
		{"edited message (wrapper)", fromPartner(&waE2E.Message{EditedMessage: wrap(text("$ 30k"))}), rule, want{}},
		{"edited message (protocol, edit attr)", fromPartner(&waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), EditedMessage: text("$ 30k"),
		}}, func(i *types.MessageInfo) { i.Edit = types.EditAttributeMessageEdit }), rule, want{}},
		{"$ text with edit attr", fromPartner(text("$ 30k"), func(i *types.MessageInfo) { i.Edit = types.EditAttributeMessageEdit }), rule, want{}},
		{"deleted message", fromPartner(&waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum()}},
			func(i *types.MessageInfo) { i.Edit = types.EditAttributeSenderRevoke }), rule, want{}},
		{"reaction", fromPartner(&waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("$")}}), rule, want{}},
		{"group with the partner", fromPartner(text("$1"), chat(group), func(i *types.MessageInfo) { i.IsGroup = true }), rule, want{}},
		{"group JID without IsGroup", fromPartner(text("$1"), chat(group)), rule, want{}},
		{"other 1:1 chat", fromPartner(text("$1"), chat(other), func(i *types.MessageInfo) { i.Sender = other }), rule, want{}},
		{"other sender claiming partner chat", fromPartner(text("$1"), func(i *types.MessageInfo) { i.Sender = other }), rule, want{}},
		{"unknown LID chat without alt", fromPartner(text("$1"), chat(types.NewJID("999", types.HiddenUserServer)), func(i *types.MessageInfo) { i.Sender = i.Chat }), noLID, want{}},
		{"own LID chat, partner phone as SenderAlt but sent by me", fromMe(0, text("$1"), chat(myLID), func(i *types.MessageInfo) { i.SenderAlt = partnerPN }), noLID, want{}},
		{"chat with myself", fromMe(0, text("$1"), chat(myPN)), rule, want{}},
		{"status update", fromPartner(text("$1"), chat(types.StatusBroadcastJID)), rule, want{}},
		{"channel", fromPartner(text("$1"), chat(types.NewJID("120363000000000001", types.NewsletterServer))), rule, want{}},
		{"broadcast list", fromPartner(text("$1"), chat(types.NewJID("1700000000", types.BroadcastServer))), rule, want{}},
		{"call log", fromPartner(&waE2E.Message{CallLogMesssage: &waE2E.CallLogMessage{}}), rule, want{}},
		{"bot reply 🤖", fromPartner(text("🤖 tercatat $25000")), rule, want{}},
		{"bot reply 🤖 with $ after it", fromPartner(text("🤖$1")), rule, want{}},
		{"sent by this connector", fromMe(23, text("$ contoh")), rule, want{}},
		{"history message 15 days old", fromPartner(text("$1"), func(i *types.MessageInfo) { i.Timestamp = now.Add(-15 * 24 * time.Hour) }), rule, want{}},
		{"no config yet", fromPartner(text("$1")), Rule{Now: now}, want{}},
		{"nil message", &events.Message{}, rule, want{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, ok := Decide(tt.m, tt.r)
			got := want{ok, out.From, out.Text, out.Image != nil, out.TooBig}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			if ok && (out.ID != tt.m.Info.ID || !out.SentAt.Equal(tt.m.Info.Timestamp) ||
				out.Chat != tt.m.Info.Chat || out.Sender != tt.m.Info.Sender.ToNonAD()) {
				t.Fatalf("id/sentAt/chat/sender not carried: %+v", out)
			}
		})
	}
}
