package outbox

import (
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func ids(t *testing.T, o *Outbox, at time.Time) []string {
	t.Helper()
	items, err := o.Due(at)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, it := range items {
		out = append(out, it.ID)
	}
	return out
}

func TestOutbox(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(1_800_000_000, 0)

	// put: not due until Base has passed; duplicate ids are ignored
	must(t, o.Put("A", []byte(`{"type":"message","id":"A"}`), "chat@s.whatsapp.net", "sender@s.whatsapp.net", t0))
	must(t, o.Put("B", []byte(`{"id":"B"}`), "", "", t0.Add(time.Second)))
	must(t, o.Put("A", []byte(`other`), "", "", t0.Add(2*time.Second)))
	if n, _ := o.Len(); n != 2 {
		t.Fatalf("len = %d, want 2", n)
	}
	if got := ids(t, o, t0.Add(59*time.Second)); len(got) != 0 {
		t.Fatalf("due before 60 s: %v", got)
	}
	if got := ids(t, o, t0.Add(time.Minute)); !slices.Equal(got, []string{"A"}) {
		t.Fatalf("due at 60 s: %v", got)
	}

	// backoff: 60 s · 2^attempts, capped at 30 min
	for i, want := range []time.Duration{2, 4, 8, 16, 30, 30} {
		at := t0.Add(time.Duration(i+1) * time.Hour)
		must(t, o.Sent("A", at))
		if got := ids(t, o, at.Add(want*time.Minute-time.Millisecond)); slices.Contains(got, "A") {
			t.Fatalf("attempt %d: due too early", i+1)
		}
		if got := ids(t, o, at.Add(want*time.Minute)); !slices.Contains(got, "A") {
			t.Fatalf("attempt %d: not due after %v", i+1, want*time.Minute)
		}
	}
	must(t, o.Sent("gone", t0)) // acked meanwhile: no error

	// ack
	must(t, o.Ack("B"))
	if got := ids(t, o, t0.Add(100*time.Hour)); !slices.Equal(got, []string{"A"}) {
		t.Fatalf("after ack: %v", got)
	}

	// survives a restart, frame intact
	must(t, o.Close())
	o, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	items, _ := o.Due(t0.Add(100 * time.Hour))
	if len(items) != 1 || string(items[0].Frame) != `{"type":"message","id":"A"}` || items[0].Attempts != 6 {
		t.Fatalf("after reopen: %+v", items)
	}
	// the react/reply context is kept with it
	if q, err := o.Queued(); err != nil || len(q) != 1 || q[0].Chat != "chat@s.whatsapp.net" || q[0].Sender != "sender@s.whatsapp.net" {
		t.Fatalf("queued after reopen: %+v, %v", q, err)
	}

	// 24 h prune by created_at
	must(t, o.Put("C", []byte(`{}`), "", "", t0.Add(23*time.Hour)))
	pruned, err := o.Prune(t0.Add(24*time.Hour + time.Millisecond))
	if err != nil || !slices.Equal(pruned, []string{"A"}) {
		t.Fatalf("pruned %v, %v", pruned, err)
	}
	if n, _ := o.Len(); n != 1 {
		t.Fatalf("len after prune = %d, want 1", n)
	}
}

// An image is held (frame without data + media reference) until it is
// downloaded: never due, kept across a restart, then released as a normal row.
func TestHoldRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(1_800_000_000, 0)
	must(t, o.Hold("I", []byte(`{"id":"I"}`), []byte("ref"), "c", "s", t0))
	must(t, o.Hold("I", []byte(`other`), []byte("other"), "", "", t0)) // repeat ignored
	if got := ids(t, o, t0.Add(100*time.Hour)); len(got) != 0 {
		t.Fatalf("held image is due: %v", got)
	}
	must(t, o.Close())
	if o, err = Open(path); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	held, err := o.Held()
	if err != nil || len(held) != 1 || held[0].ID != "I" || string(held[0].Frame) != `{"id":"I"}` || string(held[0].Media) != "ref" || held[0].Chat != "c" {
		t.Fatalf("held after reopen: %+v, %v", held, err)
	}
	must(t, o.Release("I", []byte(`{"id":"I","data":"x"}`), t0))
	if held, _ := o.Held(); len(held) != 0 {
		t.Fatalf("still held: %+v", held)
	}
	items, _ := o.Due(t0.Add(time.Minute))
	if len(items) != 1 || string(items[0].Frame) != `{"id":"I","data":"x"}` {
		t.Fatalf("released row: %+v", items)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
