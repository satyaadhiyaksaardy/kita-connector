// Package outbox keeps forwarded `message` frames on disk until the brain
// answers `received`, so nothing is lost across disconnects or restarts.
package outbox

import (
	"database/sql"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `CREATE TABLE IF NOT EXISTS outbox (
  msg_id      TEXT PRIMARY KEY,
  frame       BLOB NOT NULL,      -- the full message frame, image included
  created_at  INTEGER NOT NULL,   -- unix ms
  attempts    INTEGER NOT NULL DEFAULT 0,
  next_at     INTEGER NOT NULL,   -- unix ms, next resend time
  media       BLOB,               -- set while an image is still to be downloaded: its media reference; never due
  chat        TEXT,               -- where the brain's react/reply go (JIDs): restored after a restart
  sender      TEXT
)`

type Outbox struct {
	db *sql.DB
	// Resend after Base, doubling per attempt up to Max; drop after TTL.
	// Fields only so tests can shrink them (PROTOCOL.md → Resending).
	Base, Max, TTL time.Duration
}

type Item struct {
	ID       string
	Frame    []byte
	Attempts int
	Media    []byte // Held only
	Chat     string // Queued and Held
	Sender   string
}

func Open(path string) (*Outbox, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one writer; avoids SQLITE_BUSY inside the process
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	_ = os.Chmod(path, 0o600)
	return &Outbox{db: db, Base: time.Minute, Max: 30 * time.Minute, TTL: 24 * time.Hour}, nil
}

func (o *Outbox) Close() error { return o.db.Close() }

// Put stores a frame before it is first sent, with the chat and sender its
// react/reply need. A repeated id is ignored.
func (o *Outbox) Put(id string, frame []byte, chat, sender string, now time.Time) error {
	_, err := o.db.Exec(`INSERT OR IGNORE INTO outbox (msg_id, frame, created_at, next_at, chat, sender) VALUES (?, ?, ?, ?, ?, ?)`,
		id, frame, now.UnixMilli(), now.Add(o.Base).UnixMilli(), chat, sender)
	return err
}

// Queued lists every message still waiting for `received`, oldest first.
func (o *Outbox) Queued() ([]Item, error) {
	return o.list(`SELECT msg_id, frame, media, IFNULL(chat, ''), IFNULL(sender, '') FROM outbox ORDER BY created_at`)
}

func (o *Outbox) list(query string) ([]Item, error) {
	rows, err := o.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.Frame, &it.Media, &it.Chat, &it.Sender); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// Hold stores an image message before its image is downloaded: the frame
// without the data, and the media reference to download it with. It is not
// sent until Release. A repeated id is ignored.
func (o *Outbox) Hold(id string, frame, media []byte, chat, sender string, now time.Time) error {
	_, err := o.db.Exec(`INSERT OR IGNORE INTO outbox (msg_id, frame, created_at, next_at, media, chat, sender) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, frame, now.UnixMilli(), now.Add(o.Base).UnixMilli(), media, chat, sender)
	return err
}

// Held lists images still to be downloaded, oldest first.
func (o *Outbox) Held() ([]Item, error) {
	return o.list(`SELECT msg_id, frame, media, IFNULL(chat, ''), IFNULL(sender, '') FROM outbox WHERE media IS NOT NULL ORDER BY created_at`)
}

// Release replaces a held row's frame with the one to send and makes it a
// normal row (first resend after Base).
func (o *Outbox) Release(id string, frame []byte, now time.Time) error {
	_, err := o.db.Exec(`UPDATE outbox SET frame = ?, media = NULL, next_at = ? WHERE msg_id = ? AND media IS NOT NULL`,
		frame, now.Add(o.Base).UnixMilli(), id)
	return err
}

// Ack drops a message the brain has `received`.
func (o *Outbox) Ack(id string) error {
	_, err := o.db.Exec(`DELETE FROM outbox WHERE msg_id = ?`, id)
	return err
}

// Due lists messages whose resend time is at or before t, oldest first.
// Pass a far-future t to get everything (resend after reconnect).
func (o *Outbox) Due(t time.Time) ([]Item, error) {
	rows, err := o.db.Query(`SELECT msg_id, frame, attempts FROM outbox WHERE next_at <= ? AND media IS NULL ORDER BY created_at`, t.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.Frame, &it.Attempts); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// Sent records a resend and pushes next_at out: Base·2^attempts, at most Max.
func (o *Outbox) Sent(id string, now time.Time) error {
	var attempts int
	if err := o.db.QueryRow(`UPDATE outbox SET attempts = attempts + 1 WHERE msg_id = ? RETURNING attempts`, id).Scan(&attempts); err != nil {
		if err == sql.ErrNoRows {
			return nil // acked meanwhile
		}
		return err
	}
	_, err := o.db.Exec(`UPDATE outbox SET next_at = ? WHERE msg_id = ?`, now.Add(o.backoff(attempts)).UnixMilli(), id)
	return err
}

func (o *Outbox) backoff(attempts int) time.Duration {
	d := o.Base
	for i := 0; i < attempts && d < o.Max; i++ {
		d *= 2
	}
	return min(d, o.Max)
}

// Prune deletes messages older than TTL and returns their ids for the log.
func (o *Outbox) Prune(now time.Time) ([]string, error) {
	rows, err := o.db.Query(`DELETE FROM outbox WHERE created_at < ? RETURNING msg_id`, now.Add(-o.TTL).UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (o *Outbox) Len() (n int, err error) {
	err = o.db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n)
	return n, err
}
