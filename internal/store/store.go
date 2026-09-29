// Package store menyimpan job di bbolt: bucket "jobs" (id → JSON)
// dan bucket "pending" (run_at||id → id) sebagai antrian FIFO by run_at.
package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrNotFound dipakai saat job dengan id tidak ada.
var ErrNotFound = errors.New("job not found")

// Status job.
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
	StatusDead    = "dead"
)

var (
	bucketJobs    = []byte("jobs")
	bucketPending = []byte("pending")
)

// Job adalah unit kerja di antrian.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	LastError   string          `json:"last_error,omitempty"`
}

// Store adalah wrapper bbolt untuk job + antrian pending.
type Store struct {
	db *bolt.DB
}

// Open membuka (atau membuat) file bbolt di path.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketJobs); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(bucketPending)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init buckets: %w", err)
	}
	return &Store{db: db}, nil
}

// Close menutup database.
func (s *Store) Close() error { return s.db.Close() }

// Create menyimpan job baru dan memasukkannya ke antrian pending.
// Field kosong diisi default: ID acak, Status pending, MaxAttempts 3, RunAt now.
func (s *Store) Create(ctx context.Context, j *Job) error {
	if j.Type == "" {
		return errors.New("job type wajib diisi")
	}
	now := time.Now().UTC()
	if j.ID == "" {
		j.ID = newID(now)
	}
	if j.Status == "" {
		j.Status = StatusPending
	}
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = 3
	}
	if j.RunAt.IsZero() {
		j.RunAt = now
	}
	j.CreatedAt = now
	j.UpdatedAt = now

	return s.db.Update(func(tx *bolt.Tx) error {
		raw, err := json.Marshal(j)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketJobs).Put([]byte(j.ID), raw); err != nil {
			return err
		}
		return tx.Bucket(bucketPending).Put(pendingKey(j), []byte(j.ID))
	})
}

// Get mengambil job berdasarkan id. Mengembalikan ErrNotFound jika tidak ada.
func (s *Store) Get(ctx context.Context, id string) (*Job, error) {
	var j Job
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketJobs).Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &j)
	})
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// List mengembalikan hingga limit job (urutan id, ≈ waktubuat), filter status opsional.
func (s *Store) List(ctx context.Context, status string, limit int) ([]*Job, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []*Job
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketJobs).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var j Job
			if err := json.Unmarshal(v, &j); err != nil {
				return err
			}
			if status != "" && j.Status != status {
				continue
			}
			out = append(out, &j)
			if len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	return out, err
}

// newID: 8 byte unix nano (big-endian, sortable) + 8 byte acak, hex 32 char.
func newID(now time.Time) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(now.UnixNano()))
	_, _ = rand.Read(b[8:])
	return hex.EncodeToString(b[:])
}

// pendingKey: run_at big-endian supaya antrian terurut waktu eksekusi.
func pendingKey(j *Job) []byte {
	k := make([]byte, 8+len(j.ID))
	binary.BigEndian.PutUint64(k[:8], uint64(j.RunAt.UnixNano()))
	copy(k[8:], j.ID)
	return k
}
