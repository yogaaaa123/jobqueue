// Package store menyimpan job di SQLite (WAL): API dan worker boleh buka
// file DB yang sama dari proses berbeda (bbolt tidak bisa, lock eksklusif).
// Antrian = baris status 'pending' terurut run_at, id.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure Go, tanpa CGO; driver name "sqlite"
)

// ErrNotFound dipakai saat job dengan id tidak ada.
var ErrNotFound = errors.New("job not found")

// ErrEmpty saat tidak ada job yang siap diambil (antrian kosong / semua belum jadwal).
var ErrEmpty = errors.New("antrian kosong")

// Status job.
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
	StatusDead    = "dead"
)

// jobCols: urutan kolom yang cocok dengan scanJob.
const jobCols = `id, type, payload, status, attempts, max_attempts, run_at, created_at, updated_at, last_error`

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

// Store adalah wrapper SQLite untuk job + antrian.
type Store struct {
	db *sql.DB
}

// Open membuka (atau membuat) file SQLite di path.
// WAL + busy_timeout supaya banyak proses/proseser aman berbagi satu file.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			payload BLOB,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL,
			run_at INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			last_error TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, run_at)`,
	}
	for _, q := range stmts {
		if _, err := db.ExecContext(context.Background(), q); err != nil {
			db.Close()
			return nil, fmt.Errorf("init schema: %w", err)
		}
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

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, run_at, created_at, updated_at, last_error)
		VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, '')`,
		j.ID, j.Type, []byte(j.Payload), j.Status, j.MaxAttempts,
		j.RunAt.UnixNano(), j.CreatedAt.UnixNano(), j.UpdatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// Get mengambil job berdasarkan id. Mengembalikan ErrNotFound jika tidak ada.
func (s *Store) Get(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id)
	return scanJob(row.Scan)
}

// List mengembalikan hingga limit job (terurut run_at), filter status opsional.
func (s *Store) List(ctx context.Context, status string, limit int) ([]*Job, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT ` + jobCols + ` FROM jobs`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY run_at, id LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Claim mengambil satu job siap (status pending, run_at <= now) secara atomik:
// status → running, attempts++. Satu statement UPDATE...RETURNING = atomic.
func (s *Store) Claim(ctx context.Context, now time.Time) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = ?, attempts = attempts + 1, updated_at = ?
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = ? AND run_at <= ?
			ORDER BY run_at, id
			LIMIT 1
		)
		RETURNING `+jobCols,
		StatusRunning, now.UnixNano(), StatusPending, now.UnixNano())
	j, err := scanJob(row.Scan)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrEmpty
	}
	return j, err
}

// Complete menandai job selesai (done).
func (s *Store) Complete(ctx context.Context, id string) error {
	return s.execOne(ctx, `
		UPDATE jobs SET status = ?, last_error = '', updated_at = ? WHERE id = ?`,
		StatusDone, time.Now().UnixNano(), id)
}

// Requeue mengembalikan job ke antrian dengan run_at baru (retry/backoff).
func (s *Store) Requeue(ctx context.Context, j *Job) error {
	if j.RunAt.IsZero() {
		j.RunAt = time.Now().UTC()
	}
	now := time.Now().UTC()
	if err := s.execOne(ctx, `
		UPDATE jobs SET status = ?, run_at = ?, last_error = ?, updated_at = ? WHERE id = ?`,
		StatusPending, j.RunAt.UnixNano(), j.LastError, now.UnixNano(), j.ID); err != nil {
		return err
	}
	j.Status = StatusPending
	j.UpdatedAt = now
	return nil
}

// MarkDead menandai job gagal permanen (habis max_attempts).
func (s *Store) MarkDead(ctx context.Context, id, lastError string) error {
	return s.execOne(ctx, `
		UPDATE jobs SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`,
		StatusDead, lastError, time.Now().UnixNano(), id)
}

// execOne: UPDATE yang wajih kena 1 baris, else ErrNotFound.
func (s *Store) execOne(ctx context.Context, query string, args ...any) error {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// scanJob: kolom jobCols → Job (waktu unix nano → time.Time).
func scanJob(scan func(dest ...any) error) (*Job, error) {
	var (
		j               Job
		payload         []byte
		runAt, cre, upd int64
	)
	err := scan(&j.ID, &j.Type, &payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&runAt, &cre, &upd, &j.LastError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	j.Payload = payload
	j.RunAt = time.Unix(0, runAt).UTC()
	j.CreatedAt = time.Unix(0, cre).UTC()
	j.UpdatedAt = time.Unix(0, upd).UTC()
	return &j, nil
}

// newID: 8 byte unix nano (big-endian, sortable) + 8 byte acak, hex 32 char.
func newID(now time.Time) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(now.UnixNano()))
	_, _ = rand.Read(b[8:])
	return hex.EncodeToString(b[:])
}
