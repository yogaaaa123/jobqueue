package store

import (
	"path/filepath"
	"testing"
)

func TestPragmasApplied(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var busy, sync int
	var mode string
	if err := s.db.QueryRow(`PRAGMA busy_timeout`).Scan(&busy); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA synchronous`).Scan(&sync); err != nil {
		t.Fatal(err)
	}
	t.Logf("busy_timeout=%d journal_mode=%s synchronous=%d", busy, mode, sync)
	if busy != 5000 {
		t.Errorf("busy_timeout = %d, want 5000", busy)
	}
}
