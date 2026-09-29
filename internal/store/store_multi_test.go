package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestMultiProcessAccess: dua Store (dua koneksi) ke file yang sama —
// yang membuat di A, yang claim/complete di B, yang baca hasil di A.
// Ini alasan bbolt diganti SQLite: bbolt lock eksklusif, cuma 1 proses.
func TestMultiProcessAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	a, err := Open(path)
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatalf("open b (proses kedua): %v", err)
	}
	defer b.Close()
	ctx := context.Background()

	// Verifikasi WAL aktif.
	var mode string
	if err := a.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q (err %v), want wal", mode, err)
	}

	j := &Job{Type: "echo"}
	if err := a.Create(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, err := b.Claim(ctx, time.Now())
	if err != nil {
		t.Fatalf("claim dari koneksi kedua: %v", err)
	}
	if got.ID != j.ID {
		t.Fatalf("claim %s, want %s", got.ID, j.ID)
	}
	if err := b.Complete(ctx, got.ID); err != nil {
		t.Fatal(err)
	}
	back, err := a.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if back.Status != StatusDone {
		t.Fatalf("status = %s, want done", back.Status)
	}
}

// TestConcurrentClaimExactlyOnce: 8 goroutine di 2 Store berebut 20 job —
// tiap job harus di-claim persis sekali (atomik, tanpa double claim).
func TestConcurrentClaimExactlyOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()

	const n = 20
	for i := 0; i < n; i++ {
		if err := a.Create(ctx, &Job{Type: "echo"}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for _, st := range []*Store{a, b} {
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(st *Store) {
				defer wg.Done()
				for {
					j, err := st.Claim(ctx, time.Now())
					if errors.Is(err, ErrEmpty) {
						return
					}
					if err != nil {
						t.Errorf("claim: %v", err)
						return
					}
					mu.Lock()
					seen[j.ID]++
					mu.Unlock()
				}
			}(st)
		}
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("claim %d job unik, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %s di-claim %d kali, want 1", id, c)
		}
	}
}
