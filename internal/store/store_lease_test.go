package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLeaseReclaimRequeues: worker "mati" (claim tanpa heartbeat) → lease lewat
// → Reclaim mengembalikan job ke pending, attempts tidak di-reset.
func TestLeaseReclaimRequeues(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	j := &Job{Type: "echo", MaxAttempts: 3}
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := s.Claim(ctx, now, 20*time.Millisecond); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Lease belum lewat → tidak di-reclaim.
	if n, err := s.Reclaim(ctx, now.Add(10*time.Millisecond)); err != nil || n != 0 {
		t.Fatalf("reclaim premature = %d, %v; want 0, nil", n, err)
	}
	// Lease lewat → di-reclaim, kembali pending.
	if n, err := s.Reclaim(ctx, now.Add(30*time.Millisecond)); err != nil || n != 1 {
		t.Fatalf("reclaim = %d, %v; want 1, nil", n, err)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != StatusPending || got.Attempts != 1 {
		t.Fatalf("setelah reclaim: %+v", got)
	}
	// Bisa di-claim lagi, attempts naik lagi (pakai waktu sintetis konsisten
	// dengan run_at hasil reclaim, tanpa sleep beneran).
	if _, err := s.Claim(ctx, now.Add(40*time.Millisecond), time.Minute); err != nil {
		t.Fatalf("reclaim setelah reclaim: %v", err)
	}
	got, _ = s.Get(ctx, j.ID)
	if got.Attempts != 2 || got.Status != StatusRunning {
		t.Fatalf("re-claim: %+v", got)
	}
}

// TestReclaimDeadWhenAttemptsExhausted: lease mati tapi attempts sudah
// max_attempts → dead, bukan requeue (hindari loop crash tanpa batas).
func TestReclaimDeadWhenAttemptsExhausted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	j := &Job{Type: "echo", MaxAttempts: 1}
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := s.Claim(ctx, now, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Reclaim(ctx, now.Add(20*time.Millisecond)); err != nil || n != 1 {
		t.Fatalf("reclaim = %d, %v", n, err)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != StatusDead {
		t.Fatalf("status = %s, want dead", got.Status)
	}
	if !strings.Contains(got.LastError, "lease expired") {
		t.Fatalf("last_error = %q, want pesan lease expired", got.LastError)
	}
}

// TestHeartbeatPreventsReclaim: lease asli lewat, tapi heartbeat memperpanjang
// → Reclaim tidak menyentuh job.
func TestHeartbeatPreventsReclaim(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	j := &Job{Type: "echo"}
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := s.Claim(ctx, now, 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Perpanjang lease jauh ke depan.
	if err := s.Heartbeat(ctx, j.ID, now.Add(time.Second)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if n, err := s.Reclaim(ctx, now.Add(100*time.Millisecond)); err != nil || n != 0 {
		t.Fatalf("reclaim = %d, %v; want 0, nil", n, err)
	}
	// Heartbeat job yang sudah bukan running → ErrNotFound (no-op aman).
	if err := s.Complete(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, j.ID, time.Now()); err != ErrNotFound {
		t.Fatalf("heartbeat setelah done = %v, want ErrNotFound", err)
	}
}

// TestMigrationLeaseColumn: buka DB dua kali → kolom lease_until tidak dobel,
// claim tetap jalan (migrasi idempoten).
func TestMigrationLeaseColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mig.db")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Create(ctx, &Job{Type: "echo"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := Open(path) // kena jalur "duplicate column" → diabaikan
	if err != nil {
		t.Fatalf("buka ulang: %v", err)
	}
	defer b.Close()
	if _, err := b.Claim(ctx, time.Now(), time.Minute); err != nil {
		t.Fatalf("claim setelah migrasi: %v", err)
	}
}
