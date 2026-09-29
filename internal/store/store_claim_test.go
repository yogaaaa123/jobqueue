package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestClaimEmpty: antrian kosong → ErrEmpty.
func TestClaimEmpty(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Claim(context.Background(), time.Now(), time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
}

// TestClaimOrderAndSkipFuture: job paling awal duluan, run_at masa depan dilewati.
func TestClaimOrderAndSkipFuture(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	late := &Job{Type: "echo", RunAt: now.Add(time.Hour)}
	early := &Job{Type: "echo", RunAt: now.Add(-time.Second)}
	if err := s.Create(ctx, late); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, early); err != nil {
		t.Fatal(err)
	}

	got, err := s.Claim(ctx, now, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got.ID != early.ID {
		t.Fatalf("claim %s, want job paling awal %s", got.ID, early.ID)
	}
	if got.Status != StatusRunning || got.Attempts != 1 {
		t.Fatalf("claim harus running + attempts=1: %+v", got)
	}

	// Sisa: job future belum jadwal → ErrEmpty.
	if _, err := s.Claim(ctx, now, time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	// Setelah jadwalnya tiba, baru bisa di-claim.
	if _, err := s.Claim(ctx, now.Add(2*time.Hour), time.Minute); err != nil {
		t.Fatalf("claim future setelah jadwal: %v", err)
	}
}

// TestCompleteRequeueDead: siklus ack, retry, dead letter.
func TestCompleteRequeueDead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Complete → tidak bisa di-claim lagi.
	j1 := &Job{Type: "echo"}
	s.Create(ctx, j1)
	c, err := s.Claim(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("claim j1: %v", err)
	}
	if err := s.Complete(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, j1.ID)
	if got.Status != StatusDone || got.LastError != "" {
		t.Fatalf("complete: %+v", got)
	}
	if _, err := s.Claim(ctx, now.Add(time.Minute), time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("job done tidak boleh di-claim lagi, got %v", err)
	}

	// Requeue → pending lagi, bisa di-claim sesuai run_at.
	j2 := &Job{Type: "echo"}
	s.Create(ctx, j2)
	c2, err := s.Claim(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("claim j2: %v", err)
	}
	c2.RunAt = time.Now().UTC().Add(time.Minute)
	c2.LastError = "gagal sementara"
	if err := s.Requeue(ctx, c2); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, j2.ID)
	if got.Status != StatusPending || got.LastError != "gagal sementara" {
		t.Fatalf("requeue: %+v", got)
	}
	if _, err := s.Claim(ctx, time.Now().UTC(), time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("belum jadwal, got %v", err)
	}
	reclaimed, err := s.Claim(ctx, time.Now().UTC().Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if reclaimed.Attempts != 2 {
		t.Fatalf("attempts harus 2 setelah reclaim, got %d", reclaimed.Attempts)
	}

	// MarkDead → status dead, last_error tersimpan.
	if err := s.MarkDead(ctx, j2.ID, "habis retry"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, j2.ID)
	if got.Status != StatusDead || got.LastError != "habis retry" {
		t.Fatalf("markdead: %+v", got)
	}
}
