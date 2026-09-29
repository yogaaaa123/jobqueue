package worker

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yogaaaa123/jobqueue/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// waitStatus poll sampai job mencapai status yang diinginkan.
func waitStatus(t *testing.T, s *store.Store, id, want string) *store.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err == nil && j.Status == want {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := s.Get(context.Background(), id)
	t.Fatalf("job %s tidak mencapai status %q (sekarang: %+v)", id, want, j)
	return nil
}

// TestPoolProcessesJobs: 10 job echo, 4 worker, semua done, Run bersih saat batal.
func TestPoolProcessesJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var handled atomic.Int64
	p := New(s, 4)
	p.PollEvery = 5 * time.Millisecond
	p.Register("echo", func(ctx context.Context, j *store.Job) error {
		handled.Add(1)
		return nil
	})

	var ids []string
	for i := 0; i < 10; i++ {
		j := &store.Job{Type: "echo"}
		if err := s.Create(ctx, j); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(runCtx) }()

	for _, id := range ids {
		waitStatus(t, s, id, store.StatusDone)
	}
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run harus return context.Canceled, got %v", err)
	}
	if handled.Load() != 10 {
		t.Fatalf("handler dipanggil %d kali, want 10", handled.Load())
	}
}

// TestRetryThenDead: handler selalu gagal → retry sesuai max_attempts → dead.
func TestRetryThenDead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	p := New(s, 1)
	p.PollEvery = 5 * time.Millisecond
	p.Backoff = func(int) time.Duration { return 5 * time.Millisecond }
	p.Register("boom", func(ctx context.Context, j *store.Job) error {
		return errors.New("meledak")
	})

	j := &store.Job{Type: "boom", MaxAttempts: 3}
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	go p.Run(runCtx)

	got := waitStatus(t, s, j.ID, store.StatusDead)
	cancel()
	if got.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", got.Attempts)
	}
	if got.LastError != "meledak" {
		t.Fatalf("last_error = %q, want %q", got.LastError, "meledak")
	}
}

// TestUnknownTypeDead: tipe tanpa handler → gagal → dead di percobaan terakhir.
func TestUnknownTypeDead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	p := New(s, 1)
	p.PollEvery = 5 * time.Millisecond
	p.Backoff = func(int) time.Duration { return 5 * time.Millisecond }

	j := &store.Job{Type: "tidak-dikenal", MaxAttempts: 1}
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	go p.Run(runCtx)

	got := waitStatus(t, s, j.ID, store.StatusDead)
	cancel()
	if got.LastError == "" {
		t.Fatal("last_error harus berisi pesan handler tidak ada")
	}
}

func TestBackoff(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, time.Second},
		{1, 2 * time.Second},
		{3, 8 * time.Second},
		{10, maxBackoff},  // 1024s > 5m → cap
		{100, maxBackoff}, // ekstrem, tidak overflow
	}
	for _, c := range cases {
		if got := backoff(c.attempts); got != c.want {
			t.Errorf("backoff(%d) = %v, want %v", c.attempts, got, c.want)
		}
	}
}
