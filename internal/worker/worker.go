// Package worker: pool goroutine yang poll antrian, jalankan handler,
// lalu complete / retry dengan backoff / dead letter. Resilience:
// setiap claim punya lease (visibility timeout) yang diperpanjang heartbeat;
// lease lewat (worker crash) → job di-reclaim jadi pending lagi.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yogaaaa123/jobqueue/internal/store"
)

// Handler memproses satu job. Error → job di-retry sampai max_attempts.
type Handler func(ctx context.Context, job *store.Job) error

// Pool konfigurasi worker. Field terbuka supaya mudah di-tune di test.
type Pool struct {
	Store        *store.Store
	Workers      int
	PollEvery    time.Duration // idle polling interval
	Visibility   time.Duration // lease job running tanpa heartbeat; default 60s
	ReclaimEvery time.Duration // interval cek lease mati; default Visibility/3
	Backoff      func(attempts int) time.Duration

	mu       sync.RWMutex
	handlers map[string]Handler
}

// New membuat pool dengan default: 4 worker, poll 200ms, visibility 60s,
// backoff eksponensial.
func New(s *store.Store, workers int) *Pool {
	if workers <= 0 {
		workers = 4
	}
	return &Pool{
		Store:      s,
		Workers:    workers,
		PollEvery:  200 * time.Millisecond,
		Visibility: 60 * time.Second,
		Backoff:    backoff,
		handlers:   map[string]Handler{},
	}
}

// Register mendaftarkan handler untuk tipe job. Panik bila tipe duplikat/kosong.
func (p *Pool) Register(jobType string, h Handler) {
	if jobType == "" || h == nil {
		panic("worker: tipe job dan handler wajib")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.handlers[jobType]; ok {
		panic(fmt.Sprintf("worker: handler %q sudah terdaftar", jobType))
	}
	p.handlers[jobType] = h
}

// Run menjalankan semua worker + reclaimer sampai ctx batal, lalu tunggu
// job in-flight selesai.
func (p *Pool) Run(ctx context.Context) error {
	if p.Visibility <= 0 {
		p.Visibility = 60 * time.Second
	}
	reclaimEvery := p.ReclaimEvery
	if reclaimEvery <= 0 {
		reclaimEvery = max(p.Visibility/3, 10*time.Millisecond)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reclaimLoop(ctx, reclaimEvery)
	}()
	for i := 0; i < p.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (p *Pool) loop(ctx context.Context) {
	for ctx.Err() == nil {
		job, err := p.Store.Claim(ctx, time.Now(), p.Visibility)
		switch {
		case errors.Is(err, store.ErrEmpty):
			if !p.sleep(ctx, p.PollEvery) {
				return
			}
			continue
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			slog.Error("claim gagal", "err", err)
			if !p.sleep(ctx, p.PollEvery) {
				return
			}
			continue
		}
		p.process(ctx, job)
	}
}

// process: heartbeat selama handler jalan, lalu ack/retry/dead.
func (p *Pool) process(ctx context.Context, job *store.Job) {
	stop := make(chan struct{})
	go p.heartbeat(ctx, job.ID, stop)
	runErr := p.runHandler(ctx, job)
	close(stop)

	var err error
	switch {
	case runErr == nil:
		err = p.Store.Complete(ctx, job.ID)
	case job.Attempts >= job.MaxAttempts:
		slog.Error("job dead", "id", job.ID, "type", job.Type, "attempts", job.Attempts, "err", runErr)
		err = p.Store.MarkDead(ctx, job.ID, runErr.Error())
	default:
		job.LastError = runErr.Error()
		job.RunAt = time.Now().UTC().Add(p.Backoff(job.Attempts))
		slog.Warn("job retry", "id", job.ID, "type", job.Type, "attempt", job.Attempts, "next", job.RunAt, "err", runErr)
		err = p.Store.Requeue(ctx, job)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("update job gagal", "id", job.ID, "err", err)
	}
}

// heartbeat: perpanjang lease selama handler berjalan. Kalau job sudah
// di-reclaim proses lain, Heartbeat no-op dan loop tetap jalan (Complete
// tetap dipanggil — at-least-once).
func (p *Pool) heartbeat(ctx context.Context, id string, stop <-chan struct{}) {
	every := max(p.Visibility/3, 10*time.Millisecond)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			_ = p.Store.Heartbeat(ctx, id, time.Now().Add(p.Visibility))
		}
	}
}

// runHandler ambil handler + recover panic supaya satu handler nakal tidak
// membunuh seluruh proses worker.
func (p *Pool) runHandler(ctx context.Context, job *store.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	p.mu.RLock()
	h, ok := p.handlers[job.Type]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("tidak ada handler untuk tipe %q", job.Type)
	}
	return h(ctx, job)
}

// reclaimLoop: job running yang lease-nya lewat (worker mati) → pending lagi.
func (p *Pool) reclaimLoop(ctx context.Context, every time.Duration) {
	for {
		if !p.sleep(ctx, every) {
			return
		}
		n, err := p.Store.Reclaim(ctx, time.Now())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("reclaim gagal", "err", err)
			continue
		}
		if n > 0 {
			slog.Warn("reclaim job lease expired", "count", n)
		}
	}
}

// sleep tunggu d, false kalau ctx batal duluan.
func (p *Pool) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

const maxBackoff = 5 * time.Minute

// backoff: 2^attempts detik, cap 5 menit.
func backoff(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 16 {
		return maxBackoff
	}
	d := time.Duration(1<<attempts) * time.Second
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
