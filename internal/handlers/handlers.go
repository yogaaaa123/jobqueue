// Package handlers: handler bawaan per tipe job.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/yogaaaa123/jobqueue/internal/store"
)

// Echo log payload dan selesai. Buat test/connectivity.
func Echo(ctx context.Context, j *store.Job) error {
	slog.Info("echo", "id", j.ID, "payload", string(j.Payload))
	return nil
}

// Sleep tidur sesuai payload {"ms": N}. Respek ctx (batal saat shutdown).
func Sleep(ctx context.Context, j *store.Job) error {
	var p struct {
		MS int `json:"ms"`
	}
	if len(j.Payload) > 0 {
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return fmt.Errorf("payload sleep: %w", err)
		}
	}
	if p.MS <= 0 {
		p.MS = 100
	}
	t := time.NewTimer(time.Duration(p.MS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
