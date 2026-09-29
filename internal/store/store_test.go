package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreateAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	j := &Job{Type: "echo", Payload: json.RawMessage(`{"msg":"halo"}`)}
	if err := s.Create(ctx, j); err != nil {
		t.Fatalf("create: %v", err)
	}
	if j.ID == "" || j.Status != StatusPending || j.MaxAttempts != 3 {
		t.Fatalf("default tidak diterapkan: %+v", j)
	}

	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Type != "echo" || string(got.Payload) != `{"msg":"halo"}` {
		t.Fatalf("job tidak cocok: %+v", got)
	}
}

func TestGetNotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Get(context.Background(), "tidak-ada"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCreateValidation(t *testing.T) {
	s := newTestStore(t)
	if err := s.Create(context.Background(), &Job{}); err == nil {
		t.Fatal("job tanpa type harus gagal")
	}
}

func TestListFilterByStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, st := range []string{StatusPending, StatusDone, StatusPending} {
		if err := s.Create(ctx, &Job{Type: "echo", Status: st}); err != nil {
			t.Fatalf("create: %v", err)
		}
	}
	all, _ := s.List(ctx, "", 10)
	if len(all) != 3 {
		t.Fatalf("want 3 job, got %d", len(all))
	}
	done, _ := s.List(ctx, StatusDone, 10)
	if len(done) != 1 || done[0].Status != StatusDone {
		t.Fatalf("filter status gagal: %+v", done)
	}
}
