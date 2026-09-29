package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/yogaaaa123/jobqueue/internal/store"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st), st
}

func doJSON(t *testing.T, h http.Handler, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd *bytes.Reader
	if body == "" {
		rd = bytes.NewReader(nil)
	} else {
		rd = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	s, _ := newTestServer(t)
	rec := doJSON(t, s.Handler(), "GET", "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestCreateJobOK(t *testing.T) {
	s, st := newTestServer(t)
	rec := doJSON(t, s.Handler(), "POST", "/jobs", `{"type":"echo","payload":{"msg":"halo"}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body)
	}
	var j store.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if j.ID == "" || j.Status != store.StatusPending || j.Type != "echo" {
		t.Fatalf("job aneh: %+v", j)
	}
	// Tersimpan dan bisa diambil.
	if _, err := st.Get(t.Context(), j.ID); err != nil {
		t.Fatalf("get setelah create: %v", err)
	}
}

func TestCreateJobValidation(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler()
	cases := []struct {
		name string
		body string
	}{
		{"tanpa type", `{"payload":{}}`},
		{"bukan json", `{"type":`},
		{"max_attempts gila", `{"type":"echo","max_attempts":99}`},
		{"bukan object", `["echo"]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := doJSON(t, h, "POST", "/jobs", c.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler()

	rec := doJSON(t, h, "POST", "/jobs", `{"type":"echo"}`)
	var j store.Job
	json.Unmarshal(rec.Body.Bytes(), &j)

	rec = doJSON(t, h, "GET", "/jobs/"+j.ID, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get = %d, want 200", rec.Code)
	}

	rec = doJSON(t, h, "GET", "/jobs/tidak-ada", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get unknown = %d, want 404", rec.Code)
	}
}

func TestListJobs(t *testing.T) {
	s, st := newTestServer(t)
	h := s.Handler()

	// Kosong → [] bukan null.
	rec := doJSON(t, h, "GET", "/jobs", "")
	if rec.Code != http.StatusOK || !bytes.Equal(bytes.TrimSpace(rec.Body.Bytes()), []byte("[]")) {
		t.Fatalf("list kosong = %d %s, want 200 []", rec.Code, rec.Body)
	}

	st.Create(t.Context(), &store.Job{Type: "echo"})
	st.Create(t.Context(), &store.Job{Type: "echo", Status: store.StatusDone})

	rec = doJSON(t, h, "GET", "/jobs?status=done", "")
	var jobs []store.Job
	json.Unmarshal(rec.Body.Bytes(), &jobs)
	if len(jobs) != 1 || jobs[0].Status != store.StatusDone {
		t.Fatalf("filter done = %s", rec.Body)
	}

	// Limit invalid → 400.
	if rec := doJSON(t, h, "GET", "/jobs?limit=99999", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("limit gila = %d, want 400", rec.Code)
	}
}
