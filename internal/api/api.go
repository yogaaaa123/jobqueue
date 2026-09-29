// Package api: HTTP API standar (net/http) buat submit dan cek job.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/yogaaaa123/jobqueue/internal/store"
)

const maxBody = 1 << 20 // 1 MiB

// Server pegang referensi store.
type Server struct {
	store *store.Store
}

// New membuat server API di atas store.
func New(s *store.Store) *Server { return &Server{store: s} }

// Handler lengkapi routing (pola Go 1.22: "METHOD /path/{param}").
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

type createJobReq struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       *time.Time      `json:"run_at"` // RFC3339, kosong = sekarang
}

// createJob: POST /jobs → 202 {job}. Field lain diabaikan, selalu job baru.
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	var req createJobReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "body bukan JSON valid: "+err.Error())
		return
	}
	if req.Type == "" {
		writeErr(w, http.StatusBadRequest, "type wajib diisi")
		return
	}
	if req.MaxAttempts < 0 || req.MaxAttempts > 10 {
		writeErr(w, http.StatusBadRequest, "max_attempts harus 0 (default) atau 1..10")
		return
	}
	j := &store.Job{Type: req.Type, Payload: req.Payload, MaxAttempts: req.MaxAttempts}
	if req.RunAt != nil {
		j.RunAt = req.RunAt.UTC()
	}
	if err := s.store.Create(r.Context(), j); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, j)
}

// getJob: GET /jobs/{id} → 200 job | 404.
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "job tidak ditemukan")
	case err != nil:
		writeErr(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, j)
	}
}

// listJobs: GET /jobs?status=pending&limit=50 → 200 [job]. Limit default 100, cap 1000.
func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 1000 {
			writeErr(w, http.StatusBadRequest, "limit harus 1..1000")
			return
		}
		limit = n
	}
	jobs, err := s.store.List(r.Context(), q.Get("status"), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if jobs == nil {
		jobs = []*store.Job{} // JSON [] bukan null
	}
	writeJSON(w, http.StatusOK, jobs)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
