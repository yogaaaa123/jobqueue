// Integration test end-to-end: API (HTTP beneran via httptest) + worker pool +
// SQLite satu file. Submit → proses → retry → dead → hasil resize, lewat HTTP.
package jobqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yogaaaa123/jobqueue/internal/api"
	"github.com/yogaaaa123/jobqueue/internal/handlers"
	"github.com/yogaaaa123/jobqueue/internal/store"
	"github.com/yogaaaa123/jobqueue/internal/worker"
)

// env menjalankan API + pool terhadap satu DB di temp dir, dibersihkan otomatis.
type env struct {
	srv *httptest.Server
	st  *store.Store
	dir string
}

func startEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "jobqueue.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := httptest.NewServer(api.New(st).Handler())

	pool := worker.New(st, 2)
	pool.PollEvery = 10 * time.Millisecond
	pool.Visibility = 2 * time.Second
	pool.ReclaimEvery = 50 * time.Millisecond
	pool.Backoff = func(int) time.Duration { return 10 * time.Millisecond }
	pool.Register("echo", handlers.Echo)
	pool.Register("sleep", handlers.Sleep)
	pool.Register("resize", handlers.Resize(st, filepath.Join(dir, "out")))
	pool.Register("fail", func(ctx context.Context, j *store.Job) error {
		return errors.New("selalu gagal")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-runDone
		srv.Close()
		st.Close()
	})
	return &env{srv: srv, st: st, dir: dir}
}

// postJob kirim JSON ke POST /jobs, balikin job id.
func postJob(t *testing.T, baseURL, body string) string {
	t.Helper()
	resp, err := http.Post(baseURL+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /jobs status = %d, want 202", resp.StatusCode)
	}
	var j store.Job
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return j.ID
}

// waitJob poll GET /jobs/{id} sampai status yang diinginkan.
func waitJob(t *testing.T, baseURL, id, want string) store.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/jobs/" + id)
		if err != nil {
			t.Fatalf("GET /jobs/%s: %v", id, err)
		}
		var j store.Job
		err = json.NewDecoder(resp.Body).Decode(&j)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if j.Status == want {
			return j
		}
		if j.Status == store.StatusDead && want != store.StatusDead {
			t.Fatalf("job %s dead: %s", id, j.LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s tidak mencapai status %q", id, want)
	return store.Job{}
}

// TestE2ESemuaSelesai: 5 job campuran lewat HTTP → semua done.
func TestE2ESemuaSelesai(t *testing.T) {
	e := startEnv(t)
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, postJob(t, e.srv.URL, `{"type":"echo"}`))
	}
	ids = append(ids,
		postJob(t, e.srv.URL, `{"type":"sleep","payload":{"ms":50}}`),
		postJob(t, e.srv.URL, `{"type":"sleep","payload":{"ms":50}}`))

	for _, id := range ids {
		if j := waitJob(t, e.srv.URL, id, store.StatusDone); j.Attempts != 1 {
			t.Fatalf("job %s attempts = %d, want 1", id, j.Attempts)
		}
	}
	// List menunjukkan semua done.
	resp, err := http.Get(e.srv.URL + "/jobs?status=done&limit=50")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var jobs []store.Job
	json.NewDecoder(resp.Body).Decode(&jobs)
	if len(jobs) != 5 {
		t.Fatalf("list done = %d, want 5", len(jobs))
	}
}

// TestE2ERetryKeDead: handler gagal terus → retry sesuai max_attempts → dead
// dengan last_error, job lain tetap jalan.
func TestE2ERetryKeDead(t *testing.T) {
	e := startEnv(t)
	id := postJob(t, e.srv.URL, `{"type":"fail","max_attempts":2}`)

	j := waitJob(t, e.srv.URL, id, store.StatusDead)
	if j.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", j.Attempts)
	}
	if j.LastError != "selalu gagal" {
		t.Fatalf("last_error = %q", j.LastError)
	}
	// Job sehat tetap diproses setelahnya.
	ok := postJob(t, e.srv.URL, `{"type":"echo"}`)
	waitJob(t, e.srv.URL, ok, store.StatusDone)
}

// TestE2EResizeLewatHTTP: submit resize via API → done → result menunjuk file
// dengan dimensi benar di disk.
func TestE2EResizeLewatHTTP(t *testing.T) {
	e := startEnv(t)

	// Sumber PNG 80×40.
	src := filepath.Join(e.dir, "src.png")
	img := image.NewRGBA(image.Rect(0, 0, 80, 40))
	for y := 0; y < 40; y++ {
		for x := 0; x < 80; x++ {
			img.Set(x, y, color.RGBA{uint8(x), uint8(y), 0, 255})
		}
	}
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := png.Encode(f, img); err != nil {
		t.Fatal(err)
	}
	f.Close()

	body := `{"type":"resize","payload":{"src":"` + src + `","widths":[40,20]}}`
	id := postJob(t, e.srv.URL, body)
	j := waitJob(t, e.srv.URL, id, store.StatusDone)

	var paths []string
	if err := json.Unmarshal([]byte(j.Result), &paths); err != nil || len(paths) != 2 {
		t.Fatalf("result = %q (err %v), want 2 path", j.Result, err)
	}
	for _, p := range paths {
		got, err := os.Open(p)
		if err != nil {
			t.Fatalf("buka hasil %s: %v", p, err)
		}
		cfg, _, err := image.DecodeConfig(got)
		got.Close()
		if err != nil {
			t.Fatalf("decode %s: %v", p, err)
		}
		wantH := cfg.Width / 2 // rasio 2:1
		if cfg.Height != wantH {
			t.Fatalf("%s = %dx%d, tinggi harus %d", p, cfg.Width, cfg.Height, wantH)
		}
	}
}
