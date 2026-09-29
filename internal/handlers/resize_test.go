package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/yogaaaa123/jobqueue/internal/store"
)

// makePNG bikin PNG solid warna berukuran w×h di path.
func makePNG(t *testing.T, path string, w, h int) {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x), G: uint8(y), B: 100, A: 255})
		}
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := png.Encode(f, img); err != nil {
		t.Fatal(err)
	}
}

func newTestJob(t *testing.T, st *store.Store, payload string) *store.Job {
	t.Helper()
	j := &store.Job{Type: "resize", Payload: json.RawMessage(payload)}
	if err := st.Create(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	return j
}

func decodeSize(t *testing.T, path string) (int, int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Width, cfg.Height
}

// TestResizeLocalFile: file lokal → 2 output dengan tinggi ikut rasio,
// result tersimpan di job.
func TestResizeLocalFile(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	src := filepath.Join(dir, "src.png")
	makePNG(t, src, 100, 50) // rasio 2:1

	outDir := filepath.Join(dir, "out")
	j := newTestJob(t, st, `{"src":"`+src+`","widths":[32,64]}`)
	if err := Resize(st, outDir)(context.Background(), j); err != nil {
		t.Fatalf("resize: %v", err)
	}

	for _, c := range []struct{ w, h int }{{32, 16}, {64, 32}} {
		// Format kosong → ikut sumber (png).
		p := filepath.Join(outDir, j.ID+"-"+itoa(c.w)+".png")
		gotW, gotH := decodeSize(t, p)
		if gotW != c.w || gotH != c.h {
			t.Fatalf("%s = %dx%d, want %dx%d", p, gotW, gotH, c.w, c.h)
		}
	}

	got, err := st.Get(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	if err := json.Unmarshal([]byte(got.Result), &paths); err != nil {
		t.Fatalf("result bukan JSON array: %q", got.Result)
	}
	if len(paths) != 2 {
		t.Fatalf("result %v, want 2 path", paths)
	}
}

// TestResizeHTTPSource: sumber via URL + format jpeg eksplisit (encode jpg).
func TestResizeHTTPSource(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Server serve PNG in-memory.
	var buf bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 40, 20))
	png.Encode(&buf, img)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	outDir := filepath.Join(dir, "out")
	j := newTestJob(t, st, `{"src":"`+srv.URL+`/x.png","widths":[10],"format":"jpeg"}`)
	if err := Resize(st, outDir)(context.Background(), j); err != nil {
		t.Fatalf("resize: %v", err)
	}
	p := filepath.Join(outDir, j.ID+"-10.jpg")
	gotW, gotH := decodeSize(t, p)
	if gotW != 10 || gotH != 5 {
		t.Fatalf("output = %dx%d, want 10x5", gotW, gotH)
	}
}

// TestResizeValidation: payload bermasalah → error, tidak ada file dibuat.
func TestResizeValidation(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	h := Resize(st, filepath.Join(dir, "out"))

	cases := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"tanpa src", `{"widths":[32]}`, "src wajib"},
		{"tanpa widths", `{"src":"x.png"}`, "widths wajib"},
		{"width nol", `{"src":"x.png","widths":[0]}`, "1..4096"},
		{"width gede", `{"src":"x.png","widths":[5000]}`, "1..4096"},
		{"format aneh", `{"src":"x.png","widths":[32],"format":"webp"}`, "tidak didukung"},
		{"bukan json", `{`, "payload resize"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := &store.Job{Type: "resize", Payload: json.RawMessage(c.payload)}
			err := h(context.Background(), j)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want mengandung %q", err, c.wantErr)
			}
		})
	}
}

// TestResizeHTTPErrorStatus: sumber 404 → error (bukan file rusak).
func TestResizeHTTPErrorStatus(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	j := &store.Job{Type: "resize", Payload: json.RawMessage(`{"src":"` + srv.URL + `","widths":[32]}`)}
	err = Resize(st, dir)(context.Background(), j)
	if err == nil || !strings.Contains(err.Error(), "status 404") {
		t.Fatalf("err = %v, want status 404", err)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
