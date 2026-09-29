package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/disintegration/imaging"
	"github.com/yogaaaa123/jobqueue/internal/store"

	// decoder bawaan: jpeg, png, gif
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
)

// Batas keamanan handler resize.
const (
	maxSrcBytes     = 20 << 20 // 20 MiB per gambar (file/URL)
	maxDecodePixels = 100e6    // 100 MP, cegah decompression bomb
	maxWidths       = 10
	srcFetchTimeout = 30 * time.Second
)

// resizePayload adalah payload job tipe "resize".
type resizePayload struct {
	Src    string `json:"src"`              // path file ATAU http(s) URL
	Widths []int  `json:"widths"`           // lebar output, tinggi ikut rasio
	Format string `json:"format,omitempty"` // "jpeg"|"png", kosong = ikut sumber
	OutDir string `json:"outdir,omitempty"` // default outDir argumen
}

// Resize membuat handler tipe "resize": download/baca gambar → resize per
// width (Lanczos) → tulis ke outDir → simpan daftar path ke job.Result.
func Resize(st *store.Store, outDir string) func(context.Context, *store.Job) error {
	client := &http.Client{Timeout: srcFetchTimeout}
	return func(ctx context.Context, j *store.Job) error {
		var p resizePayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return fmt.Errorf("payload resize: %w", err)
		}
		if err := validateResize(&p); err != nil {
			return err
		}
		if p.OutDir == "" {
			p.OutDir = outDir
		}

		raw, err := fetchSrc(ctx, client, p.Src)
		if err != nil {
			return fmt.Errorf("ambil sumber: %w", err)
		}
		cfg, srcFormat, err := image.DecodeConfig(bytes.NewReader(raw))
		if err != nil {
			return fmt.Errorf("decode gambar: %w", err)
		}
		if int64(cfg.Width)*int64(cfg.Height) > maxDecodePixels {
			return fmt.Errorf("gambar terlalu besar: %dx%d (max %d piksel)", cfg.Width, cfg.Height, int64(maxDecodePixels))
		}
		img, _, err := image.Decode(bytes.NewReader(raw))
		if err != nil {
			return fmt.Errorf("decode gambar: %w", err)
		}

		// Format output: minta client, atau ikut sumber (gif → jpeg, imaging
		// tidak punya encoder gif).
		outFormat := p.Format
		if outFormat == "" {
			if srcFormat == "png" {
				outFormat = "png"
			} else {
				outFormat = "jpeg"
			}
		}
		ext := "jpg"
		if outFormat == "png" {
			ext = "png"
		}
		if err := os.MkdirAll(p.OutDir, 0o755); err != nil {
			return fmt.Errorf("buat outdir: %w", err)
		}

		var paths []string
		for _, w := range p.Widths {
			resized := imaging.Resize(img, w, 0, imaging.Lanczos)
			name := fmt.Sprintf("%s-%d.%s", j.ID, w, ext)
			path := filepath.Join(p.OutDir, name)
			if err := writeImage(path, resized, outFormat); err != nil {
				return err
			}
			paths = append(paths, path)
		}

		out, err := json.Marshal(paths)
		if err != nil {
			return err
		}
		if err := st.SaveResult(ctx, j.ID, string(out)); err != nil {
			return fmt.Errorf("simpan result: %w", err)
		}
		return nil
	}
}

func validateResize(p *resizePayload) error {
	if p.Src == "" {
		return fmt.Errorf("src wajib diisi")
	}
	if len(p.Widths) == 0 {
		return fmt.Errorf("widths wajib diisi")
	}
	if len(p.Widths) > maxWidths {
		return fmt.Errorf("widths maksimal %d", maxWidths)
	}
	for _, w := range p.Widths {
		if w < 1 || w > 4096 {
			return fmt.Errorf("width %d di luar rentang 1..4096", w)
		}
	}
	if p.Format != "" && p.Format != "jpeg" && p.Format != "png" {
		return fmt.Errorf("format %q tidak didukung (jpeg|png)", p.Format)
	}
	return nil
}

// fetchSrc: http(s) via client dengan limit ukuran, selain itu path file lokal.
func fetchSrc(ctx context.Context, client *http.Client, src string) ([]byte, error) {
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("status %d", resp.StatusCode)
		}
		return readLimited(resp.Body, maxSrcBytes)
	}
	fi, err := os.Stat(src)
	if err != nil {
		return nil, err
	}
	if fi.Size() > maxSrcBytes {
		return nil, fmt.Errorf("file %d byte, max %d", fi.Size(), maxSrcBytes)
	}
	return os.ReadFile(src)
}

func readLimited(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("melebihi %d byte", max)
	}
	return b, nil
}

func writeImage(path string, img image.Image, format string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("buat file: %w", err)
	}
	defer f.Close() // close kedua diabaikan; error flush dicek di f.Close() bawah
	var encErr error
	switch format {
	case "png":
		encErr = imaging.Encode(f, img, imaging.PNG)
	default:
		encErr = imaging.Encode(f, img, imaging.JPEG, imaging.JPEGQuality(85))
	}
	if encErr != nil {
		return fmt.Errorf("encode: %w", encErr)
	}
	return f.Close()
}
