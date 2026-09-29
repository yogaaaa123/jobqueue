#!/usr/bin/env bash
# One-off: E2E resize via API. Generate PNG → POST job → poll done → cek file + result.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/scripts/jobqueue"
DB=smoke.db
ADDR=127.0.0.1:18080
rm -f "$DB" "$DB-wal" "$DB-shm" "$DB.serve.log" "$DB.worker.log"
go build -o "$BIN" ./cmd/jobqueue

cleanup() {
  [[ -n "${SERVE_PID:-}" ]] && kill "$SERVE_PID" 2>/dev/null || true
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null || true
  rm -f "$BIN" "$DB" "$DB-wal" "$DB-shm" "$DB.serve.log" "$DB.worker.log" src.png
}
trap cleanup EXIT

# PNG 200x100 via Go
cat > $JCODE_SCRATCH_DIR/genpng.go << 'EOF'
package main

import (
	"image"
	"image/color"
	"image/png"
	"os"
)

func main() {
	img := image.NewRGBA(image.Rect(0, 0, 200, 100))
	for y := 0; y < 100; y++ {
		for x := 0; x < 200; x++ {
			img.Set(x, y, color.RGBA{uint8(x), uint8(y), 50, 255})
		}
	}
	f, _ := os.Create("src.png")
	defer f.Close()
	png.Encode(f, img)
}
EOF
go run $JCODE_SCRATCH_DIR/genpng.go

"$BIN" serve -db "$DB" -addr "$ADDR" >"$DB.serve.log" 2>&1 </dev/null &
SERVE_PID=$!
"$BIN" worker -db "$DB" -n 2 -poll 50ms -outdir out >"$DB.worker.log" 2>&1 </dev/null &
WORKER_PID=$!

for _ in $(seq 1 30); do
  curl -sf --max-time 1 "http://$ADDR/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done

ID=$(curl -sf -X POST "http://$ADDR/jobs" -H 'Content-Type: application/json' \
  -d '{"type":"resize","payload":{"src":"src.png","widths":[50,100]}}' \
  | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "job: $ID"

STATUS=""
for _ in $(seq 1 30); do
  STATUS=$(curl -sf "http://$ADDR/jobs/$ID" | sed -E 's/.*"status":"([^"]+)".*/\1/')
  [[ "$STATUS" == "done" || "$STATUS" == "dead" ]] && break
  sleep 0.2
done
echo "status: $STATUS"
echo "job json: $(curl -sf "http://$ADDR/jobs/$ID")"
ls -la out/ 2>/dev/null || echo "out/ kosong"

[[ "$STATUS" == "done" ]] || { echo "E2E GAGAL"; cat "$DB.serve.log" "$DB.worker.log"; exit 1; }
[[ -f "out/$ID-50.png" && -f "out/$ID-100.png" ]] || { echo "file hasil hilang"; exit 1; }
echo "E2E RESIZE OK"
