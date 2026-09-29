#!/usr/bin/env bash
# Smoke test M3: serve + worker + submit job lewat HTTP sampai status done.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/scripts/jobqueue"
go build -o "$BIN" ./cmd/jobqueue
DB=smoke.db
ADDR=127.0.0.1:18080
rm -f "$DB"

cleanup() {
  [[ -n "${SERVE_PID:-}" ]] && kill "$SERVE_PID" 2>/dev/null || true
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null || true
  rm -f "$BIN" smoke.db
}
trap cleanup EXIT

"$BIN" serve -db "$DB" -addr "$ADDR" >/dev/null 2>&1 &
SERVE_PID=$!
"$BIN" worker -db "$DB" -n 2 -poll 50ms >/dev/null 2>&1 &
WORKER_PID=$!

sleep 0.5
ID=$(curl -sf -X POST "http://$ADDR/jobs" -d '{"type":"sleep","payload":{"ms":200}}' \
  | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "job id: $ID"

STATUS=""
for _ in $(seq 1 30); do
  STATUS=$(curl -sf "http://$ADDR/jobs/$ID" | sed -E 's/.*"status":"([^"]+)".*/\1/')
  [[ "$STATUS" == "done" ]] && break
  sleep 0.2
done
echo "final status: $STATUS"
echo "list: $(curl -sf "http://$ADDR/jobs?limit=5" | head -c 300)"

[[ "$STATUS" == "done" ]] || { echo "SMOKE GAGAL"; exit 1; }
echo "SMOKE OK"
