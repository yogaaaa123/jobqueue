#!/usr/bin/env bash
# Smoke test M3: serve + worker + submit job lewat HTTP sampai status done.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/scripts/jobqueue"
go build -o "$BIN" ./cmd/jobqueue
DB=smoke.db
ADDR=127.0.0.1:18080
rm -f "$DB" "$DB-wal" "$DB-shm"

cleanup() {
  [[ -n "${SERVE_PID:-}" ]] && kill "$SERVE_PID" 2>/dev/null || true
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null || true
  rm -f "$BIN" "$DB" "$DB-wal" "$DB-shm" "$DB.serve.log" "$DB.worker.log"
}
trap cleanup EXIT

"$BIN" serve -db "$DB" -addr "$ADDR" >"$DB.serve.log" 2>&1 </dev/null &
SERVE_PID=$!
"$BIN" worker -db "$DB" -n 2 -poll 50ms >"$DB.worker.log" 2>&1 </dev/null &
WORKER_PID=$!

# Tunggu serve siap (startup bisa >0.5s, jangan sleep tetap).
READY=0
for _ in $(seq 1 30); do
  if curl -sf --max-time 1 "http://$ADDR/healthz" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.2
done
if [[ "$READY" != 1 ]]; then
  echo "SMOKE GAGAL: serve tidak siap"
  echo "=== serve.log ==="; cat "$DB.serve.log" 2>/dev/null
  echo "=== worker.log ==="; cat "$DB.worker.log" 2>/dev/null
  exit 1
fi

ID=$(curl -sf --max-time 2 -X POST "http://$ADDR/jobs" -d '{"type":"sleep","payload":{"ms":200}}' \
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

if [[ "$STATUS" != "done" ]]; then
  echo "SMOKE GAGAL"
  echo "=== serve.log ==="; cat "$DB.serve.log" 2>/dev/null
  echo "=== worker.log ==="; cat "$DB.worker.log" 2>/dev/null
  exit 1
fi
echo "SMOKE OK"
