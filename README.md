# jobqueue

Job queue + worker sederhana di Go. HTTP API buat submit tugas, worker pool buat proses, persist ke SQLite (WAL) — API & worker proses terpisah berbagi satu file DB.

## Fitur

- Submit job via `POST /jobs`, cek status via `GET /jobs/{id}`
- Worker pool terpisah dari API (multi-process, satu file SQLite WAL)
- Retry dengan exponential backoff, dead letter setelah max attempts
- Visibility timeout + heartbeat: worker crash → job di-reclaim
- Graceful shutdown: selesaikan job in-flight sebelum keluar
- Delivery at-least-once → handler wajib idempotent

## Cara jalan

```sh
go run ./cmd/jobqueue serve    # API di :8080
go run ./cmd/jobqueue worker   # worker pool
```

## API

| Method | Path | Keterangan |
|---|---|---|
| POST | `/jobs` | Body `{"type":"echo","payload":{},"max_attempts":3,"run_at":"RFC3339"}` → 202 + job |
| GET | `/jobs/{id}` | 200 job, 404 jika tidak ada |
| GET | `/jobs?status=pending&limit=50` | List job (limit 1..1000, default 100) |
| GET | `/healthz` | 200 `ok` |

Tipe job bawaan: `echo`, `sleep` (payload `{"ms":100}`).

## Milestone

- [x] M1 Store: SQLite WAL, job CRUD, antrian FIFO
- [x] M2 Worker core: claim, ack, retry, dead letter
- [x] M3 API: submit, status, list
- [x] M4 Resilience: graceful shutdown, visibility timeout
- [ ] M5 Handlers: echo, sleep, resize
- [ ] M6 Test: integration test end-to-end
