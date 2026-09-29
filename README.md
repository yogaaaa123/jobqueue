# jobqueue

Job queue + worker sederhana di Go. HTTP API buat submit tugas, worker pool buat proses, persist ke bbolt.

## Fitur (target)

- Submit job via `POST /jobs`, cek status via `GET /jobs/{id}`
- Worker pool terpisah dari API (multi-process, satu file bbolt)
- Retry dengan exponential backoff, dead letter setelah max attempts
- Visibility timeout + heartbeat: worker crash → job di-reclaim
- Graceful shutdown: selesaikan job in-flight sebelum keluar
- Delivery at-least-once → handler wajib idempotent

## Cara jalan (target)

```sh
go run ./cmd/jobqueue serve    # API di :8080
go run ./cmd/jobqueue worker   # worker pool
```

## Milestone

- [x] M1 Store: bbolt wrapper, job CRUD, antrian FIFO
- [x] M2 Worker core: claim, ack, retry, dead letter
- [ ] M3 API: submit, status, list
- [ ] M4 Resilience: graceful shutdown, visibility timeout
- [ ] M5 Handlers: echo, sleep, resize
- [ ] M6 Test: integration test end-to-end
