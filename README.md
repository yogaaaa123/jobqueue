# jobqueue

Job queue sederhana di Go: **HTTP API menerima tugas, worker pool memprosesnya di proses terpisah, state disimpan di SQLite.** Tugas tidak hilang walau proses crash dan request user tidak pernah menunggu kerja berat.

## Apa ini, buat apa?

Contoh nyata: endpoint upload foto — server harus resize ke 5 ukuran. Kalau dikerjakan langsung di request, user menunggu lama; kalau server mati di tengah jalan, pekerjaan hilang. `jobqueue` memecahnya:

- **`jobqueue serve`** (API) hanya menerima tugas dan langsung membalas `202 accepted` — request tetap cepat.
- **`jobqueue worker`** mengambil tugas dari antrian dan memprosesnya — boleh banyak proses, boleh ditambah worker-nya.
- **SQLite (WAL)** menyimpan antrian dan status — proses crash tinggal dinyalakan lagi, tugas dilanjut (retry / reclaim otomatis).

Cocok untuk:

- proses upload: resize gambar, generate thumbnail
- kirim email / notifikasi massal tanpa memperlambat request
- generate laporan periodik
- scraping atau tugas rutin yang butuh retry otomatis

Kurang cocok: butuh skala multi-node (pakai queue berbasis Redis/Postgres seperti River atau Faktory), atau tugas ringan sekali jalan (goroutine biasa sudah cukup).

## Fitur

- Submit job via `POST /jobs`, cek status via `GET /jobs/{id}`
- Worker pool terpisah dari API (multi-process, satu file SQLite WAL)
- Retry dengan exponential backoff (2^n detik, cap 5 menit), dead letter setelah max attempts
- Visibility timeout + heartbeat: worker crash → job di-reclaim proses lain
- Graceful shutdown: selesaikan job in-flight sebelum keluar
- Delivery at-least-once → handler wajib idempotent
- Handler bawaan: `echo`, `sleep`, `resize` (dengan hasil di `result`)

## Quick start

Prasyarat: [Go](https://go.dev/dl/) 1.22+ (routing `GET /jobs/{id}` pakai stdlib Go 1.22).

```sh
git clone https://github.com/yogaaaa123/jobqueue.git
cd jobqueue
```

Terminal 1 — API:

```sh
go run ./cmd/jobqueue serve      # http://localhost:8080, DB: jobqueue.db
```

Terminal 2 — worker (file `-db` harus sama dengan serve):

```sh
go run ./cmd/jobqueue worker
```

Terminal 3 — submit tugas pertama:

```sh
curl -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":{"msg":"halo"}}'
# → 202 {"id":"18d9...","status":"pending",...}

curl localhost:8080/jobs/18d9...        # poll sampai "status":"done"
```

Contoh nyata — resize gambar (`foto.jpg` ada di folder yang sama dengan worker):

```sh
curl -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"resize","payload":{"src":"foto.jpg","widths":[320,640,1280]}}'
# hasil: out/<job-id>-320.jpg dst.; daftar path ada di field "result" job
```

### Flag CLI

| Perintah | Flag | Default | Keterangan |
|---|---|---|---|
| `serve` | `-db` | `jobqueue.db` | file SQLite |
| | `-addr` | `:8080` | alamat listen |
| `worker` | `-db` | `jobqueue.db` | file SQLite (harus sama dengan serve) |
| | `-n` | `4` | jumlah goroutine worker |
| | `-poll` | `200ms` | interval poll saat antrian kosong |
| | `-visibility` | `60s` | lease job running tanpa heartbeat |
| | `-outdir` | `out` | folder hasil handler resize |

