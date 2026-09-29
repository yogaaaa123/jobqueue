// jobqueue: CLI dengan subcommand worker (serve menyusul di M3).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yogaaaa123/jobqueue/internal/api"
	"github.com/yogaaaa123/jobqueue/internal/handlers"
	"github.com/yogaaaa123/jobqueue/internal/store"
	"github.com/yogaaaa123/jobqueue/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "perintah tidak dikenal: %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: jobqueue <perintah>")
	fmt.Fprintln(os.Stderr, "perintah:")
	fmt.Fprintln(os.Stderr, "  serve    jalankan HTTP API (default :8080)")
	fmt.Fprintln(os.Stderr, "  worker   jalankan worker pool")
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dbPath := fs.String("db", "jobqueue.db", "path file SQLite")
	addr := fs.String("addr", ":8080", "alamat listen")
	fs.Parse(args)

	st, err := store.Open(*dbPath)
	if err != nil {
		slog.Error("buka store gagal", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(st).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(sh); err != nil {
			slog.Error("shutdown gagal", "err", err)
		}
	}()

	slog.Info("api mulai", "addr", *addr, "db", *dbPath)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server gagal", "err", err)
		os.Exit(1)
	}
	slog.Info("server shutdown bersih")
}

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	dbPath := fs.String("db", "jobqueue.db", "path file SQLite")
	n := fs.Int("n", 4, "jumlah worker goroutine")
	poll := fs.Duration("poll", 200*time.Millisecond, "interval poll saat antrian kosong")
	vis := fs.Duration("visibility", 60*time.Second, "lease job running tanpa heartbeat")
	fs.Parse(args)

	st, err := store.Open(*dbPath)
	if err != nil {
		slog.Error("buka store gagal", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	p := worker.New(st, *n)
	p.PollEvery = *poll
	p.Visibility = *vis
	p.Register("echo", handlers.Echo)
	p.Register("sleep", handlers.Sleep)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("worker mulai", "n", *n, "db", *dbPath, "poll", *poll, "visibility", *vis)
	if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("worker berhenti", "err", err)
		os.Exit(1)
	}
	slog.Info("worker shutdown bersih")
}
