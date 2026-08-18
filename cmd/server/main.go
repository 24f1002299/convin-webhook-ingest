package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/httpapi"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const shutdownTimeout = 10 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := store.New(ctx, cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	svc, err := ingest.New(ctx, st, stats.NewCache(), log)
	if err != nil {
		st.Close()
		return fmt.Errorf("hydrate account stats: %w", err)
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	hostname, _ := os.Hostname()
	go func() {
		defer close(workerDone)
		svc.RunRecordingWorker(workerCtx, fmt.Sprintf("%s:%d", hostname, os.Getpid()))
	}()

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.NewRouter(svc, log)}
	serveErr := make(chan error, 1)
	log.Info("listening", "addr", cfg.HTTPAddr)
	go func() {
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		cancelWorker()
		<-workerDone
		st.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve http: %w", err)
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = srv.Close()
	}

	cancelWorker()
	<-workerDone
	st.Close()

	if shutdownErr != nil {
		return fmt.Errorf("shutdown http: %w", shutdownErr)
	}
	return nil
}
