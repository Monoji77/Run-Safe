// Command api serves the safe-to-run query over the Gold schema, using a read-only database role.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Monoji77/Run-Safe/internal/api"
	"github.com/Monoji77/Run-Safe/internal/guidance"
	"github.com/Monoji77/Run-Safe/internal/store"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := os.Getenv("API_DSN")
	if dsn == "" {
		slog.Error("API_DSN is required")
		os.Exit(1)
	}
	g, err := guidance.LoadConfig(env("GUIDANCE_PATH", "/config/guidance.yaml"))
	if err != nil {
		slog.Error("guidance config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           api.NewHandler(store.NewPG(pool), g, time.Now),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	slog.Info("api listening", "addr", srv.Addr, "guidance_version", g.Meta().Version)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}
