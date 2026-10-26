// Command fetcher polls data.gov.sg and publishes new or revised raw responses to Redpanda.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Monoji77/Run-Safe/internal/fetcher"
)

func main() {
	cfg, err := fetcher.LoadConfig(os.Getenv)
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}
	pub, err := fetcher.NewKafkaPublisher(cfg.Brokers)
	if err != nil {
		slog.Error("kafka", "error", err)
		os.Exit(1)
	}
	defer pub.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{Timeout: 30 * time.Second}
	p := fetcher.NewPoller(client, pub, cfg.Topic, cfg.APIKey, time.Now)
	slog.Info("fetcher starting", "interval", cfg.Interval.String(), "api_key_set", cfg.APIKey != "")
	fetcher.Run(ctx, p, fetcher.DefaultEndpoints(), cfg.Interval)
}
