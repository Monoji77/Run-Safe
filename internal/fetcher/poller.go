// Package fetcher polls the data.gov.sg real-time APIs and publishes new or revised raw responses.
package fetcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const maxBody = 10 << 20

// Record is one message for the Bronze topic.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

type Publisher interface {
	Publish(ctx context.Context, r Record) error
}

type Endpoint struct{ Name, URL string }

func DefaultEndpoints() []Endpoint {
	const base = "https://api-open.data.gov.sg/v2/real-time/api/"
	return []Endpoint{{"pm25", base + "pm25"}, {"psi", base + "psi"}, {"uv", base + "uv"}}
}

type Poller struct {
	HTTP   *http.Client
	Pub    Publisher
	Topic  string
	APIKey string
	Now    func() time.Time
	Log    *slog.Logger

	mu   sync.Mutex
	seen map[string]string // endpoint name -> last published fingerprint
}

func NewPoller(client *http.Client, pub Publisher, topic, apiKey string, now func() time.Time) *Poller {
	return &Poller{HTTP: client, Pub: pub, Topic: topic, APIKey: apiKey, Now: now, Log: slog.Default(), seen: map[string]string{}}
}

// PollOnce fetches ep and publishes the body unchanged if its fingerprint differs from the last one published.
// Only HTTP 200 bodies are published (even if they are not valid JSON, so the pipeline can dead-letter them).
func (p *Poller) PollOnce(ctx context.Context, ep Endpoint) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.URL, nil)
	if err != nil {
		return false, err
	}
	if p.APIKey != "" {
		req.Header.Set("x-api-key", p.APIKey)
	}
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return false, fmt.Errorf("%s: %w", ep.Name, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return false, fmt.Errorf("%s: read body: %w", ep.Name, err)
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%s: unexpected status %d", ep.Name, resp.StatusCode)
	}

	fp := Fingerprint(ep.Name, body)
	p.mu.Lock()
	unchanged := p.seen[ep.Name] == fp
	p.mu.Unlock()
	if unchanged {
		p.Log.Info("poll", "endpoint", ep.Name, "fingerprint", fp, "published", false)
		return false, nil
	}

	sum := sha256.Sum256(body)
	rec := Record{
		Topic: p.Topic,
		Key:   []byte(ep.Name + ":" + dataTimestamp(ep.Name, body)),
		Value: body,
		Headers: map[string]string{
			"source":         ep.Name,
			"fetched_at":     p.Now().UTC().Format(time.RFC3339),
			"payload_sha256": hex.EncodeToString(sum[:]),
		},
	}
	if err := p.Pub.Publish(ctx, rec); err != nil {
		return false, fmt.Errorf("%s: publish: %w", ep.Name, err)
	}
	p.mu.Lock()
	p.seen[ep.Name] = fp
	p.mu.Unlock()
	p.Log.Info("poll", "endpoint", ep.Name, "fingerprint", fp, "published", true)
	return true, nil
}

// Run polls every endpoint immediately and then every interval until ctx is cancelled.
func Run(ctx context.Context, p *Poller, eps []Endpoint, interval time.Duration) {
	pollAll := func() {
		for _, ep := range eps {
			if ctx.Err() != nil {
				return
			}
			if _, err := p.PollOnce(ctx, ep); err != nil && ctx.Err() == nil {
				p.Log.Error("poll failed", "endpoint", ep.Name, "error", err)
			}
		}
	}
	pollAll()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pollAll()
		}
	}
}
