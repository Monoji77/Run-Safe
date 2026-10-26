// Package harness holds helpers shared by the integration tests that run against the Compose stack.
package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	BronzeTopic = "weather.bronze"
	DLQTopic    = "weather.dlq"

	defaultKafka = "127.0.0.1:19092"
	defaultDSN   = "postgres://postgres:postgres-dev-only@127.0.0.1:15432/runsafe?sslmode=disable"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// DSN is the superuser connection string used by tests.
func DSN() string { return env("RUNSAFE_DB", defaultDSN) }

// Kafka returns a client connected to the dev listener; it is closed when the test ends.
func Kafka(t *testing.T) *kgo.Client {
	t.Helper()
	mark(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(env("RUNSAFE_KAFKA", defaultKafka)))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

var pools sync.Map // test name -> *pgxpool.Pool

// DB returns the test's superuser pool (one small pool per test, closed when the test ends).
func DB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	mark(t)
	if p, ok := pools.Load(t.Name()); ok {
		return p.(*pgxpool.Pool)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(DSN())
	if err != nil {
		t.Fatalf("db config: %v", err)
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("db pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("db ping: %v", err)
	}
	pools.Store(t.Name(), pool)
	t.Cleanup(func() { pools.Delete(t.Name()); pool.Close() })
	return pool
}

// TruncateGold empties gold.observations.
func TruncateGold(t *testing.T) {
	t.Helper()
	if _, err := DB(t).Exec(context.Background(), "TRUNCATE gold.observations"); err != nil {
		t.Fatalf("truncate gold.observations: %v", err)
	}
}

// PublishRaw publishes body to weather.bronze the way the fetcher does and returns where it landed.
func PublishRaw(t *testing.T, source string, body []byte, fetchedAt time.Time) (int32, int64) {
	t.Helper()
	cl := Kafka(t)
	sum := sha256.Sum256(body)
	rec := &kgo.Record{
		Topic: BronzeTopic,
		Key:   []byte(source + ":" + payloadTimestamp(body)),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte(source)},
			{Key: "fetched_at", Value: []byte(fetchedAt.UTC().Format(time.RFC3339))},
			{Key: "payload_sha256", Value: []byte(hex.EncodeToString(sum[:]))},
		},
	}
	res := cl.ProduceSync(context.Background(), rec)
	if err := res.FirstErr(); err != nil {
		t.Fatalf("publish: %v", err)
	}
	r := res[0].Record
	return r.Partition, r.Offset
}

// payloadTimestamp extracts data.items[0].timestamp or data.records[0].timestamp, else "unknown".
func payloadTimestamp(body []byte) string {
	var p struct {
		Data struct {
			Items []struct {
				Timestamp string `json:"timestamp"`
			} `json:"items"`
			Records []struct {
				Timestamp string `json:"timestamp"`
			} `json:"records"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &p) != nil {
		return "unknown"
	}
	if len(p.Data.Items) > 0 && p.Data.Items[0].Timestamp != "" {
		return p.Data.Items[0].Timestamp
	}
	if len(p.Data.Records) > 0 && p.Data.Records[0].Timestamp != "" {
		return p.Data.Records[0].Timestamp
	}
	return "unknown"
}

var (
	marks   sync.Map // test name -> map[int32]int64 DLQ end offsets at the test's first harness call
	markMu  sync.Mutex
	markCtx = context.Background()
)

// mark records the DLQ end offsets the first time a test touches the harness, so ReadDLQ
// returns only records appended during that test.
func mark(t *testing.T) {
	markMu.Lock()
	defer markMu.Unlock()
	if _, ok := marks.Load(t.Name()); ok {
		return
	}
	offs := map[int32]int64{}
	cl, err := kgo.NewClient(kgo.SeedBrokers(env("RUNSAFE_KAFKA", defaultKafka)))
	if err == nil {
		defer cl.Close()
		ctx, cancel := context.WithTimeout(markCtx, 5*time.Second)
		defer cancel()
		if ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, DLQTopic); err == nil {
			ends.Each(func(o kadm.ListedOffset) {
				if o.Err == nil {
					offs[o.Partition] = o.Offset
				}
			})
		}
	}
	marks.Store(t.Name(), offs)
}

// ReadDLQ returns the first n records appended to weather.dlq since the test's first harness call,
// failing the test if fewer arrive within timeout.
func ReadDLQ(t *testing.T, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	mark(t)
	v, _ := marks.Load(t.Name())
	start := v.(map[int32]int64)
	parts := map[int32]kgo.Offset{}
	if len(start) == 0 {
		parts[0] = kgo.NewOffset().AtStart()
	}
	for p, o := range start {
		parts[p] = kgo.NewOffset().At(o)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(env("RUNSAFE_KAFKA", defaultKafka)),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{DLQTopic: parts}),
	)
	if err != nil {
		t.Fatalf("dlq client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	if len(out) < n {
		t.Fatalf("expected %d DLQ record(s) within %s, got %d", n, timeout, len(out))
	}
	return out[:n]
}
