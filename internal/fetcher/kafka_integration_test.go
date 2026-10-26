//go:build integration

package fetcher_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Monoji77/Run-Safe/internal/fetcher"
	"github.com/Monoji77/Run-Safe/test/harness"
)

func TestKafkaPublisherPublishesKeyValueAndHeaders(t *testing.T) {
	ctx := context.Background()
	brokers := strings.Split(envOr("RUNSAFE_KAFKA", "127.0.0.1:19092"), ",")
	topic := "fetcher-test-" + time.Now().Format("150405.000")

	admin := kadm.NewClient(harness.Kafka(t))
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.DeleteTopics(ctx, topic) })

	pub, err := fetcher.NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	err = pub.Publish(ctx, fetcher.Record{
		Topic:   topic,
		Key:     []byte("pm25:2026-10-26T00:00:00+08:00"),
		Value:   []byte(`{"a":1}`),
		Headers: map[string]string{"source": "pm25", "fetched_at": "2026-10-26T00:46:00Z", "payload_sha256": "abc"},
	})
	if err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	fetches := cl.PollFetches(pctx)
	var got *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { got = r })
	if got == nil {
		t.Fatal("no record consumed")
	}
	h := map[string]string{}
	for _, x := range got.Headers {
		h[x.Key] = string(x.Value)
	}
	if string(got.Key) != "pm25:2026-10-26T00:00:00+08:00" || string(got.Value) != `{"a":1}` ||
		h["source"] != "pm25" || h["fetched_at"] != "2026-10-26T00:46:00Z" || h["payload_sha256"] != "abc" {
		t.Errorf("record = key %q value %q headers %v", got.Key, got.Value, h)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
