//go:build integration

package replay_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Monoji77/Run-Safe/internal/replay"
	"github.com/Monoji77/Run-Safe/test/harness"
)

var ctx = context.Background()

// commitToEnd marks everything already in the DLQ as replayed for group, so a test sees only its own records.
func commitToEnd(t *testing.T, cl *kgo.Client, group string) {
	t.Helper()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, harness.DLQTopic)
	if err != nil {
		t.Fatal(err)
	}
	offs := kadm.Offsets{}
	ends.Each(func(o kadm.ListedOffset) { offs.Add(kadm.Offset{Topic: o.Topic, Partition: o.Partition, At: o.Offset}) })
	if _, err := adm.CommitOffsets(ctx, group, offs); err != nil {
		t.Fatal(err)
	}
}

func bronzeEnd(t *testing.T, cl *kgo.Client) map[int32]int64 {
	t.Helper()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, harness.BronzeTopic)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) { out[o.Partition] = o.Offset })
	return out
}

func putInDLQ(t *testing.T, cl *kgo.Client, body []byte) {
	t.Helper()
	rec := &kgo.Record{
		Topic: harness.DLQTopic,
		Key:   []byte("pm25:2026-10-26T00:00:00+08:00"),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte("pm25")},
			{Key: "fetched_at", Value: []byte("2026-10-26T00:46:00Z")},
			{Key: "payload_sha256", Value: []byte("abc123")},
			{Key: "error", Value: []byte("unexpected_shape: test")},
		},
	}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../deploy/connect/testdata/pm25.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReplayRepublishesDLQRecordAndGoldLoads(t *testing.T) {
	harness.TruncateGold(t)
	cl := harness.Kafka(t)
	group := "replay-test-1-" + time.Now().Format("150405.000")
	commitToEnd(t, cl, group)
	before := bronzeEnd(t, cl)
	body := fixture(t)
	putInDLQ(t, cl, body)

	n, err := replay.Replay(ctx, cl, group)
	if err != nil || n != 1 {
		t.Fatalf("Replay = %d, %v; want 1, nil", n, err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cl.OptValue(kgo.SeedBrokers).([]string)...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{harness.BronzeTopic: {0: kgo.NewOffset().At(before[0])}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	pctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var got *kgo.Record
	for got == nil {
		consumer.PollFetches(pctx).EachRecord(func(r *kgo.Record) {
			if got == nil && string(r.Value) == string(body) {
				got = r
			}
		})
		if pctx.Err() != nil {
			t.Fatal("republished Bronze record not found")
		}
	}
	h := map[string]string{}
	for _, x := range got.Headers {
		h[x.Key] = string(x.Value)
	}
	if h["source"] != "pm25" || h["fetched_at"] != "2026-10-26T00:46:00Z" || h["payload_sha256"] != "abc123" {
		t.Errorf("source headers not preserved: %v", h)
	}
	if !strings.HasPrefix(h["replayed_from_dlq_offset"], "0:") {
		t.Errorf("replayed_from_dlq_offset = %q, want partition:offset", h["replayed_from_dlq_offset"])
	}
	if string(got.Key) != "pm25:2026-10-26T00:00:00+08:00" {
		t.Errorf("key = %q, want original key", got.Key)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var c int
		if err := harness.DB(t).QueryRow(ctx, "SELECT count(*) FROM gold.observations").Scan(&c); err != nil {
			t.Fatal(err)
		}
		if c == 5 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("Gold did not receive five rows from the replayed record")
}

func TestReplayTwiceReplaysNothingNew(t *testing.T) {
	cl := harness.Kafka(t)
	group := "replay-test-2-" + time.Now().Format("150405.000")
	commitToEnd(t, cl, group)
	putInDLQ(t, cl, fixture(t))
	if n, err := replay.Replay(ctx, cl, group); err != nil || n != 1 {
		t.Fatalf("first Replay = %d, %v; want 1, nil", n, err)
	}
	if n, err := replay.Replay(ctx, cl, group); err != nil || n != 0 {
		t.Fatalf("second Replay = %d, %v; want 0, nil", n, err)
	}
}
