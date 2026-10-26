//go:build integration

package pipeline_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Monoji77/Run-Safe/test/harness"
)

var ctx = context.Background()

const (
	original = `"updatedTimestamp":"2026-10-26T00:45:32+08:00"`
	revised  = `"updatedTimestamp":"2026-10-26T00:59:59+08:00"`
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../deploy/connect/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	wait := 30 * time.Second
	if v := os.Getenv("RUNSAFE_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			wait = d
		}
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", wait, what)
}

func count(t *testing.T, where string) int {
	t.Helper()
	var n int
	if err := harness.DB(t).QueryRow(ctx, "SELECT count(*) FROM gold.observations WHERE "+where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func west(t *testing.T) (value float64, updated time.Time) {
	t.Helper()
	err := harness.DB(t).QueryRow(ctx,
		`SELECT value, source_updated_at FROM gold.observations WHERE region='west' AND metric='pm25_one_hourly' AND observed_at='2026-10-26T00:00:00+08:00'`).
		Scan(&value, &updated)
	if err != nil {
		t.Fatal(err)
	}
	return value, updated
}

// awaitProcessed publishes a sentinel for a far-future hour and waits for its rows; the topic has one
// partition, so everything published before the sentinel has been processed once they appear.
func awaitProcessed(t *testing.T) {
	t.Helper()
	sentinel := strings.NewReplacer("2026-10-26T00:00:00+08:00", "2099-01-01T00:00:00+08:00",
		"2026-10-26T00:45:32+08:00", "2099-01-01T00:45:00+08:00").Replace(string(fixture(t, "pm25.json")))
	harness.PublishRaw(t, "pm25", []byte(sentinel), time.Now())
	waitFor(t, "sentinel rows", func() bool { return count(t, "observed_at >= '2090-01-01'") == 5 })
}

func TestPM25BecomesFiveGoldRows(t *testing.T) {
	harness.TruncateGold(t)
	harness.PublishRaw(t, "pm25", fixture(t, "pm25.json"), time.Now())
	waitFor(t, "five rows", func() bool { return count(t, "true") == 5 })
	if v, _ := west(t); v != 39 {
		t.Fatalf("west = %v, want 39", v)
	}
}

func TestReplayingSameMessageIsIdempotent(t *testing.T) {
	harness.TruncateGold(t)
	body := fixture(t, "pm25.json")
	harness.PublishRaw(t, "pm25", body, time.Now())
	harness.PublishRaw(t, "pm25", body, time.Now())
	awaitProcessed(t)
	if n := count(t, "observed_at < '2090-01-01'"); n != 5 {
		t.Fatalf("rows = %d, want 5", n)
	}
}

func TestNewerRevisionOverwrites(t *testing.T) {
	harness.TruncateGold(t)
	body := fixture(t, "pm25.json")
	harness.PublishRaw(t, "pm25", body, time.Now())
	newer := strings.NewReplacer(original, revised, `"west":39`, `"west":41`).Replace(string(body))
	harness.PublishRaw(t, "pm25", []byte(newer), time.Now())
	awaitProcessed(t)
	v, updated := west(t)
	if v != 41 || !updated.Equal(time.Date(2026, 10, 25, 16, 59, 59, 0, time.UTC)) {
		t.Fatalf("west = %v updated %v, want 41 at 00:59:59+08:00", v, updated)
	}
}

func TestOlderRevisionArrivingLaterDoesNotOverwrite(t *testing.T) {
	harness.TruncateGold(t)
	body := fixture(t, "pm25.json")
	newer := strings.NewReplacer(original, revised, `"west":39`, `"west":41`).Replace(string(body))
	harness.PublishRaw(t, "pm25", []byte(newer), time.Now())
	harness.PublishRaw(t, "pm25", body, time.Now()) // older revision arrives later
	awaitProcessed(t)
	if v, _ := west(t); v != 41 {
		t.Fatalf("west = %v, want 41 (older revision must not overwrite)", v)
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestMalformedJSONGoesToDLQWithContext(t *testing.T) {
	harness.TruncateGold(t)
	body := []byte("{not json")
	partition, offset := harness.PublishRaw(t, "pm25", body, time.Now())
	rec := harness.ReadDLQ(t, 1, 30*time.Second)[0]
	if string(rec.Value) != string(body) {
		t.Errorf("DLQ value = %q, want the original body", rec.Value)
	}
	if !strings.Contains(header(rec, "error"), "invalid_json") {
		t.Errorf("error header = %q, want invalid_json", header(rec, "error"))
	}
	if header(rec, "stage") != "mapping" {
		t.Errorf("stage = %q, want mapping", header(rec, "stage"))
	}
	if header(rec, "source_partition") != strconv.FormatInt(int64(partition), 10) || header(rec, "source_offset") != strconv.FormatInt(offset, 10) {
		t.Errorf("source position = %s/%s, want %d/%d", header(rec, "source_partition"), header(rec, "source_offset"), partition, offset)
	}
	for _, k := range []string{"source", "fetched_at", "payload_sha256"} {
		if header(rec, k) == "" {
			t.Errorf("DLQ record lost original header %q", k)
		}
	}
	if n := count(t, "true"); n != 0 {
		t.Errorf("gold rows = %d, want 0", n)
	}
}

func TestUnknownRegionGoesToDLQ(t *testing.T) {
	harness.TruncateGold(t)
	harness.PublishRaw(t, "pm25", fixture(t, "unknown_region.json"), time.Now())
	rec := harness.ReadDLQ(t, 1, 30*time.Second)[0]
	if !strings.Contains(header(rec, "error"), "unknown_region") {
		t.Errorf("error header = %q, want unknown_region", header(rec, "error"))
	}
	awaitProcessed(t)
	if n := count(t, "observed_at < '2090-01-01'"); n != 0 {
		t.Errorf("rows from the rejected message = %d, want 0", n)
	}
}

func TestNonNumericValueGoesToDLQ(t *testing.T) {
	harness.TruncateGold(t)
	harness.PublishRaw(t, "pm25", fixture(t, "non_numeric.json"), time.Now())
	rec := harness.ReadDLQ(t, 1, 30*time.Second)[0]
	if !strings.Contains(header(rec, "error"), "non_numeric_value") {
		t.Errorf("error header = %q, want non_numeric_value", header(rec, "error"))
	}
}
