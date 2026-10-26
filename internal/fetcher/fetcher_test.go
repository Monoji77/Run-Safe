package fetcher_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Monoji77/Run-Safe/internal/fetcher"
)

type fakePub struct {
	mu   sync.Mutex
	recs []fetcher.Record
}

func (f *fakePub) Publish(_ context.Context, r fetcher.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs = append(f.recs, r)
	return nil
}

func (f *fakePub) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recs)
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../deploy/connect/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var fixedNow = func() time.Time { return time.Date(2026, 10, 26, 0, 46, 0, 0, time.UTC) }

// serve returns a server whose response body and status can be changed between polls.
func serve(t *testing.T, body *[]byte, status *int, gotKey *string) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if gotKey != nil {
			*gotKey = r.Header.Get("x-api-key")
		}
		w.WriteHeader(*status)
		w.Write(*body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newPoller(pub fetcher.Publisher, apiKey string) *fetcher.Poller {
	return fetcher.NewPoller(http.DefaultClient, pub, "weather.bronze", apiKey, fixedNow)
}

func TestUnchangedResponsePublishesOnce(t *testing.T) {
	body, status := fixture(t, "pm25.json"), 200
	srv := serve(t, &body, &status, nil)
	pub := &fakePub{}
	p := newPoller(pub, "")
	ep := fetcher.Endpoint{Name: "pm25", URL: srv.URL}
	first, err := p.PollOnce(context.Background(), ep)
	second, err2 := p.PollOnce(context.Background(), ep)
	if err != nil || err2 != nil || !first || second || pub.count() != 1 {
		t.Fatalf("first=%v second=%v published=%d errs=%v/%v; want true,false,1", first, second, pub.count(), err, err2)
	}
}

func TestRevisedUpdatedTimestampPublishesAgain(t *testing.T) {
	body, status := fixture(t, "pm25.json"), 200
	srv := serve(t, &body, &status, nil)
	pub := &fakePub{}
	p := newPoller(pub, "")
	ep := fetcher.Endpoint{Name: "pm25", URL: srv.URL}
	if _, err := p.PollOnce(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "2026-10-26T00:45:32+08:00", "2026-10-26T00:59:59+08:00", 1))
	again, err := p.PollOnce(context.Background(), ep)
	if err != nil || !again || pub.count() != 2 {
		t.Fatalf("again=%v published=%d err=%v; want true,2", again, pub.count(), err)
	}
}

func TestInvalidJSONPublishedOnceThenDeduped(t *testing.T) {
	body, status := []byte("{not json"), 200
	srv := serve(t, &body, &status, nil)
	pub := &fakePub{}
	p := newPoller(pub, "")
	ep := fetcher.Endpoint{Name: "pm25", URL: srv.URL}
	a, _ := p.PollOnce(context.Background(), ep)
	b, _ := p.PollOnce(context.Background(), ep)
	if !a || b || pub.count() != 1 {
		t.Fatalf("a=%v b=%v published=%d; want true,false,1", a, b, pub.count())
	}
	if string(pub.recs[0].Value) != "{not json" || !strings.HasSuffix(string(pub.recs[0].Key), ":unknown") {
		t.Fatalf("record = %+v", pub.recs[0])
	}
}

func TestNon200PublishesNothingAndReturnsError(t *testing.T) {
	for _, code := range []int{429, 500} {
		body, status := []byte(`{"error":"x"}`), code
		srv := serve(t, &body, &status, nil)
		pub := &fakePub{}
		published, err := newPoller(pub, "").PollOnce(context.Background(), fetcher.Endpoint{Name: "pm25", URL: srv.URL})
		if err == nil || published || pub.count() != 0 {
			t.Errorf("status %d: published=%v count=%d err=%v; want false,0,error", code, published, pub.count(), err)
		}
	}
}

func TestHeadersAndKey(t *testing.T) {
	body, status := fixture(t, "pm25.json"), 200
	srv := serve(t, &body, &status, nil)
	pub := &fakePub{}
	if _, err := newPoller(pub, "").PollOnce(context.Background(), fetcher.Endpoint{Name: "pm25", URL: srv.URL}); err != nil {
		t.Fatal(err)
	}
	r := pub.recs[0]
	sum := sha256.Sum256(body)
	if r.Topic != "weather.bronze" || string(r.Key) != "pm25:2026-10-26T00:00:00+08:00" || !bytes.Equal(r.Value, body) {
		t.Errorf("topic/key/value wrong: %q %q", r.Topic, r.Key)
	}
	want := map[string]string{"source": "pm25", "fetched_at": "2026-10-26T00:46:00Z", "payload_sha256": hex.EncodeToString(sum[:])}
	for k, v := range want {
		if r.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, r.Headers[k], v)
		}
	}
}

func TestAPIKeyHeaderSentWhenConfigured(t *testing.T) {
	body, status := fixture(t, "pm25.json"), 200
	var got string
	srv := serve(t, &body, &status, &got)
	ep := fetcher.Endpoint{Name: "pm25", URL: srv.URL}
	if _, err := newPoller(&fakePub{}, "secret-key").PollOnce(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	if got != "secret-key" {
		t.Errorf("x-api-key = %q, want secret-key", got)
	}
	if _, err := newPoller(&fakePub{}, "").PollOnce(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("x-api-key = %q, want none when no key configured", got)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	body, status := fixture(t, "pm25.json"), 200
	srv := serve(t, &body, &status, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		fetcher.Run(ctx, newPoller(&fakePub{}, ""), []fetcher.Endpoint{{Name: "pm25", URL: srv.URL}}, 10*time.Millisecond)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestFingerprintPerEndpointShape(t *testing.T) {
	cases := []struct {
		endpoint string
		body     []byte
		want     string
	}{
		{"pm25", fixture(t, "pm25.json"), "2026-10-26T00:00:00+08:00|2026-10-26T00:45:32+08:00"},
		{"psi", fixture(t, "psi.json"), "2026-10-26T16:00:00+08:00|2026-10-26T16:45:34+08:00"},
		{"uv", fixture(t, "uv.json"), "2026-10-26T16:00:00+08:00|2026-10-26T16:11:09+08:00"},
	}
	for _, tc := range cases {
		if got := fetcher.Fingerprint(tc.endpoint, tc.body); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.endpoint, got, tc.want)
		}
	}
	garbage := []byte("{not json")
	sum := sha256.Sum256(garbage)
	if got := fetcher.Fingerprint("pm25", garbage); got != "sha256:"+hex.EncodeToString(sum[:]) {
		t.Errorf("garbage fingerprint = %q", got)
	}
	empty := []byte(`{"code":0,"data":{"items":[]}}`)
	if got := fetcher.Fingerprint("pm25", empty); !strings.HasPrefix(got, "sha256:") {
		t.Errorf("empty items fingerprint = %q, want sha256 fallback", got)
	}
}

func TestLogsOneStructuredLinePerPoll(t *testing.T) {
	body, status := fixture(t, "pm25.json"), 200
	srv := serve(t, &body, &status, nil)
	var buf bytes.Buffer
	p := newPoller(&fakePub{}, "")
	p.Log = slog.New(slog.NewJSONHandler(&buf, nil))
	ep := fetcher.Endpoint{Name: "pm25", URL: srv.URL}
	p.PollOnce(context.Background(), ep)
	p.PollOnce(context.Background(), ep)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want 2: %s", len(lines), buf.String())
	}
	var first, second map[string]any
	json.Unmarshal([]byte(lines[0]), &first)
	json.Unmarshal([]byte(lines[1]), &second)
	if first["endpoint"] != "pm25" || first["published"] != true ||
		first["fingerprint"] != "2026-10-26T00:00:00+08:00|2026-10-26T00:45:32+08:00" || second["published"] != false {
		t.Errorf("log lines: %v / %v", first, second)
	}
}

func TestDefaultEndpoints(t *testing.T) {
	want := map[string]string{
		"pm25": "https://api-open.data.gov.sg/v2/real-time/api/pm25",
		"psi":  "https://api-open.data.gov.sg/v2/real-time/api/psi",
		"uv":   "https://api-open.data.gov.sg/v2/real-time/api/uv",
	}
	got := map[string]string{}
	for _, e := range fetcher.DefaultEndpoints() {
		got[e.Name] = e.URL
	}
	if len(got) != 3 || got["pm25"] != want["pm25"] || got["psi"] != want["psi"] || got["uv"] != want["uv"] {
		t.Errorf("endpoints = %v", got)
	}
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfigDefaults(t *testing.T) {
	c, err := fetcher.LoadConfig(env(map[string]string{"KAFKA_BROKERS": "redpanda:9092,other:9092"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Interval != fetcher.DefaultInterval || c.APIKey != "" || c.Topic != "weather.bronze" || len(c.Brokers) != 2 {
		t.Errorf("config = %+v", c)
	}
}

func TestLoadConfigRejectsBadInterval(t *testing.T) {
	for _, v := range []string{"soon", "0s", "-5s"} {
		if _, err := fetcher.LoadConfig(env(map[string]string{"KAFKA_BROKERS": "x:1", "FETCH_INTERVAL": v})); err == nil {
			t.Errorf("FETCH_INTERVAL=%q accepted", v)
		}
	}
}

func TestLoadConfigRequiresBrokers(t *testing.T) {
	if _, err := fetcher.LoadConfig(env(nil)); err == nil {
		t.Fatal("expected an error without KAFKA_BROKERS")
	}
}
