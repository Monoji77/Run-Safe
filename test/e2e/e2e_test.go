//go:build e2e

// End-to-end verification against the full dev stack. Run with:
//
//	DGO_DOCKER_SOCKET=1 scripts/dgo test -count=1 -tags=e2e ./test/e2e/... -v -timeout 20m
package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Monoji77/Run-Safe/test/harness"
)

var ctx = context.Background()

// The fixture hour used by these tests (2026-10-26T00:00+08:00), kept apart from live data.
const fixtureHour = "2026-10-25T16:00:00Z"

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../deploy/connect/testdata/pm25.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func apiURL() string {
	if v := os.Getenv("RUNSAFE_API"); v != "" {
		return v
	}
	return "http://127.0.0.1:8080"
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

func fixtureRows(t *testing.T) map[string]float64 {
	t.Helper()
	rows, err := harness.DB(t).Query(ctx, `SELECT region, value FROM gold.observations
		WHERE source='pm25' AND metric='pm25_one_hourly' AND observed_at=$1`, fixtureHour)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var region string
		var v float64
		if err := rows.Scan(&region, &v); err != nil {
			t.Fatal(err)
		}
		out[region] = v
	}
	return out
}

func deleteFixtureRows(t *testing.T) {
	t.Helper()
	if _, err := harness.DB(t).Exec(ctx, `DELETE FROM gold.observations WHERE source='pm25' AND observed_at=$1`, fixtureHour); err != nil {
		t.Fatal(err)
	}
}

func connectCaughtUp(t *testing.T) bool {
	t.Helper()
	lags, err := kadm.NewClient(harness.Kafka(t)).Lag(ctx, "connect-gold")
	if err != nil {
		return false
	}
	l, ok := lags["connect-gold"]
	return ok && l.Error() == nil && l.State == "Stable" && l.Lag.Total() == 0
}

func TestReplayFromBronzeRebuildsAndIsIdempotent(t *testing.T) {
	deleteFixtureRows(t)
	partition, offset := harness.PublishRaw(t, "pm25", fixture(t), time.Now())
	waitFor(t, "five fixture rows", 30*time.Second, func() bool { return len(fixtureRows(t)) == 5 })
	original := fixtureRows(t)

	// Rebuild: stop Connect, drop the derived rows, rewind its offsets to the fixture message, start again.
	harness.DockerAction(t, "stop", harness.ContainerConnect)
	deleteFixtureRows(t)
	adm := kadm.NewClient(harness.Kafka(t))
	waitFor(t, "connect-gold group to be empty", 60*time.Second, func() bool {
		d, err := adm.DescribeGroups(ctx, "connect-gold")
		g, ok := d["connect-gold"]
		return err == nil && ok && (g.State == "Empty" || g.State == "Dead")
	})
	offs := kadm.Offsets{}
	offs.Add(kadm.Offset{Topic: harness.BronzeTopic, Partition: partition, At: offset, LeaderEpoch: -1})
	commitOffsets(t, adm, offs)
	harness.DockerAction(t, "start", harness.ContainerConnect)
	waitFor(t, "rebuilt rows", 60*time.Second, func() bool { return len(fixtureRows(t)) == 5 })
	if got := fixtureRows(t); !equal(got, original) {
		t.Fatalf("rebuilt rows %v differ from original %v", got, original)
	}

	// Replay again without deleting anything: still exactly the same five rows.
	harness.DockerAction(t, "stop", harness.ContainerConnect)
	waitFor(t, "connect-gold group to be empty", 60*time.Second, func() bool {
		d, err := adm.DescribeGroups(ctx, "connect-gold")
		g, ok := d["connect-gold"]
		return err == nil && ok && (g.State == "Empty" || g.State == "Dead")
	})
	commitOffsets(t, adm, offs)
	harness.DockerAction(t, "start", harness.ContainerConnect)
	waitFor(t, "connect to catch up", 60*time.Second, func() bool { return connectCaughtUp(t) })
	if got := fixtureRows(t); !equal(got, original) {
		t.Fatalf("after second replay rows %v differ from original %v", got, original)
	}
}

// commitOffsets rewinds connect-gold and fails the test if the broker rejects any partition's commit.
func commitOffsets(t *testing.T, adm *kadm.Client, offs kadm.Offsets) {
	t.Helper()
	resp, err := adm.CommitOffsets(ctx, "connect-gold", offs)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatalf("commit offsets rejected: %v", err)
	}
}

func equal(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func dlqEnd(t *testing.T) int64 {
	t.Helper()
	ends, err := kadm.NewClient(harness.Kafka(t)).ListEndOffsets(ctx, harness.DLQTopic)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	return total
}

func TestPostgresOutageDrainsWithoutDLQ(t *testing.T) {
	deleteFixtureRows(t)
	before := dlqEnd(t)
	harness.DockerAction(t, "stop", harness.ContainerPostgres)
	t.Cleanup(func() { harness.DockerAction(t, "start", harness.ContainerPostgres) })

	harness.PublishRaw(t, "pm25", fixture(t), time.Now())
	time.Sleep(10 * time.Second) // Connect is now failing to write and must be retrying, not dead-lettering
	if after := dlqEnd(t); after != before {
		t.Fatalf("DLQ grew from %d to %d during a database outage", before, after)
	}

	harness.DockerAction(t, "start", harness.ContainerPostgres)
	waitFor(t, "database to accept connections", 60*time.Second, func() bool {
		var one int
		return harness.DB(t).QueryRow(ctx, "SELECT 1").Scan(&one) == nil
	})
	waitFor(t, "backlog to drain into Gold", 120*time.Second, func() bool {
		rows, err := harness.DB(t).Query(ctx, `SELECT 1 FROM gold.observations WHERE source='pm25' AND observed_at=$1`, fixtureHour)
		if err != nil {
			return false
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		return n == 5
	})
	if after := dlqEnd(t); after != before {
		t.Fatalf("DLQ grew from %d to %d after recovery", before, after)
	}
}

func TestStaleDataGivesUnknownNotSafe(t *testing.T) {
	db := harness.DB(t)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`DROP TABLE IF EXISTS public.e2e_backup`)
	exec(`CREATE TABLE public.e2e_backup AS SELECT * FROM gold.observations WHERE metric='pm25_one_hourly' AND region='west'`)
	t.Cleanup(func() {
		db.Exec(ctx, `DELETE FROM gold.observations WHERE metric='pm25_one_hourly' AND region='west'`)
		db.Exec(ctx, `INSERT INTO gold.observations SELECT * FROM public.e2e_backup ON CONFLICT DO NOTHING`)
		db.Exec(ctx, `DROP TABLE IF EXISTS public.e2e_backup`)
	})
	exec(`DELETE FROM gold.observations WHERE metric='pm25_one_hourly' AND region='west'`)
	old := time.Now().Add(-3 * time.Hour)
	exec(`INSERT INTO gold.observations
		(source, metric, region, observed_at, source_updated_at, value, unit, ingested_at, bronze_partition, bronze_offset, payload_sha256)
		VALUES ('pm25','pm25_one_hourly','west',$1,$1,12,'ug/m3',now(),0,0,'e2e')`, old)

	resp, err := http.Get(apiURL() + "/v1/safe-to-run?lat=1.3329&lon=103.7436")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Status, Reason string
		Healthy        struct{ Headline string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || body.Status != "unknown" || body.Reason != "stale" || body.Healthy.Headline != "unknown" {
		t.Fatalf("status=%d body=%+v; want 200 unknown/stale (a 3-hour-old 12 ug/m3 reading must not look safe)", resp.StatusCode, body)
	}
}

func bronzeTotal(t *testing.T) int64 {
	t.Helper()
	ends, err := kadm.NewClient(harness.Kafka(t)).ListEndOffsets(ctx, harness.BronzeTopic)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	return total
}

func goldTotal(t *testing.T) int {
	t.Helper()
	var n int
	if err := harness.DB(t).QueryRow(ctx, "SELECT count(*) FROM gold.observations").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestVolumesSurviveRestart(t *testing.T) {
	// Other tests empty Gold, so make sure there is something to survive the restart.
	harness.PublishRaw(t, "pm25", fixture(t), time.Now())
	waitFor(t, "fixture rows in Gold", 30*time.Second, func() bool { return len(fixtureRows(t)) == 5 })
	bronzeBefore, goldBefore := bronzeTotal(t), goldTotal(t)
	if bronzeBefore == 0 || goldBefore == 0 {
		t.Fatalf("nothing to verify: bronze=%d gold=%d", bronzeBefore, goldBefore)
	}
	harness.DockerAction(t, "restart", harness.ContainerRedpanda)
	harness.DockerAction(t, "restart", harness.ContainerPostgres)
	waitFor(t, "broker and database back", 120*time.Second, func() bool {
		cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(envOr("RUNSAFE_KAFKA", "127.0.0.1:19092"), ",")...))
		if err != nil {
			return false
		}
		defer cl.Close()
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if _, err := kadm.NewClient(cl).ListEndOffsets(pctx, harness.BronzeTopic); err != nil {
			return false
		}
		var one int
		return harness.DB(t).QueryRow(ctx, "SELECT 1").Scan(&one) == nil
	})
	if got := bronzeTotal(t); got != bronzeBefore {
		t.Errorf("bronze records: %d before restart, %d after", bronzeBefore, got)
	}
	if got := goldTotal(t); got != goldBefore {
		t.Errorf("gold rows: %d before restart, %d after", goldBefore, got)
	}
	waitFor(t, "connect to be consuming again", 120*time.Second, func() bool { return connectCaughtUp(t) })
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestGISTIndexUsedForPointInPolygon(t *testing.T) {
	tx, err := harness.DB(t).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE gold.regions SET boundary = ST_Multi(ST_GeomFromText(
		'POLYGON((103.6 1.3, 103.8 1.3, 103.8 1.4, 103.6 1.4, 103.6 1.3))', 4326)) WHERE region = 'west'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, `EXPLAIN SELECT region FROM gold.regions
		WHERE ST_Intersects(boundary, ST_SetSRID(ST_MakePoint(103.7, 1.35), 4326))`)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	rows.Close()
	if !strings.Contains(plan.String(), "regions_boundary_gix") {
		t.Fatalf("plan does not use the GiST index:\n%s", plan.String())
	}
}
