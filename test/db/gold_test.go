//go:build integration

package db_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Monoji77/Run-Safe/test/harness"
)

var ctx = context.Background()

func roleConn(t *testing.T, user, defaultPW, pwEnv string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(harness.DSN())
	if err != nil {
		t.Fatal(err)
	}
	cfg.User = user
	cfg.Password = defaultPW
	if v := os.Getenv(pwEnv); v != "" {
		cfg.Password = v
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	return conn
}

func insertObs(t *testing.T, exec interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, metric, region string, observedAt time.Time, value float64) error {
	t.Helper()
	_, err := exec.Exec(ctx, `INSERT INTO gold.observations
		(source, metric, region, observed_at, source_updated_at, value, unit, ingested_at, bronze_partition, bronze_offset, payload_sha256)
		VALUES ('pm25', $1, $2, $3, $3, $4, NULL, now(), 0, 0, 'x')`, metric, region, observedAt, value)
	return err
}

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestRegionsSeededWithLabelPoints(t *testing.T) {
	rows, err := harness.DB(t).Query(ctx, `SELECT region, ST_Y(label_point), ST_X(label_point) FROM gold.regions`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string][2]float64{}
	for rows.Next() {
		var name string
		var lat, lon float64
		if err := rows.Scan(&name, &lat, &lon); err != nil {
			t.Fatal(err)
		}
		got[name] = [2]float64{lat, lon}
	}
	want := map[string][2]float64{
		"north":   {1.41803, 103.82},
		"south":   {1.29587, 103.82},
		"east":    {1.35735, 103.94},
		"west":    {1.35735, 103.7},
		"central": {1.35735, 103.82},
	}
	if len(got) != len(want) {
		t.Fatalf("regions = %v, want %v", got, want)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s label point = %v, want %v", name, got[name], w)
		}
	}
}

func TestObservationsPrimaryKeyRejectsDuplicate(t *testing.T) {
	harness.TruncateGold(t)
	db := harness.DB(t)
	at := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	if err := insertObs(t, db, "pm25_one_hourly", "west", at, 39); err != nil {
		t.Fatal(err)
	}
	err := insertObs(t, db, "pm25_one_hourly", "west", at, 40)
	if pgCode(err) != "23505" {
		t.Fatalf("second insert error = %v (code %q), want unique violation 23505", err, pgCode(err))
	}
}

func TestBoundaryGISTIndexExists(t *testing.T) {
	var def string
	err := harness.DB(t).QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE schemaname='gold' AND indexname='regions_boundary_gix'`).Scan(&def)
	if err != nil {
		t.Fatalf("index lookup: %v", err)
	}
	if !strings.Contains(strings.ToLower(def), "using gist") {
		t.Fatalf("indexdef = %q, want USING gist", def)
	}
}

func TestCurrentConditionsReturnsLatestPerKey(t *testing.T) {
	harness.TruncateGold(t)
	db := harness.DB(t)
	early := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	late := early.Add(time.Hour)
	if err := insertObs(t, db, "pm25_one_hourly", "west", early, 39); err != nil {
		t.Fatal(err)
	}
	if err := insertObs(t, db, "pm25_one_hourly", "west", late, 41); err != nil {
		t.Fatal(err)
	}
	var n int
	var value float64
	var observed time.Time
	if err := db.QueryRow(ctx, `SELECT count(*) FROM gold.current_conditions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `SELECT value, observed_at FROM gold.current_conditions WHERE region='west'`).Scan(&value, &observed); err != nil {
		t.Fatal(err)
	}
	if n != 1 || value != 41 || !observed.Equal(late) {
		t.Fatalf("current_conditions rows=%d value=%v observed=%v, want 1 row, 41, %v", n, value, observed, late)
	}
}

func TestAPIRoleIsReadOnly(t *testing.T) {
	conn := roleConn(t, "api_ro", "api-ro-dev-only", "API_RO_PASSWORD")
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM gold.current_conditions`).Scan(&n); err != nil {
		t.Fatalf("api_ro SELECT: %v", err)
	}
	err := insertObs(t, conn, "pm25_one_hourly", "west", time.Now(), 1)
	if pgCode(err) != "42501" {
		t.Fatalf("api_ro INSERT error = %v (code %q), want permission denied 42501", err, pgCode(err))
	}
}

func TestConnectRoleCanUpsert(t *testing.T) {
	harness.TruncateGold(t)
	conn := roleConn(t, "connect_rw", "connect-rw-dev-only", "CONNECT_RW_PASSWORD")
	at := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	if err := insertObs(t, conn, "pm25_one_hourly", "west", at, 39); err != nil {
		t.Fatalf("connect_rw INSERT: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE gold.observations SET value = 41 WHERE region = 'west'`); err != nil {
		t.Fatalf("connect_rw UPDATE: %v", err)
	}
}

func TestConnectRoleCannotDelete(t *testing.T) {
	conn := roleConn(t, "connect_rw", "connect-rw-dev-only", "CONNECT_RW_PASSWORD")
	_, err := conn.Exec(ctx, `DELETE FROM gold.observations`)
	if pgCode(err) != "42501" {
		t.Fatalf("connect_rw DELETE error = %v (code %q), want permission denied 42501", err, pgCode(err))
	}
}
