//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/Monoji77/Run-Safe/internal/store"
	"github.com/Monoji77/Run-Safe/test/harness"
)

var ctx = context.Background()

func TestNearestRegionFiveLocations(t *testing.T) {
	s := store.NewPG(harness.DB(t))
	cases := []struct {
		name     string
		lat, lon float64
		want     string
	}{
		{"Woodlands", 1.4382, 103.7890, "north"},
		{"Jurong East", 1.3329, 103.7436, "west"},
		{"Tampines", 1.3496, 103.9568, "east"},
		{"Marina Bay", 1.2800, 103.8510, "south"},
		{"Bishan", 1.3526, 103.8352, "central"},
	}
	for _, tc := range cases {
		got, err := s.NearestRegion(ctx, tc.lat, tc.lon)
		if err != nil || got != tc.want {
			t.Errorf("%s: region = %q, err = %v; want %q", tc.name, got, err, tc.want)
		}
	}
}

func insert(t *testing.T, observed time.Time, value float64) {
	t.Helper()
	_, err := harness.DB(t).Exec(ctx, `INSERT INTO gold.observations
		(source, metric, region, observed_at, source_updated_at, value, unit, ingested_at, bronze_partition, bronze_offset, payload_sha256)
		VALUES ('pm25','pm25_one_hourly','west',$1,$1,$2,'ug/m3',now(),0,0,'x')`, observed, value)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLatestReturnsNewest(t *testing.T) {
	harness.TruncateGold(t)
	early := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	insert(t, early, 39)
	insert(t, early.Add(time.Hour), 41)
	o, found, err := store.NewPG(harness.DB(t)).Latest(ctx, "pm25_one_hourly", "west")
	if err != nil || !found || o.Value != 41 || !o.ObservedAt.Equal(early.Add(time.Hour)) || o.Unit != "ug/m3" {
		t.Fatalf("Latest = %+v found=%v err=%v; want value 41 at %v", o, found, err, early.Add(time.Hour))
	}
}

func TestLatestNotFound(t *testing.T) {
	harness.TruncateGold(t)
	_, found, err := store.NewPG(harness.DB(t)).Latest(ctx, "pm25_one_hourly", "west")
	if err != nil || found {
		t.Fatalf("found=%v err=%v; want false, nil", found, err)
	}
}
