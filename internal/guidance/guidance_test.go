package guidance_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Monoji77/Run-Safe/internal/guidance"
)

const configPath = "../../config/guidance.yaml"

var now = time.Date(2026, 10, 26, 15, 30, 0, 0, time.FixedZone("SGT", 8*3600))

func load(t *testing.T) guidance.Config {
	t.Helper()
	c, err := guidance.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return c
}

func TestPM25BandBoundaries(t *testing.T) {
	c := load(t)
	cases := []struct {
		value float64
		band  string
	}{
		{0, "normal"}, {55, "normal"}, {55.5, "elevated"}, {56, "elevated"}, {150, "elevated"},
		{151, "high"}, {250, "high"}, {250.5, "very_high"}, {251, "very_high"},
	}
	for _, tc := range cases {
		got := c.EvaluatePM25(tc.value, now.Add(-10*time.Minute), now)
		if got.Band != tc.band {
			t.Errorf("value %v: band = %q, want %q", tc.value, got.Band, tc.band)
		}
	}
}

func TestHeadlinesPerBand(t *testing.T) {
	c := load(t)
	cases := []struct {
		value                  float64
		status                 guidance.Status
		healthy, vulnerable    guidance.Status
		healthyTxt, vulnerTxt  string
	}{
		{30, guidance.StatusOK, guidance.StatusOK, guidance.StatusOK,
			"Continue normal activities", "Continue normal activities"},
		{80, guidance.StatusCaution, guidance.StatusCaution, guidance.StatusAvoid,
			"Reduce strenuous outdoor activities for the next hour", "Avoid strenuous outdoor activities for the next hour"},
		{200, guidance.StatusAvoid, guidance.StatusAvoid, guidance.StatusAvoid,
			"Avoid strenuous outdoor activities for the next hour",
			"Avoid all outdoor activities for the next hour; may wear a mask to reduce exposure"},
		{300, guidance.StatusAvoid, guidance.StatusAvoid, guidance.StatusAvoid,
			"Minimise all outdoor activities for the next hour; may wear a mask to reduce exposure",
			"Avoid all outdoor activities for the next hour; may wear a mask to reduce exposure"},
	}
	for _, tc := range cases {
		got := c.EvaluatePM25(tc.value, now.Add(-10*time.Minute), now)
		if got.Status != tc.status || got.Healthy.Headline != tc.healthy || got.Vulnerable.Headline != tc.vulnerable ||
			got.Healthy.Text != tc.healthyTxt || got.Vulnerable.Text != tc.vulnerTxt {
			t.Errorf("value %v: got %+v", tc.value, got)
		}
	}
}

func TestInvalidValuesAreUnknown(t *testing.T) {
	c := load(t)
	for _, v := range []float64{-1, math.NaN(), math.Inf(1)} {
		got := c.EvaluatePM25(v, now.Add(-10*time.Minute), now)
		if got.Status != guidance.StatusUnknown || got.Reason != "invalid_value" {
			t.Errorf("value %v: got %+v, want unknown/invalid_value", v, got)
		}
	}
}

func TestStaleIsUnknown(t *testing.T) {
	c := load(t)
	stale := c.EvaluatePM25(30, now.Add(-91*time.Minute), now)
	if stale.Status != guidance.StatusUnknown || stale.Reason != "stale" {
		t.Errorf("91m old: got %+v, want unknown/stale", stale)
	}
	fresh := c.EvaluatePM25(30, now.Add(-89*time.Minute), now)
	if fresh.Status != guidance.StatusOK {
		t.Errorf("89m old: got %+v, want ok", fresh)
	}
}

func TestUnknownVerdictNeverLooksSafe(t *testing.T) {
	got := guidance.Unknown("no_data")
	if got.Status != guidance.StatusUnknown || got.Healthy.Headline != guidance.StatusUnknown ||
		got.Vulnerable.Headline != guidance.StatusUnknown || got.Healthy.Text == "" || got.Reason != "no_data" {
		t.Errorf("got %+v", got)
	}
}

func TestUVBands(t *testing.T) {
	c := load(t)
	cases := map[float64]string{2: "low", 3: "moderate", 5: "moderate", 6: "high", 8: "very_high", 11: "extreme"}
	for idx, want := range cases {
		if got := c.EvaluateUV(idx).Band; got != want {
			t.Errorf("UV %v: band = %q, want %q", idx, got, want)
		}
	}
}

func TestSunscreenFromThreshold(t *testing.T) {
	c := load(t)
	if c.EvaluateUV(2).SunscreenRecommended {
		t.Error("UV 2 should not recommend sunscreen")
	}
	if !c.EvaluateUV(3).SunscreenRecommended {
		t.Error("UV 3 should recommend sunscreen")
	}
}

func TestStaleAfterIs90Minutes(t *testing.T) {
	if got := load(t).StaleAfter(); got != 90*time.Minute {
		t.Errorf("StaleAfter = %v, want 90m", got)
	}
}

func TestMetaCarriesSourceAndDate(t *testing.T) {
	m := load(t).Meta()
	if m.Version != "2026-10-26" || m.Source != "https://www.haze.gov.sg/" || m.Retrieved != "2026-10-26" {
		t.Errorf("Meta = %+v", m)
	}
}

func TestLoadConfigRejectsUnorderedBands(t *testing.T) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	bad := string(raw)
	bad = replaceOnce(bad, "max: 55", "max: 300") // elevated (150) would now be below normal
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := guidance.LoadConfig(path); err == nil {
		t.Fatal("expected an error for unordered bands")
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	if _, err := guidance.LoadConfig(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func replaceOnce(s, old, repl string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + repl + s[i+len(old):]
		}
	}
	panic("pattern not found: " + old)
}
