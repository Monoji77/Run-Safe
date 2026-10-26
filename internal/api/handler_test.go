package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Monoji77/Run-Safe/internal/api"
	"github.com/Monoji77/Run-Safe/internal/guidance"
	"github.com/Monoji77/Run-Safe/internal/store"
)

var sgt = time.FixedZone("SGT", 8*3600)
var now = time.Date(2026, 10, 26, 15, 30, 0, 0, sgt)

type fakeStore struct {
	region    string
	regionErr error
	latestErr error
	obs       map[string]store.Observation // key: metric + "/" + region
}

func (f *fakeStore) NearestRegion(context.Context, float64, float64) (string, error) {
	return f.region, f.regionErr
}

func (f *fakeStore) Latest(_ context.Context, metric, region string) (store.Observation, bool, error) {
	if f.latestErr != nil {
		return store.Observation{}, false, f.latestErr
	}
	o, ok := f.obs[metric+"/"+region]
	return o, ok, nil
}

func pm25(value float64, observed time.Time) store.Observation {
	return store.Observation{Source: "pm25", Metric: "pm25_one_hourly", Region: "west", ObservedAt: observed,
		SourceUpdatedAt: observed.Add(45 * time.Second), Value: value, Unit: "ug/m3"}
}

func uv(value float64, observed time.Time) store.Observation {
	return store.Observation{Source: "uv", Metric: "uv_index", Region: "national", ObservedAt: observed,
		SourceUpdatedAt: observed.Add(11 * time.Minute), Value: value}
}

func newHandler(t *testing.T, f *fakeStore) http.Handler {
	t.Helper()
	g, err := guidance.LoadConfig("../../config/guidance.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return api.NewHandler(f, g, func() time.Time { return now })
}

func get(t *testing.T, h http.Handler, target string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	var body map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body %q is not JSON: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, body
}

// at reads a dotted path such as "pm25.band"; missing keys give nil.
func at(m map[string]any, path string) any {
	var cur any = m
	for _, k := range strings.Split(path, ".") {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}

const westURL = "/v1/safe-to-run?lat=1.3329&lon=103.7436"

func west(f *fakeStore, value float64, observed time.Time) *fakeStore {
	f.region = "west"
	if f.obs == nil {
		f.obs = map[string]store.Observation{}
	}
	f.obs["pm25_one_hourly/west"] = pm25(value, observed)
	return f
}

func TestNormalBandIsOK(t *testing.T) {
	code, body := get(t, newHandler(t, west(&fakeStore{}, 39, now.Add(-30*time.Minute))), westURL)
	if code != 200 || at(body, "status") != "ok" || at(body, "healthy.headline") != "ok" || at(body, "vulnerable.headline") != "ok" ||
		at(body, "pm25.band") != "normal" || at(body, "pm25.age_minutes") != float64(30) ||
		at(body, "pm25.observed_at") != "2026-10-26T15:00:00+08:00" || at(body, "pm25.value") != float64(39) {
		t.Fatalf("code=%d body=%v", code, body)
	}
}

func TestElevatedIsCautionForHealthyAvoidForVulnerable(t *testing.T) {
	_, body := get(t, newHandler(t, west(&fakeStore{}, 80, now.Add(-30*time.Minute))), westURL)
	if at(body, "status") != "caution" || at(body, "healthy.headline") != "caution" || at(body, "vulnerable.headline") != "avoid" {
		t.Fatalf("body=%v", body)
	}
}

func TestHighIsAvoid(t *testing.T) {
	_, body := get(t, newHandler(t, west(&fakeStore{}, 200, now.Add(-30*time.Minute))), westURL)
	if at(body, "status") != "avoid" || at(body, "healthy.headline") != "avoid" {
		t.Fatalf("body=%v", body)
	}
}

func TestStaleReadingIsUnknown(t *testing.T) {
	code, body := get(t, newHandler(t, west(&fakeStore{}, 39, now.Add(-150*time.Minute))), westURL)
	if code != 200 || at(body, "status") != "unknown" || at(body, "reason") != "stale" || at(body, "healthy.headline") != "unknown" ||
		at(body, "vulnerable.headline") != "unknown" {
		t.Fatalf("code=%d body=%v", code, body)
	}
}

func TestNoObservationIsUnknownNoData(t *testing.T) {
	code, body := get(t, newHandler(t, &fakeStore{region: "west"}), westURL)
	if code != 200 || at(body, "status") != "unknown" || at(body, "reason") != "no_data" {
		t.Fatalf("code=%d body=%v", code, body)
	}
}

func TestMissingLatLonIs400(t *testing.T) {
	h := newHandler(t, &fakeStore{region: "west"})
	for _, target := range []string{"/v1/safe-to-run", "/v1/safe-to-run?lat=1.3", "/v1/safe-to-run?lon=103.8"} {
		if code, body := get(t, h, target); code != 400 || at(body, "error") != "invalid_coordinates" {
			t.Errorf("%s: code=%d body=%v", target, code, body)
		}
	}
}

func TestNonNumericAndNaNAre400(t *testing.T) {
	h := newHandler(t, &fakeStore{region: "west"})
	for _, target := range []string{
		"/v1/safe-to-run?lat=abc&lon=103.8", "/v1/safe-to-run?lat=NaN&lon=103.8", "/v1/safe-to-run?lat=1.3&lon=Inf",
		"/v1/safe-to-run?lat=95&lon=103.8", "/v1/safe-to-run?lat=1.3&lon=-181",
	} {
		if code, body := get(t, h, target); code != 400 || at(body, "error") != "invalid_coordinates" {
			t.Errorf("%s: code=%d body=%v", target, code, body)
		}
	}
}

func TestOutsideSingaporeIs422(t *testing.T) {
	code, body := get(t, newHandler(t, &fakeStore{region: "west"}), "/v1/safe-to-run?lat=51.5&lon=-0.12")
	if code != 422 || at(body, "error") != "outside_coverage" {
		t.Fatalf("code=%d body=%v", code, body)
	}
}

func TestStoreErrorIs503(t *testing.T) {
	for name, f := range map[string]*fakeStore{
		"region": {regionErr: errors.New("db down")},
		"latest": {region: "west", latestErr: errors.New("db down")},
	} {
		if code, body := get(t, newHandler(t, f), westURL); code != 503 || at(body, "error") != "data_unavailable" {
			t.Errorf("%s: code=%d body=%v", name, code, body)
		}
	}
}

func ptr(o store.Observation) *store.Observation { return &o }

func TestUVSunscreenAndUnavailable(t *testing.T) {
	cases := []struct {
		name      string
		obs       *store.Observation
		status    string
		sunscreen bool
		band      any
	}{
		{"fresh index 5", ptr(uv(5, now.Add(-30*time.Minute))), "ok", true, "moderate"},
		{"fresh index 1", ptr(uv(1, now.Add(-30*time.Minute))), "ok", false, "low"},
		{"stale (night)", ptr(uv(0, now.Add(-5*time.Hour))), "unavailable", false, nil},
		{"missing", nil, "unavailable", false, nil},
	}
	for _, tc := range cases {
		f := west(&fakeStore{}, 39, now.Add(-30*time.Minute))
		if tc.obs != nil {
			f.obs["uv_index/national"] = *tc.obs
		}
		_, body := get(t, newHandler(t, f), westURL)
		if at(body, "uv.status") != tc.status || at(body, "uv.sunscreen_recommended") != tc.sunscreen || at(body, "uv.band") != tc.band {
			t.Errorf("%s: uv=%v", tc.name, at(body, "uv"))
		}
	}
}

func TestResponseLabelsRegionApproximate(t *testing.T) {
	_, body := get(t, newHandler(t, west(&fakeStore{}, 39, now.Add(-30*time.Minute))), westURL)
	if at(body, "region.name") != "west" || at(body, "region.method") != "nearest_label_point" || at(body, "region.approximate") != true {
		t.Fatalf("region=%v", at(body, "region"))
	}
	if d, _ := at(body, "disclaimer").(string); !strings.Contains(strings.ToLower(d), "not medical advice") {
		t.Fatalf("disclaimer = %q", d)
	}
}

func TestGuidanceMetadataIncluded(t *testing.T) {
	_, body := get(t, newHandler(t, west(&fakeStore{}, 39, now.Add(-30*time.Minute))), westURL)
	if at(body, "guidance.version") != "2026-10-26" || at(body, "guidance.source") != "https://www.haze.gov.sg/" ||
		at(body, "guidance.retrieved") != "2026-10-26" {
		t.Fatalf("guidance=%v", at(body, "guidance"))
	}
}

func TestPSIContextIncludedOnlyWhenAvailable(t *testing.T) {
	f := west(&fakeStore{}, 39, now.Add(-30*time.Minute))
	_, without := get(t, newHandler(t, f), westURL)
	if _, present := without["psi_24h"]; present {
		t.Errorf("psi_24h present without a reading: %v", without["psi_24h"])
	}
	f.obs["psi_twenty_four_hourly/west"] = store.Observation{Metric: "psi_twenty_four_hourly", Region: "west",
		ObservedAt: now.Add(-30 * time.Minute), Value: 72}
	_, with := get(t, newHandler(t, f), westURL)
	if at(with, "psi_24h.value") != float64(72) || at(with, "psi_24h.observed_at") != "2026-10-26T15:00:00+08:00" {
		t.Errorf("psi_24h = %v", with["psi_24h"])
	}
}

func TestHealthzAndMethods(t *testing.T) {
	h := newHandler(t, &fakeStore{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("/healthz = %d, want 200", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, westURL, nil))
	if rec.Code != 405 {
		t.Errorf("POST = %d, want 405", rec.Code)
	}
}
