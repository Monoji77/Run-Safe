// Package api serves the "is it safe to run here, now?" query over Gold.
package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/Monoji77/Run-Safe/internal/guidance"
	"github.com/Monoji77/Run-Safe/internal/store"
)

// Coarse bounding box around Singapore; coordinates outside it are rejected rather than mapped to a region.
const (
	minLat, maxLat = 1.15, 1.48
	minLon, maxLon = 103.55, 104.10
)

var sgt = time.FixedZone("SGT", 8*3600)

type handler struct {
	r   store.Reader
	g   guidance.Config
	now func() time.Time
}

// NewHandler serves GET /v1/safe-to-run?lat=&lon= and GET /healthz.
func NewHandler(r store.Reader, g guidance.Config, now func() time.Time) http.Handler {
	h := &handler{r: r, g: g, now: now}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/safe-to-run", h.safeToRun)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func coordinate(s string, limit float64) (float64, bool) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > limit {
		return 0, false
	}
	return v, true
}

func (h *handler) safeToRun(w http.ResponseWriter, req *http.Request) {
	lat, okLat := coordinate(req.URL.Query().Get("lat"), 90)
	lon, okLon := coordinate(req.URL.Query().Get("lon"), 180)
	if !okLat || !okLon {
		writeJSON(w, http.StatusBadRequest, errorBody{"invalid_coordinates"})
		return
	}
	if lat < minLat || lat > maxLat || lon < minLon || lon > maxLon {
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{"outside_coverage"})
		return
	}

	ctx := req.Context()
	region, err := h.r.NearestRegion(ctx, lat, lon)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{"data_unavailable"})
		return
	}
	pm, pmFound, err1 := h.r.Latest(ctx, "pm25_one_hourly", region)
	uvObs, uvFound, err2 := h.r.Latest(ctx, "uv_index", "national")
	psi, psiFound, err3 := h.r.Latest(ctx, "psi_twenty_four_hourly", region)
	if err1 != nil || err2 != nil || err3 != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{"data_unavailable"})
		return
	}

	now := h.now()
	verdict := guidance.Unknown("no_data")
	if pmFound {
		verdict = h.g.EvaluatePM25(pm.Value, pm.ObservedAt, now)
	}
	meta := h.g.Meta()
	resp := response{
		Status:     string(verdict.Status),
		Reason:     verdict.Reason,
		Healthy:    advice{string(verdict.Healthy.Headline), verdict.Healthy.Text},
		Vulnerable: advice{string(verdict.Vulnerable.Headline), verdict.Vulnerable.Text},
		Region:     regionBlock{Name: region, Method: "nearest_label_point", Approximate: true},
		UV:         uvBlock{Status: "unavailable"},
		Guidance:   guidanceBlock{meta.Version, meta.Source, meta.Retrieved},
		Disclaimer: disclaimer,
	}
	if pmFound {
		resp.PM25 = &pm25Block{
			Value: pm.Value, Unit: pm.Unit, Band: verdict.Band,
			ObservedAt:      pm.ObservedAt.In(sgt).Format(time.RFC3339),
			SourceUpdatedAt: pm.SourceUpdatedAt.In(sgt).Format(time.RFC3339),
			AgeMinutes:      ageMinutes(now, pm.ObservedAt),
		}
	}
	if uvFound && now.Sub(uvObs.ObservedAt) <= h.g.StaleAfter() {
		u := h.g.EvaluateUV(uvObs.Value)
		index := uvObs.Value
		resp.UV = uvBlock{Status: "ok", Index: &index, Band: u.Band,
			ObservedAt: uvObs.ObservedAt.In(sgt).Format(time.RFC3339), SunscreenRecommended: u.SunscreenRecommended}
	}
	if psiFound {
		resp.PSI24h = &psiBlock{Value: psi.Value, ObservedAt: psi.ObservedAt.In(sgt).Format(time.RFC3339)}
	}
	writeJSON(w, http.StatusOK, resp)
}

func ageMinutes(now, observed time.Time) int {
	if d := now.Sub(observed); d > 0 {
		return int(d.Minutes())
	}
	return 0
}
