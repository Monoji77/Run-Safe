// Package store reads current conditions and resolves coordinates to a reporting region.
package store

import (
	"context"
	"time"
)

type Observation struct {
	Source, Metric, Region string
	ObservedAt             time.Time
	SourceUpdatedAt        time.Time
	Value                  float64
	Unit                   string
}

// Reader is the API's read-only view of Gold.
type Reader interface {
	// Latest returns the newest observation for metric in region; found is false when there is none.
	Latest(ctx context.Context, metric, region string) (o Observation, found bool, err error)
	// NearestRegion returns the region whose label point is closest to (lat, lon). It is an approximation:
	// the source publishes one point per region, not boundaries.
	NearestRegion(ctx context.Context, lat, lon float64) (string, error)
}
