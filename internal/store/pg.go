package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pg struct{ pool *pgxpool.Pool }

// NewPG returns a Reader backed by PostgreSQL.
func NewPG(pool *pgxpool.Pool) Reader { return &pg{pool: pool} }

func (p *pg) Latest(ctx context.Context, metric, region string) (Observation, bool, error) {
	var o Observation
	err := p.pool.QueryRow(ctx, `
		SELECT source, metric, region, observed_at, source_updated_at, value, COALESCE(unit, '')
		FROM gold.current_conditions
		WHERE metric = $1 AND region = $2`, metric, region).
		Scan(&o.Source, &o.Metric, &o.Region, &o.ObservedAt, &o.SourceUpdatedAt, &o.Value, &o.Unit)
	if errors.Is(err, pgx.ErrNoRows) {
		return Observation{}, false, nil
	}
	if err != nil {
		return Observation{}, false, err
	}
	return o, true, nil
}

func (p *pg) NearestRegion(ctx context.Context, lat, lon float64) (string, error) {
	var region string
	err := p.pool.QueryRow(ctx, `
		SELECT region FROM gold.regions
		ORDER BY label_point <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
		LIMIT 1`, lon, lat).Scan(&region)
	return region, err
}
