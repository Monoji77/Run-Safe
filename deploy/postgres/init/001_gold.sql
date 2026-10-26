CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA gold;

CREATE TABLE gold.observations (
    source            text             NOT NULL,
    metric            text             NOT NULL,
    region            text             NOT NULL,
    observed_at       timestamptz      NOT NULL,
    source_updated_at timestamptz      NOT NULL,
    value             double precision NOT NULL,
    unit              text,
    ingested_at       timestamptz      NOT NULL,
    bronze_partition  integer          NOT NULL,
    bronze_offset     bigint           NOT NULL,
    payload_sha256    text             NOT NULL,
    PRIMARY KEY (source, metric, region, observed_at)
);

CREATE TABLE gold.regions (
    region            text PRIMARY KEY,
    label_point       geometry(Point, 4326) NOT NULL,
    boundary          geometry(MultiPolygon, 4326),
    boundary_source   text,
    boundary_vintage  text
);

CREATE INDEX regions_boundary_gix ON gold.regions USING GIST (boundary);

INSERT INTO gold.regions (region, label_point) VALUES
    ('north',   ST_SetSRID(ST_MakePoint(103.82, 1.41803), 4326)),
    ('south',   ST_SetSRID(ST_MakePoint(103.82, 1.29587), 4326)),
    ('east',    ST_SetSRID(ST_MakePoint(103.94, 1.35735), 4326)),
    ('west',    ST_SetSRID(ST_MakePoint(103.70, 1.35735), 4326)),
    ('central', ST_SetSRID(ST_MakePoint(103.82, 1.35735), 4326));

CREATE VIEW gold.current_conditions AS
SELECT DISTINCT ON (source, metric, region) *
FROM gold.observations
ORDER BY source, metric, region, observed_at DESC;
