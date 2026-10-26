# Run Safe SG Core Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest data.gov.sg PM2.5, PSI and UV readings into PostgreSQL/PostGIS through Redpanda and Redpanda Connect, and answer "is it safe to run here, now?" through a Go HTTP API.

**Architecture:** A Go fetcher publishes raw JSON to the `weather.bronze` topic (infinite retention). Redpanda Connect maps each message to typed rows and upserts them into `gold.observations`; permanently invalid messages go to `weather.dlq`. A Go API resolves coordinates to the nearest of five NEA regions, reads Gold through a read-only role, and applies the official 1-hour PM2.5 bands from a versioned config. Everything runs in Docker Compose on one machine.

**Tech Stack:** Go, franz-go (Kafka client), pgx, Redpanda (Community, BSL), Redpanda Connect (community bundle, Bloblang), PostgreSQL + PostGIS, Docker Compose.

**Spec:** `docs/architecture-local-compose.md`

**Out of scope (later plans):** boundary polygon load and point-in-polygon queries (only the nullable column and GiST index are created here), the mobile app/web UI, UV alert delivery (the API only reports `sunscreen_recommended`), remote access/HTTPS, basemap, backup, monitoring, and the final polling schedule beyond the default below.

## Global Constraints

- Free Community Edition software only; no Redpanda enterprise features (tiered storage, RBAC, audit logging, continuous data balancing).
- Go for all application code; no Python. Module path `github.com/Monoji77/Run-Safe`.
- Two layers only: Bronze (`weather.bronze`, infinite retention) and Gold (PostgreSQL schema `gold`). No Silver.
- Dead-letter topic is `weather.dlq`. A permanently invalid record is written to the DLQ before it is acknowledged. A temporary failure (for example PostgreSQL down) is never sent to the DLQ: the pipeline retries and applies back-pressure.
- `gold.observations` primary key `(source, metric, region, observed_at)`; upsert `WHERE EXCLUDED.source_updated_at > observations.source_updated_at`.
- Redpanda data lives on a Docker named volume, never a Windows or OneDrive bind mount. In the main Compose file the broker and database are internal; only the API port is published.
- At-least-once delivery with idempotent writes; never claim exactly-once.
- 1-hour PM2.5 bands (µg/m³): Normal 0–55, Elevated 56–150, High 151–250, Very High 251 and above.
- The verdict is a lookup of official advice, not an invented score. Stale or missing data gives `unknown`, never a safe-looking answer. Responses label the region as regional and approximate.
- Guidance thresholds and advice text live in a versioned config with source and retrieval date.
- Observation timestamps are Singapore time (+08:00) in the API and are stored as `timestamptz`.

## Review Focus

Failure modes the spec implies; each has a test in the named task.

1. An older revision (smaller `updatedTimestamp`) arriving after a newer one must not overwrite it (Task 5).
2. A payload containing a region outside the five (for example `northeast`) must go to the DLQ, never reach the database: a constraint violation there would be retried forever and stall the stream (Tasks 4, 5).
3. PM2.5 values exactly on band edges (55, 56, 150, 151, 250, 251) and non-integers (55.5) must land in the documented band; negative, NaN and infinite values are `unknown` (Task 3).
4. Missing, non-numeric, NaN or non-Singapore coordinates must return a 4xx error, not a region (Task 8).
5. UV at night (last record hours old, or an empty `index` list) must not crash or alert; it reports unavailable (Tasks 4, 8).

## File Structure

| Path | Responsibility |
|---|---|
| `deploy/compose.yaml`, `deploy/compose.dev.yaml` | Services; the dev override publishes loopback-only ports for tests |
| `deploy/postgres/init/` | Gold schema, seed data, roles |
| `deploy/connect/` | Pipeline config, Bloblang mapping, mapping unit tests, fixtures |
| `config/guidance.yaml` | Versioned PM2.5 and UV guidance |
| `internal/guidance/` | Pure verdict logic |
| `internal/fetcher/` | Polling, fingerprinting, Kafka publishing |
| `internal/replay/` | DLQ to Bronze replay |
| `internal/store/` | Read-only Gold queries and nearest-region lookup |
| `internal/api/` | HTTP handler and response contract |
| `cmd/fetcher`, `cmd/api`, `cmd/replay-dlq` | Binaries |
| `test/harness/` | Shared integration-test helpers |
| `test/infra`, `test/db`, `test/pipeline`, `test/e2e` | Integration tests (build tags below) |

Integration tests carry `//go:build integration` and read `RUNSAFE_KAFKA` (default `127.0.0.1:19092`) and `RUNSAFE_DB` (default from `.env.example`). Run them against the stack started with both Compose files.

---

### Task 1: Toolchain, Compose infrastructure and test harness

**Files:**
- Create: `go.mod`, `.gitignore`, `.env.example`, `deploy/compose.yaml`, `deploy/compose.dev.yaml`, `test/harness/harness.go`, `test/infra/infra_test.go`
- Modify: `README.md` (prerequisites and "run locally" commands)

**Interfaces:**
- Produces (`test/harness`): `Kafka(t *testing.T) *kgo.Client`; `DB(t *testing.T) *pgxpool.Pool` (superuser); `TruncateGold(t *testing.T)`; `PublishRaw(t *testing.T, source string, body []byte, fetchedAt time.Time) (partition int32, offset int64)` (publishes to `weather.bronze` with headers `source`, `fetched_at` RFC3339 UTC, `payload_sha256` hex, key `<source>:<payload timestamp or "unknown">`); `ReadDLQ(t *testing.T, n int, timeout time.Duration) []*kgo.Record`.

- [ ] **Step 1: Install and verify Go.** Go is not installed. Run `winget install GoLang.Go`, open a new shell, confirm `go version` prints a stable release, then `go mod init github.com/Monoji77/Run-Safe`.
- [ ] **Step 2: Write the failing infra test** in `test/infra/infra_test.go`: `TestBronzeTopicHasInfiniteRetention` (`weather.bronze` exists with `retention.ms=-1` and `write.caching=false`), `TestDLQTopicExists`, `TestPostGISAvailable` (`SELECT postgis_version()` succeeds), `TestBrokerAndDatabaseNotPublishedByMainCompose` (parse `docker compose -f deploy/compose.yaml config --format json` and assert only the `api` service, once it exists, has `ports`; before Task 8 assert none do).
- [ ] **Step 3: Run it to verify it fails.** `go test -tags=integration ./test/infra/... -v` fails (stack not running).
- [ ] **Step 4: Write the Compose files.** `redpanda` (pinned stable tag, single broker, data on named volume `redpanda-data`, no published ports), `init-topics` (one-shot `rpk topic create weather.bronze -c retention.ms=-1 -c retention.bytes=-1 -c write.caching=false` and `weather.dlq -c retention.ms=-1`; if `rpk` rejects `write.caching` on the pinned version, record that in the README known-risks list instead), `postgres` (pinned `postgis/postgis` tag, named volume `pgdata`, mounts `deploy/postgres/init`, no published ports). `compose.dev.yaml` publishes `127.0.0.1:19092` (Kafka external listener) and `127.0.0.1:15432` (Postgres). Secrets come from `.env` (git-ignored); commit `.env.example`.
- [ ] **Step 5: Implement the harness** with the signatures above (franz-go for Kafka, pgx for Postgres).
- [ ] **Step 6: Run the test to verify it passes.** `docker compose -f deploy/compose.yaml -f deploy/compose.dev.yaml up -d`, then the Step 3 command. Also run `docker compose exec redpanda rpk cluster license info` and confirm no enterprise feature is reported in use; note the result in the README.
- [ ] **Step 7: Commit.** `git add go.mod .gitignore .env.example deploy test README.md && git commit -m "feat: Compose infrastructure and integration harness"`

### Task 2: Gold schema, seed data and roles

**Files:**
- Create: `deploy/postgres/init/001_gold.sql`, `deploy/postgres/init/002_roles.sh`, `test/db/gold_test.go`
- Modify: `docs/architecture-local-compose.md` (data model: drop the separate `averaging` column; the period is encoded in the metric name)

**Interfaces:**
- Produces: table `gold.observations(source text, metric text, region text, observed_at timestamptz, source_updated_at timestamptz, value double precision, unit text NULL, ingested_at timestamptz, bronze_partition int, bronze_offset bigint, payload_sha256 text)`, all NOT NULL except `unit`, primary key `(source, metric, region, observed_at)`, no foreign key on `region` (UV uses `national`). Table `gold.regions(region text PK, label_point geometry(Point,4326) NOT NULL, boundary geometry(MultiPolygon,4326) NULL, boundary_source text NULL, boundary_vintage text NULL)` with GiST index `regions_boundary_gix` on `boundary`. View `gold.current_conditions` returning the row with the greatest `observed_at` per `(source, metric, region)`. Roles `connect_rw` (SELECT, INSERT, UPDATE on `gold.observations`) and `api_ro` (SELECT on all `gold`), passwords from `CONNECT_RW_PASSWORD` and `API_RO_PASSWORD`.
- Seed `gold.regions` (lat, lon): north (1.41803, 103.82), south (1.29587, 103.82), east (1.35735, 103.94), west (1.35735, 103.7), central (1.35735, 103.82). Build points with `ST_SetSRID(ST_MakePoint(lon, lat), 4326)`.

- [ ] **Step 1: Write the failing tests** in `test/db/gold_test.go`: `TestRegionsSeededWithLabelPoints` (five names, exact coordinates above), `TestObservationsPrimaryKeyRejectsDuplicate`, `TestBoundaryGISTIndexExists` (`pg_indexes` shows `regions_boundary_gix` using gist), `TestCurrentConditionsReturnsLatestPerKey` (two `observed_at` for one key, view returns the later), `TestAPIRoleIsReadOnly` (SELECT ok, INSERT denied), `TestConnectRoleCannotDelete`.
- [ ] **Step 2: Run to verify failure.** `go test -tags=integration ./test/db/... -v` (schema absent).
- [ ] **Step 3: Write the SQL and role script.** Run on an empty volume: `docker compose ... down -v && up -d`.
- [ ] **Step 4: Run to verify pass.** Same command.
- [ ] **Step 5: Commit.** `git commit -m "feat: gold schema, regions seed and roles"`

### Task 3: Guidance logic

**Files:**
- Create: `config/guidance.yaml`, `internal/guidance/guidance.go`, `internal/guidance/guidance_test.go`

**Interfaces:**
- Produces:
  - `type Status string` with values `"ok"`, `"caution"`, `"avoid"`, `"unknown"`.
  - `type Advice struct{ Headline Status; Text string }`.
  - `type PM25Verdict struct{ Status Status; Band string; Healthy, Vulnerable Advice; Reason string }` (`Status` is the healthy-person headline; `Reason` is set only for `unknown`).
  - `type UVVerdict struct{ Band string; SunscreenRecommended bool }`.
  - `func LoadConfig(path string) (Config, error)`; `func (c Config) EvaluatePM25(value float64, observedAt, now time.Time) PM25Verdict`; `func (c Config) EvaluateUV(index float64) UVVerdict`; `func (c Config) Meta() Meta` (`Version`, `Source`, `Retrieved` strings); `func (c Config) StaleAfter() time.Duration`; `func Unknown(reason string) PM25Verdict`.
- `config/guidance.yaml` holds: `version: "2026-10-26"`, `source: https://www.haze.gov.sg/`, `retrieved: "2026-10-26"`, PM2.5 bands with `max` upper bounds 55, 150, 250 and the last open-ended, the four advice pairs from the spec's verdict table, `stale_after: 90m`, UV bands (Low 0–2, Moderate 3–5, High 6–7, Very High 8–10, Extreme 11 and above) and `sunscreen_from: 3`.

Band lookup uses `value <= max`. Headlines: Normal gives ok/ok; Elevated gives caution (healthy) and avoid (vulnerable); High and Very High give avoid/avoid. Age is `now - observedAt`.

- [ ] **Step 1: Write the failing tests:** `TestPM25BandBoundaries` (table: 0→normal, 55→normal, 55.5→elevated, 56→elevated, 150→elevated, 151→high, 250→high, 250.5→very_high, 251→very_high), `TestHeadlinesPerBand` (healthy and vulnerable headline and exact advice text per band), `TestInvalidValuesAreUnknown` (-1, NaN, +Inf → status unknown, reason `invalid_value`), `TestStaleIsUnknown` (age 91m → unknown/`stale`; age 89m → evaluated), `TestUVBands` (2→low, 3→moderate, 5→moderate, 6→high, 8→very_high, 11→extreme), `TestSunscreenFromThreshold` (2 false, 3 true), `TestLoadConfigRejectsUnorderedBands`, `TestLoadConfigMissingFile`, `TestStaleAfterIs90Minutes`.
- [ ] **Step 2: Run to verify failure.** `go test ./internal/guidance/... -v`
- [ ] **Step 3: Implement** the interfaces above in `guidance.go`.
- [ ] **Step 4: Run to verify pass.** Same command.
- [ ] **Step 5: Commit.** `git commit -m "feat: PM2.5 and UV guidance"`

### Task 4: Connect mapping and its unit tests

**Files:**
- Create: `deploy/connect/mapping.blobl`, `deploy/connect/mapping_test.yaml`, `deploy/connect/testdata/{pm25.json,psi.json,uv.json,bad_json.txt,unknown_region.json,non_numeric.json,empty_items.json,empty_uv_index.json}`, `deploy/connect/pipeline.yaml` (stub that imports the mapping; completed in Task 5)
- Modify: `deploy/compose.yaml` (add `connect` service; its command `run /connect/pipeline.yaml`, so `docker compose run --rm connect test ...` overrides it)

**Interfaces:**
- Consumes: message value = raw API body; metadata `source` (`pm25`|`psi`|`uv`), `fetched_at`, `payload_sha256`, `kafka_partition`, `kafka_offset`.
- Produces: one message per row, JSON with exactly the `gold.observations` column names. Metric is the API field name (for example `pm25_one_hourly`, `psi_twenty_four_hourly`); UV rows use metric `uv_index` and region `national`; `unit` is `ug/m3` only for `pm25_one_hourly`, otherwise null. PM2.5 and PSI come from `data.items[0]` (`timestamp`→`observed_at`, `updatedTimestamp`→`source_updated_at`, `readings.<metric>.<region>`→value); UV comes from `data.records[0]` (each `index[]` entry: `hour`→`observed_at`, `value`; `updatedTimestamp` of the record→`source_updated_at`). Failure raises a processor error whose message starts with one of `invalid_json`, `unexpected_shape`, `unknown_region`, `non_numeric_value`, `value_out_of_range`. A message is all-or-nothing. Allowed regions: north, south, east, west, central. `value_out_of_range` means outside ±1e9.

Canonical PM2.5 fixture (`testdata/pm25.json`, captured 26 Oct 2026):

```json
{"code":0,"data":{"regionMetadata":[{"name":"north","labelLocation":{"latitude":1.41803,"longitude":103.82}},{"name":"south","labelLocation":{"latitude":1.29587,"longitude":103.82}},{"name":"east","labelLocation":{"latitude":1.35735,"longitude":103.94}},{"name":"west","labelLocation":{"latitude":1.35735,"longitude":103.7}},{"name":"central","labelLocation":{"latitude":1.35735,"longitude":103.82}}],"items":[{"date":"2026-10-26","updatedTimestamp":"2026-10-26T00:45:32+08:00","timestamp":"2026-10-26T00:00:00+08:00","readings":{"pm25_one_hourly":{"north":25,"south":21,"west":39,"east":33,"central":27}}}]},"errorMsg":""}
```

Capture `psi.json` and `uv.json` live with `curl https://api-open.data.gov.sg/v2/real-time/api/{psi,uv}`.

- [ ] **Step 1: Write the failing mapping tests** in `mapping_test.yaml` (Connect's unit-test format, `file_path` inputs): `pm25_yields_five_rows` (5 messages; the `west` row has metric `pm25_one_hourly`, value 39, `observed_at` `2026-10-26T00:00:00+08:00`, `source_updated_at` `2026-10-26T00:45:32+08:00`, unit `ug/m3`, `source` `pm25`); `psi_yields_sixty_rows` (12 metrics × 5 regions); `uv_yields_one_national_row_per_index_entry` (count equals the fixture's `index` length, `observed_at` equals each `hour`, `source_updated_at` equals the record's `updatedTimestamp`); `empty_uv_index_is_unexpected_shape`; `bad_json_is_invalid_json`; `empty_items_is_unexpected_shape`; `unknown_region_rejects_whole_message`; `non_numeric_value_is_rejected`; `huge_value_is_out_of_range`.
- [ ] **Step 2: Run to verify failure.** `docker compose -f deploy/compose.yaml run --rm connect test /connect/mapping_test.yaml`
- [ ] **Step 3: Write `mapping.blobl`** per the contract above. Also run `docker compose ... run --rm connect lint /connect/pipeline.yaml` and fix findings.
- [ ] **Step 4: Run to verify pass.** Same test command.
- [ ] **Step 5: Commit.** `git commit -m "feat: Connect mapping for PM2.5, PSI and UV"`

### Task 5: Pipeline wiring with upsert and DLQ

**Files:**
- Modify: `deploy/connect/pipeline.yaml`, `deploy/compose.yaml` (`connect` env for the `connect_rw` DSN, `depends_on` healthy `redpanda` and `postgres`)
- Create: `test/pipeline/pipeline_test.go`

**Interfaces:**
- Consumes: Task 4 mapping; Task 2 tables and `connect_rw`; Task 1 harness.
- Produces: consumer group `connect-gold` on `weather.bronze`. Output is a `switch`: errored messages go to `weather.dlq` unchanged in value, with headers `error` (full message), `stage` (`mapping`), `source_partition`, `source_offset`, plus the original `source`, `fetched_at`, `payload_sha256`; all other messages are upserted in batches by one `sql_raw` statement implementing the Global Constraints upsert. Errors from the SQL output are retried by Connect and are never routed to the DLQ.

- [ ] **Step 1: Write the failing tests** in `test/pipeline/pipeline_test.go` (each starts with `TruncateGold`; assertions poll up to 30s): `TestPM25BecomesFiveGoldRows` (west=39); `TestReplayingSameMessageIsIdempotent` (publish twice, still five rows); `TestNewerRevisionOverwrites` (same `timestamp`, `updatedTimestamp` 00:59:59, west=41 → west=41 and `source_updated_at` 00:59:59); `TestOlderRevisionArrivingLaterDoesNotOverwrite` (publish the 00:59:59 revision first, then the 00:45:32 one → west stays 41); `TestMalformedJSONGoesToDLQWithContext` (body `{not json` → one DLQ record, identical value, `error` contains `invalid_json`, `stage`=`mapping`, `source_partition` and `source_offset` equal to those returned by `PublishRaw`; Gold empty); `TestUnknownRegionGoesToDLQ` (fixture plus `"northeast":50`; zero rows from that message); `TestNonNumericValueGoesToDLQ`.
- [ ] **Step 2: Run to verify failure.** `go test -tags=integration ./test/pipeline/... -v`
- [ ] **Step 3: Complete `pipeline.yaml`** and start the stack: `docker compose -f deploy/compose.yaml -f deploy/compose.dev.yaml up -d connect`.
- [ ] **Step 4: Run to verify pass.** Same command as Step 2.
- [ ] **Step 5: Commit.** `git commit -m "feat: Connect pipeline with idempotent upsert and DLQ"`

### Task 6: DLQ replay

**Files:**
- Create: `internal/replay/replay.go`, `internal/replay/replay_test.go`, `cmd/replay-dlq/main.go`

**Interfaces:**
- Produces: `func Replay(ctx context.Context, cl *kgo.Client, group string) (int, error)` consumes unconsumed records from `weather.dlq` for consumer group `group` and republishes each to `weather.bronze` with the original key, value and headers `source`, `fetched_at`, `payload_sha256`, plus header `replayed_from_dlq_offset`; it commits the group offset only after the produce is acknowledged and returns the count replayed. `cmd/replay-dlq` calls it with `KAFKA_BROKERS` and group `replay-dlq`.

- [ ] **Step 1: Write the failing integration test** `TestReplayRepublishesDLQRecordAndGoldLoads` (put the valid PM2.5 fixture directly into `weather.dlq` with the three source headers; `Replay` returns 1; a Bronze record with the same value and `replayed_from_dlq_offset` exists; five Gold rows appear) and `TestReplayTwiceReplaysNothingNew` (second call returns 0).
- [ ] **Step 2: Run to verify failure.** `go test -tags=integration ./internal/replay/... -v`
- [ ] **Step 3: Implement** `Replay` and the command.
- [ ] **Step 4: Run to verify pass.** Same command.
- [ ] **Step 5: Commit.** `git commit -m "feat: replay DLQ records into Bronze"`

### Task 7: Fetcher

**Files:**
- Create: `internal/fetcher/{fingerprint.go,poller.go,kafka.go,config.go}` with `_test.go` files, `cmd/fetcher/main.go`, `deploy/Dockerfile.fetcher`, `docs/data-source.md`
- Modify: `deploy/compose.yaml` (add `fetcher`, depends on healthy `redpanda`)

**Interfaces:**
- Produces:
  - `type Record struct{ Topic string; Key, Value []byte; Headers map[string]string }`; `type Publisher interface{ Publish(ctx context.Context, r Record) error }`.
  - `func Fingerprint(endpoint string, body []byte) string`: for `pm25` and `psi` returns `<data.items[0].timestamp>|<data.items[0].updatedTimestamp>`; for `uv` the same from `data.records[0]`; if either cannot be extracted returns `sha256:<hex of body>`.
  - `type Endpoint struct{ Name, URL string }`; `func NewPoller(client *http.Client, pub Publisher, topic, apiKey string, now func() time.Time) *Poller`; `func (p *Poller) PollOnce(ctx context.Context, ep Endpoint) (published bool, err error)`; `func Run(ctx context.Context, p *Poller, eps []Endpoint, interval time.Duration)`.
- Behaviour: publish the response body unchanged when its fingerprint differs from the last one seen for that endpoint (in memory). Only HTTP 200 bodies are published, including invalid JSON, so they reach the DLQ. Record headers `source`, `fetched_at`, `payload_sha256`; key `<endpoint>:<data timestamp or "unknown">`. Non-200 responses return an error and publish nothing. If `apiKey` is non-empty send it as header `x-api-key`. Log one structured line per poll with endpoint, fingerprint and `published`, so the real update cadence can be confirmed from logs. Env: `FETCH_INTERVAL` (default `60s`), `DATA_GOV_SG_API_KEY` (optional), `KAFKA_BROKERS`. Endpoints: `https://api-open.data.gov.sg/v2/real-time/api/{pm25,psi,uv}`.

- [ ] **Step 1: Write the failing tests** (`httptest` server, fake `Publisher`): `TestUnchangedResponsePublishesOnce`; `TestRevisedUpdatedTimestampPublishesAgain`; `TestInvalidJSONPublishedOnceThenDeduped` (hash fallback); `TestNon200PublishesNothingAndReturnsError` (429 and 500); `TestHeadersAndKey` (pm25 fixture gives key `pm25:2026-10-26T00:00:00+08:00` and the three headers); `TestAPIKeyHeaderSentWhenConfigured`; `TestRunStopsOnContextCancel`; `TestFingerprintPerEndpointShape` (pm25, psi, uv, garbage).
- [ ] **Step 2: Run to verify failure.** `go test ./internal/fetcher/... -v`
- [ ] **Step 3: Implement** the interfaces; the Kafka `Publisher` uses franz-go with idempotent produce and waits for the ack.
- [ ] **Step 4: Run to verify pass.** Same command.
- [ ] **Step 5: Record rate limits.** Read data.gov.sg's real-time API documentation for rate limits and API-key requirements and write them to `docs/data-source.md`. If the documented unauthenticated limit is below 3 requests per minute, change the default `FETCH_INTERVAL` to `5m`; otherwise keep `60s`.
- [ ] **Step 6: Build and smoke-run.** `docker compose -f deploy/compose.yaml -f deploy/compose.dev.yaml up -d fetcher`; within two intervals `rpk topic consume weather.bronze -n 3` shows messages with the three headers and `gold.observations` gains rows.
- [ ] **Step 7: Commit.** `git commit -m "feat: data.gov.sg fetcher with fingerprint dedupe"`

### Task 8: Store, region lookup and API

**Files:**
- Create: `internal/store/{store.go,pg.go}`, `internal/api/{handler.go,response.go}`, tests for each, `cmd/api/main.go`, `deploy/Dockerfile.api`
- Modify: `deploy/compose.yaml` (add `api` publishing `8080`, `api_ro` DSN, mounts `config/guidance.yaml`)

**Interfaces:**
- Consumes: Task 3 `guidance.Config`; Task 2 views and `api_ro`.
- Produces:
  - `type Observation struct{ Source, Metric, Region string; ObservedAt, SourceUpdatedAt time.Time; Value float64; Unit string }`.
  - `type Reader interface{ Latest(ctx context.Context, metric, region string) (Observation, bool, error); NearestRegion(ctx context.Context, lat, lon float64) (string, error) }`; `func NewPG(pool *pgxpool.Pool) Reader`. `NearestRegion` orders `gold.regions` by `label_point <-> ST_SetSRID(ST_MakePoint(lon,lat),4326)`.
  - `func NewHandler(r store.Reader, g guidance.Config, now func() time.Time) http.Handler` serving `GET /v1/safe-to-run?lat=&lon=` and `GET /healthz`.
- Response (JSON): `status` (healthy-person headline), `reason` (only when `unknown`: `stale`, `no_data`, `invalid_value`), `healthy` and `vulnerable` (`{headline, advice}`), `pm25` (`value`, `unit`, `band`, `observed_at`, `source_updated_at`, `age_minutes`), `region` (`name`, `method: "nearest_label_point"`, `approximate: true`), `uv` (`status: "ok"|"unavailable"`, `index`, `band`, `observed_at`, `sunscreen_recommended`), `psi_24h` (`value`, `observed_at`, omitted if absent), `guidance` (`version`, `source`, `retrieved`), `disclaimer` (regional readings, not medical advice). PM2.5 metric `pm25_one_hourly` in the resolved region; UV metric `uv_index` in region `national`; PSI metric `psi_twenty_four_hourly`. UV older than the config's `stale_after` is `unavailable` with `sunscreen_recommended: false`.
- HTTP: 200 for every successful lookup including `unknown`; 400 for missing, non-numeric or NaN `lat`/`lon` (`{"error":"invalid_coordinates"}`); 422 outside latitude 1.15–1.48 and longitude 103.55–104.10 (`{"error":"outside_coverage"}`); 503 if the store errors (`{"error":"data_unavailable"}`).

- [ ] **Step 1: Write the failing handler tests** (fake `Reader`, fixed clock): `TestNormalBandIsOK`, `TestElevatedIsCautionForHealthyAvoidForVulnerable`, `TestHighIsAvoid`, `TestStaleReadingIsUnknown`, `TestNoObservationIsUnknownNoData`, `TestMissingLatLonIs400`, `TestNonNumericAndNaNAre400`, `TestOutsideSingaporeIs422` (51.5,-0.12), `TestStoreErrorIs503`, `TestUVSunscreenAndUnavailable` (index 5 recommended; stale UV unavailable), `TestResponseLabelsRegionApproximate`, `TestGuidanceMetadataIncluded`.
- [ ] **Step 2: Write the failing store tests** (integration): `TestNearestRegionFiveLocations` (Woodlands 1.4382,103.7890→north; Jurong East 1.3329,103.7436→west; Tampines 1.3496,103.9568→east; Marina Bay 1.2800,103.8510→south; Bishan 1.3526,103.8352→central) and `TestLatestReturnsNewest`.
- [ ] **Step 3: Run to verify failure.** `go test ./internal/api/... -v` and `go test -tags=integration ./internal/store/... -v`
- [ ] **Step 4: Implement** the interfaces; `cmd/api` reads `API_DSN`, `GUIDANCE_PATH`, `LISTEN_ADDR` (default `:8080`).
- [ ] **Step 5: Run to verify pass.** Same two commands, then `docker compose ... up -d api` and `curl "http://127.0.0.1:8080/v1/safe-to-run?lat=1.3526&lon=103.8352"` returns JSON with `region.name` `central`.
- [ ] **Step 6: Update `TestBrokerAndDatabaseNotPublishedByMainCompose`** in `test/infra` so only `api` may publish ports, and run it.
- [ ] **Step 7: Commit.** `git commit -m "feat: safe-to-run API over Gold"`

### Task 9: End-to-end verification and README

**Files:**
- Create: `test/e2e/e2e_test.go` (build tag `e2e`)
- Modify: `README.md` (run locally, known risks, verification commands)

**Interfaces:** Consumes the full stack and harness.

- [ ] **Step 1: Write the end-to-end tests:** `TestReplayFromOffsetZeroCreatesNoDuplicates` (stop `connect`, `rpk group seek connect-gold --to-start`, start it, row count unchanged); `TestPostgresOutageDrainsWithoutDLQ` (stop `postgres`, publish a valid PM2.5 message, assert `weather.dlq` stays empty, start `postgres`, row appears); `TestStaleDataGivesUnknownNotSafe` (insert a PM2.5 row observed 3 hours ago, API returns `unknown`/`stale`); `TestVolumesSurviveRestart` (restart `redpanda` and `postgres`; Bronze message count and Gold rows unchanged); `TestGISTIndexUsedForPointInPolygon` (insert one `MultiPolygon` into `gold.regions.boundary`, `SET enable_seqscan=off`, `EXPLAIN` of `ST_Intersects(boundary, point)` mentions `regions_boundary_gix`; roll back the insert).
- [ ] **Step 2: Run to verify.** `go test -tags=e2e ./test/e2e/... -v -timeout 15m` passes against the full stack.
- [ ] **Step 3: Run the whole suite.** `go vet ./... && go test ./...` and the three integration packages; all pass.
- [ ] **Step 4: Update README** with the prerequisite (Go via winget, Docker), the Compose commands for both files, the test commands by tag, and the known risks carried from the spec (single-copy Bronze, durability defaults and the `write.caching` result, region approximation).
- [ ] **Step 5: Commit.** `git commit -m "test: end-to-end verification and run instructions"`

---

## Self-review notes

- **Spec coverage:** data flow steps 1–7 map to Tasks 7, 5, 4, 2, 3, 8; DLQ and replay to Tasks 5–6; staleness, location mapping and UV to Tasks 3 and 8; spec verification exercises map to Tasks 3, 5, 8, 9. Deferred by design: boundary load (step 9), map/UI (step 8), alert delivery (step 10), UV "one alert per crossing" test, basemap, backup, timing comparison.
- **Spec edits made by this plan:** Task 2 drops the `averaging` column from the data model. Task 7 may change the default polling interval after the rate-limit check.
