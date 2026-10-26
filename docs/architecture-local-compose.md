# Run Safe SG: design spec

Design spec for review, 26 October 2026. Nothing has been deployed or configured. This replaces an earlier K3s-based proposal that is no longer in the repository.

The architecture diagram is `architecture-local-compose.drawio`; open it in draw.io to view or edit. It shows the ingestion, storage and serving pipeline. It does not yet show the alert delivery or remote access, which are undecided (see Open decisions).

## Purpose and scope

An app I can ask directly: **is it safe for me to run right now, wherever I am in Singapore?** It also tells me when the UV index is high enough to wear sunscreen.

**Motivation.**
- Checking air quality today means opening haze.gov.sg and clicking through to the latest reading each time. I want one question and one answer.
- What I see on haze.gov.sg updates hourly, while the data.gov.sg real-time API appears to refresh about every 15 minutes (see Findings). Fresher data lets me decide closer to when I head out.
- The same source publishes UV, so the app can also alert me to wear sunscreen.

This is a single-machine learning project: streaming ingestion, replay, idempotent loading, spatial queries and a Go-centric codebase. It is not production, does not claim high availability, and is not medical advice.

**Success criteria**
- Asking "safe to run here, now?" returns the official guidance band for my nearest reporting region, the reading and its observation time, and how fresh it is.
- If data is missing or stale, the answer is "unknown", never a safe-looking recommendation.
- The verdict is a lookup of official guidance, not a score invented by this project.
- A UV alert fires when the UV index crosses a configurable threshold.
- Re-running or replaying any input never duplicates or corrupts stored observations.
- Malformed records are never lost or silently dropped: they land in a DLQ with the reason and can be reprocessed.
- Region geometry can later support complex polygon queries without redesign.
- Every component runs on the free Community Edition of its software.

**Not goals:** medical advice, street-level accuracy (readings are regional), a combined health index, high availability.

## Decisions made in design discussion

| # | Decision | Why |
|---|---|---|
| 1 | Docker Compose on the laptop, not K3s | Simpler and lighter for one machine. Docker Desktop on Windows runs its engine in a WSL2 VM, so WSL2 remains; only the K3s control plane and system pods are saved. |
| 2 | Go for the fetcher and the API; no Python | Go-centric repository. |
| 3 | Redpanda Connect replaces the custom stream worker | Parse, validate, route and load are configuration, not custom code. |
| 4 | Bronze is the raw Redpanda topic with infinite retention; no separate file archive | The data volume is tiny; replay is a new consumer group from offset 0. |
| 5 | Two layers: Bronze (the raw topic) and Gold (cleaned observations, regions dimension, current-conditions views, rollups). No Silver layer. | The pipeline is simple enough that Connect cleans and writes straight to Gold; replay from Bronze covers rebuilds. Gold is a schema in one PostgreSQL database. (An earlier draft of this spec assumed three layers; that was an assumption, not a decision.) |
| 6 | PostgreSQL with PostGIS from the start | Polygon queries are an eventual requirement. |
| 7 | A DLQ topic for permanently rejected records | Reprocessing after a fix without losing the failure reason. |
| 8 | Backup and replication deferred until after the project | Accepted risk: Bronze is a single copy (see Known risks). |
| 9 | Community Edition only | Cost and licensing; see Licensing. |
| 10 | The product is a direct query ("safe to run here, now?") plus UV alerts, for use wherever I am | Stated project goal. Alert delivery and remote access are not yet designed. |

## Product behaviour (proposed; response contract and routes to be designed in the plan)

**Query.** The caller supplies a location (device coordinates, or a chosen place). The API resolves it to a reporting region, reads Gold, and returns:
- a headline verdict;
- the official advice for healthy and for vulnerable persons;
- the reasons: 1-hour PM2.5 value and band, region, observation time, source-updated time and age;
- a UV block: current index, band and sun-protection advice;
- a freshness state, and `unknown` when data is missing or stale.

**Verdict is a lookup, not a score.** It is derived from the official 1-hour PM2.5 bands and advice shown on haze.gov.sg (fetched 26 Oct 2026):

| 1-hr PM2.5 band (µg/m³) | Official advice, healthy persons | Official advice, vulnerable persons | Proposed headline for a run |
|---|---|---|---|
| Normal, 0–55 | Continue normal activities | Continue normal activities | OK |
| Elevated, 56–150 | Reduce strenuous outdoor activities for the next hour | Avoid strenuous outdoor activities for the next hour | Caution (healthy), Avoid (vulnerable) |
| High, 151–250 | Avoid strenuous outdoor activities for the next hour | Avoid all outdoor activities for the next hour; may wear a mask | Avoid |
| Very High, 251 and above | Minimise all outdoor activities for the next hour; may wear a mask | Avoid all outdoor activities for the next hour; may wear a mask | Avoid |

The thresholds and wording are stored as versioned configuration with their source and retrieval date, so a change in the official bands is a config change. The page does not define "vulnerable persons" in the text I retrieved. The 24-hour PSI is shown as context but does not drive the verdict. One search result stated that health advisories during transboundary haze are tied to the 24-hour PSI; confirm which NEA guidance applies to running before finalising.

**Location to region.** Browser or phone coordinates are mapped to the nearest of NEA's five reporting regions. Until polygons are loaded this is nearest label point, an approximation that the response must label. With polygons it becomes point-in-polygon. Region-level data cannot support street-level claims.

**Staleness.** A region's latest reading older than a threshold gives `unknown`. A reading stamped 15:00 stays the newest published until about 16:00, so the threshold must exceed an hour plus margin. The starting value (illustratively 90 minutes) is set after the cadence is confirmed.

**UV alert.** UV is published as hourly values with no regions (national). An alert fires when the current index reaches a configurable threshold. NEA's advice recommends sunscreen of at least SPF 30, shade, a hat and sunglasses, with extra care between 11am and 3pm, and reapplication about every two hours. UV bands as reported by a 2024 news article: Low 0–2, Moderate 3–5, High 6–7, Very High 8–10, Extreme 11 and above. Verify these on NEA's own page, and choose the alert threshold.

## Data flow

Arrows in the diagram show the direction data moves; labels say who starts the call.

1. **Poll.** The Go fetcher polls the data.gov.sg real-time APIs on a configurable schedule. It remembers the last `(timestamp, updatedTimestamp)` per endpoint and publishes only new or revised records, unchanged. After a restart it may republish one duplicate, which the upsert absorbs.
2. **Bronze.** Raw JSON goes to the `weather.bronze` topic with long (infinite) retention. The message key identifies source and observation timestamp. Every raw version is kept.
3. **Connect.** Redpanda Connect consumes Bronze, parses and validates, and writes typed rows.
4. **Gold.** Rows are upserted into `gold.observations` in PostgreSQL. A revision replaces an older row only if its source-updated timestamp is newer, so older events never overwrite newer state.
5. **DLQ.** A permanently invalid record is written to `weather.dlq` with the original payload, error reason, stage and source offset, and only then acknowledged. A temporary failure (for example PostgreSQL down) is never sent to the DLQ: the pipeline retries and applies back-pressure.
6. **Views.** SQL views, or scheduled SQL, build `gold.current_conditions` (latest per region and metric, with freshness) and later hourly or daily rollups. `gold.regions` is the region dimension.
7. **Serve.** The Go API reads Gold through a read-only role, resolves a location to a region, applies the guidance configuration, and returns the answer described under Product behaviour. It also serves the frontend bundle and the data for the map and trends.
8. **Map.** The browser fetches basemap tiles itself. The backend is not involved in tile delivery (see Open decisions).
9. **Boundaries.** A one-off Boundary load job loads region polygons into `gold.regions`, recording source and vintage.
10. **Alerts (not yet designed).** Something must watch the UV reading and notify me. The component and channel are open decisions.

This is at-least-once delivery with idempotent writes. It does not claim end-to-end exactly-once behaviour.

## Components

| Component | Compose form | Purpose |
|---|---|---|
| Go fetcher | service, 1 replica | Poll, dedupe, publish raw JSON |
| Redpanda broker | service, 1 broker, named volume | `weather.bronze` and `weather.dlq` topics |
| Redpanda Connect | service, Community bundle | Parse, validate, upsert Gold, route rejects |
| PostgreSQL + PostGIS | service, named volume | Gold schema |
| Go API + web UI | service | Safe-to-run query, Gold read API, embedded frontend |
| Boundary load | one-off job (`run` or profile) | Load region polygons into Gold |
| Alerting | undecided | UV (and possibly air-quality) notifications |

Keep the broker and database internal to the Compose network; publish only the API port. The frontend stack (a JavaScript bundle with a map library, or Go-rendered pages) is not decided.

## Data model (illustrative, to be refined in the plan)

- `gold.observations`: source, metric, region (nullable or `national` for UV), observed_at, source_updated_at, value, unit, averaging period, ingested_at, Bronze partition/offset, payload hash. Primary key `(source, metric, region, observed_at)`. Upsert `WHERE EXCLUDED.source_updated_at > observations.source_updated_at`. Earlier versions remain in Bronze, not in Gold.
- `gold.regions`: region id (north, south, east, west, central), label point (from the API), nullable `geometry(MultiPolygon, 4326)` filled by the boundary load, boundary source and vintage. GiST index on the polygon column. For metric area/distance work, transform to Singapore's projected CRS (SVY21, EPSG:3414, to be confirmed) or use `geography`.
- `gold.current_conditions`: latest per region and metric with computed freshness. Distinct metric timestamps are treated independently.
- A guidance configuration (versioned): PM2.5 band thresholds and advice text, UV bands, staleness limits, alert thresholds. It lives in the Go API's configuration or a small table; undecided.
- Ordinary btree index on `(region, metric, observed_at DESC)` serves "latest per region". GiST only pays off on polygons.

## Findings that shaped the design

**Source payload (observed 26 Oct 2026 by fetching the live APIs, not assumed).**
- The APIs return plain JSON. Each region has a name and one label point (latitude/longitude); there are no polygons. PM2.5 is `pm25_one_hourly`, the 1-hour PM2.5 reading that haze.gov.sg's activity bands use. PSI has several 24-hour and sub-index fields; UV is a list of hourly values with no regions.
- Records are stamped on the hour. One PM2.5 snapshot showed `updatedTimestamp` 00:45:32 for the 00:00 record; a later one showed 15:00:37 for the 15:00 record. Together with the dataset page's stated 15-minute refresh, this suggests an hourly-stamped record is republished about every 15 minutes. This is a hypothesis: log `updatedTimestamp` and a payload hash for a day or two to confirm, and check whether values change between revisions. The "hourly updates on haze.gov.sg" observation in the motivation is mine and is not verified; the haze.gov.sg page I retrieved shows update timestamps but does not state a refresh frequency.
- Polling every minute is only useful if unchanged records are skipped. Check documented rate limits and API-key requirements before fixing the schedule (not yet verified).

**Redpanda storage.** Redpanda's documentation requires an XFS or ext4 data directory and states NFS is unsupported; it does not mention NTFS, bind mounts or Docker on Windows. Inference: use a Docker named volume (ext4 inside the WSL2 VM). Do not bind-mount a Windows or OneDrive path for broker data.

**Location on a phone.** Browsers expose device location only in a secure context (HTTPS, or localhost). A phone opening the app over plain HTTP on the home network would not get location. Using the app from a phone therefore needs HTTPS, which interacts with the remote-access decision below. (From general knowledge of the Geolocation API, not re-verified here.)

## Licensing (free Community versions only)

| Component | License | Notes |
|---|---|---|
| Redpanda broker | Business Source License 1.1 | Free to self-host; source-available, **not** OSI open source. Production use is permitted unless offered as a streaming service; each release converts to Apache 2.0 after four years. Read from the repository's license file. |
| Redpanda Connect | Apache-2.0 (community bundle); Redpanda Community License for enterprise connectors | Needs only components in the community bundle (`io`, `kafka`, `sql`, `pure`, `redpanda` packages). Without a license, enterprise connectors are blocked. Confirm the Postgres driver ships in the binary you run. |
| PostgreSQL | PostgreSQL License | Not re-checked this session. |
| PostGIS | GPL v2 or later | Not re-checked this session. Used as a database extension over SQL; not expected to affect the Go code. |
| Go | BSD | Not re-checked this session. |
| Docker Engine / Compose | Apache-2.0 | Not re-checked this session. Docker Desktop is proprietary with paid terms for larger organisations; Docker Engine in WSL2 or Podman avoids that. |

Redpanda enterprise features (tiered storage, RBAC, audit logging, continuous data balancing, and others) must stay disabled. A new cluster gets a 30-day trial license; afterwards enterprise features are disabled or restricted. This design needs none. If strictly OSI-open-source software becomes a requirement, replace the broker (Apache Kafka or NATS JetStream are candidates). Any remote-access or notification service chosen later must also meet the free-version requirement.

## Known risks (accepted)

- **Bronze is a single copy** on one disk inside the WSL2 VM's virtual disk. A broker volume loss loses the raw history. Backup and replication are deferred by decision, not forgotten.
- **Durability defaults.** Before treating Bronze as the source of truth, check Redpanda's production-mode versus dev-container defaults for fsync/write caching. Not verified.
- **Safety framing.** The answer could be mistaken for medical advice or for a precise local reading. The UI must show the official advice wording, the region, the observation time and the "regional, not street-level" caveat, and must show `unknown` rather than guess.
- **Guidance drift.** Official bands and advice can change; hence versioned configuration with source and date.
- Region points are not polygons. Mapping NEA's five regions onto official boundaries is an approximation and the UI must say so.
- Laptop capacity is unmeasured. Set per-service memory limits and measure before tuning. No benchmark exists.
- **Availability.** If the laptop is off or asleep, the app cannot answer. Use away from home also depends on the remote-access decision.

## Open decisions

1. **Where I use it from.** Phone on home Wi-Fi only, or also on mobile data away from home. This decides remote access (for example a tunnel or VPN, or hosting off the laptop), HTTPS for phone geolocation, and whether the free-only constraint rules some options out.
2. **How I ask:** web page, command line, chat, or a mix.
3. **Alert delivery:** which channel, and which component watches UV.
4. **Verdict mapping:** whether Elevated is "Caution" or "Avoid" for me, and whether I want to set a "vulnerable" profile.
5. **UV alert threshold** and which UV bands to treat as authoritative (verify on NEA's page).
6. **Basemap:** keep OneMap tiles live (browser fetches directly; keep attribution) or self-host an OpenStreetMap-based basemap (PMTiles with MapLibre; ODbL attribution). Undecided; the diagram shows it as an open decision.
7. **Boundary source** and licence for region polygons, and how NEA's regions map to them.
8. **Frontend stack** (JavaScript bundle versus Go-rendered pages).
9. **Fetcher in Go versus Connect's HTTP polling input.** Go was chosen for the repository; Connect could poll in YAML instead.
10. **Polling schedule** after the cadence and rate limits are confirmed.
11. Whether any data stays out of version control (API keys, local volumes).

## Verification exercises for the later implementation

- Repeat an input and replay Bronze from offset 0: no duplicate Gold rows.
- Publish a revised record (same timestamp, newer `updatedTimestamp`): Gold holds the newer values; an older revision arriving later does not overwrite it.
- Deliver malformed records: they appear in `weather.dlq` with payload, reason and offset; fix the rule, replay, and confirm they load once.
- Stop PostgreSQL: lag rises, nothing is sent to the DLQ, and the backlog drains on recovery.
- Stop source updates: the query returns `unknown` after the staleness limit and never a safe-looking answer.
- For each PM2.5 band boundary (55/56, 150/151, 250/251), the query returns the documented band and advice.
- Query from several coordinates: each resolves to the expected region, and the response labels the approximation.
- Raise the UV reading past the threshold: exactly one alert fires per crossing.
- Restart each container: volumes survive and processing resumes.
- Insert a polygon into `gold.regions` and run a point-in-polygon query; confirm the GiST index is used.
- Compare source publication time, ingestion time and answer availability.

## Sources consulted

- Live data.gov.sg APIs (`api-open.data.gov.sg/v2/real-time/api/pm25`, `/psi`, `/uv`), fetched 26 Oct 2026.
- [haze.gov.sg](https://www.haze.gov.sg/): 1-hour PM2.5 bands and activity advice, retrieved 26 Oct 2026.
- [haze.gov.sg 1-hr PM2.5 readings](https://haze.gov.sg/resources/1-hr-pm2.5-readings) and [MOH: new personal guide based on 1-hr PM2.5](https://moh.gov.sg/newsroom/new-personal-guide-based-on-1-hr-pm25-concentration-readings-to-guide-the-public-during-haze-season) (search results; band thresholds confirmed from search snippets, advice wording read from haze.gov.sg).
- [NEA ultraviolet index](https://www.nea.gov.sg/weather/ultraviolet-index) (page exists; UV band thresholds were not shown in the results). UV bands and the SPF 30 advice came from search results citing [Mothership](https://mothership.sg/2024/03/extreme-uv-index-march-27//amp) and NEA/MOH advice; verify on NEA's page.
- [data.gov.sg PM2.5 dataset](https://data.gov.sg/datasets?formats=API&sort=relevancy&resultId=d_e1058d6974c877257e32048ab128ad83) (the 15-minute refresh was reported from this page; it is JavaScript-rendered and was not read directly).
- [Redpanda licensing overview](https://docs.redpanda.com/current/get-started/licensing/overview/)
- [Redpanda Connect licenses](https://github.com/redpanda-data/connect/tree/main/licenses) and its community component bundle
- [Redpanda broker license (BSL 1.1)](https://github.com/redpanda-data/redpanda/blob/dev/licenses/bsl.md)
- [Redpanda production requirements](https://docs.redpanda.com/current/deploy/redpanda/manual/production/requirements/)
- Redpanda Connect docs for the `file` output, `sql_raw` output and `broker` output (fan-out and acknowledgement behaviour).
- [Microsoft WSL configuration](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)
- [OneMap documentation](https://www.onemap.gov.sg/apidocs/)
