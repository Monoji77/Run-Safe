# Run Safe SG

An app I can ask directly: **is it safe for me to run right now, wherever I am in Singapore?** It also tells me when the UV index is high enough that I should put on sunscreen.

**Status: design only.** Nothing is built or deployed yet.

## Why I'm building this

- Checking air quality means opening haze.gov.sg and clicking through to the latest reading every time. I want to ask one question and get an answer.
- What I see on haze.gov.sg updates hourly, while the data.gov.sg real-time APIs appear to refresh about every 15 minutes. I want the fresher data so I can decide closer to when I head out. The 15-minute cadence is consistent with what I've seen in the live API but is not yet confirmed (see the spec).
- The same data source also publishes the UV index, so the app can alert me to wear sunscreen, not just tell me whether the air is OK.

## What it should do

- Answer "safe to run here, now?" for my current location (or a place I choose), using the nearest reporting region's latest PSI and PM2.5.
- Show the reasons behind the answer, the observation time and how fresh the data is. If data is missing or stale, say "unknown" rather than guess.
- Send a sunscreen alert based on the UV index.
- Keep the history so trends can be shown later.

**What it deliberately is not:** medical advice, an invented health score, or street-level accuracy. The readings are regional (north, south, east, west, central), so the answer is only as precise as that. The verdict is meant to be derived from current official guidance, which I still need to review.

## Planned architecture

Everything runs in Docker Compose on one machine:

- **Go fetcher** polls the data.gov.sg real-time APIs and publishes new or revised raw JSON.
- **Redpanda** holds the raw records in a `weather.bronze` topic with infinite retention, plus a `weather.dlq` topic for rejected records.
- **Redpanda Connect** validates records and upserts them into PostgreSQL.
- **PostgreSQL + PostGIS** holds the `gold` schema: cleaned observations, regions and current-conditions views. There is no Silver layer.
- **Go API and web UI** read Gold and serve the answers and the map frontend.

Only free Community Edition software is planned. Note that the Redpanda broker is under the Business Source License (free to self-host, but not OSI open source).

## Documentation

- [`docs/architecture-local-compose.md`](docs/architecture-local-compose.md): design spec with data flow, data model, licensing, risks, open decisions and verification exercises.
- [`docs/architecture-local-compose.drawio`](docs/architecture-local-compose.drawio): architecture diagram; open it in draw.io.

The spec covers the "safe to run here?" query, location lookup and UV alerts as requirements. Alert delivery and use away from home are not yet designed and are listed as open decisions.
## Open decisions

- How I ask the question: web page, command line, chat, or a mix.
- How alerts are delivered (phone notification or something else), and how I use the app away from home, since the current design assumes the home network.
- How a location maps to a reporting region, and which official guidance defines "safe to run".
- Basemap (OneMap tiles or self-hosted), the source of region boundary polygons, the frontend stack, and the polling schedule.

## Run locally

Prerequisites: Docker Desktop (Compose v2) and, for editing, Go (`winget install GoLang.Go`).

```
cp .env.example .env
docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.dev.yaml up -d
```

The dev override publishes loopback-only ports so tests can reach Redpanda and Postgres. The main file `deploy/compose.yaml` publishes nothing except the API (once it exists).

Run Go commands through `scripts/dgo`, which uses a Linux Go container attached to the Compose network. Native `go test` binaries are blocked on machines where Windows Smart App Control is enforcing.

```
scripts/dgo test ./...                                                  # unit tests
scripts/dgo test -tags=integration ./...                                # integration tests against the running stack
docker compose --env-file .env -f deploy/compose.yaml run --rm --no-deps connect test /connect/pipeline_benthos_test.yaml   # pipeline mapping tests
DGO_DOCKER_SOCKET=1 scripts/dgo test -tags=e2e ./test/e2e/... -v       # end-to-end (stops and starts containers)
```

`scripts/dgo` runs test packages one at a time (`-p 1`) because the integration and e2e tests share one database and broker.
Integration tests empty `gold.observations`; restart the fetcher afterwards to refill it with current readings
(`docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.dev.yaml restart fetcher`).
The e2e suite mounts the Docker socket into the test container, which gives it control of your local Docker engine.

### Try it

With the stack up, ask whether it is safe to run at a location (latitude and longitude, Singapore only):

```
curl "http://127.0.0.1:8080/v1/safe-to-run?lat=1.3329&lon=103.7436"
```

The answer is the official 1-hour PM2.5 band advice for the nearest of the five NEA regions, the reading and its age,
the UV index with a sunscreen flag, and `unknown` (never a safe-looking answer) when data is missing or stale.
The API is bound to `127.0.0.1` only until remote access is designed. See `docs/data-source.md` for the API limits.

## Known risks (carried from the spec)

- Bronze is a single copy on one disk; backup is deferred.
- Redpanda runs with `--mode dev-container`. `weather.bronze` is created with `write.caching=false` (accepted by the pinned v26.2.6); confirm durability before treating Bronze as the source of truth.
- Redpanda's built-in trial licence enables continuous balancing by default. Compose pins `partition_autobalancing_mode=node_add` and `core_balancing_continuous=false`, and a test checks it, so no enterprise feature is in use.
- Region lookup is nearest label point: regional and approximate.
- The update cadence (about every 15 minutes) is still a hypothesis; the fetcher logs each poll so it can be confirmed.
