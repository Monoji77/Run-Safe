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
