# data.gov.sg real-time API: limits and cadence

Checked 26 October 2026 against the [API rate limits guide](https://guide.data.gov.sg/developer-guide/api-overview/api-rate-limits)
and the [data.gov.sg developer guide](https://guide.data.gov.sg/llms-full.txt).

- **Rate limit, v2 Real-time API:** 6 calls per 10-second window without an API key; 12 with a dev key; 30 with a prod key.
  Limits reset every 10 seconds. Exceeding them returns HTTP 429.
- **API key header:** `x-api-key: <key>`. The fetcher sends it when `DATA_GOV_SG_API_KEY` is set.
- **Fetcher load:** three endpoints (pm25, psi, uv) polled every `FETCH_INTERVAL` (default 60s) is 3 calls per minute,
  far under the unauthenticated limit, so the default stays at 60s. A 429 is logged and retried on the next interval.
- **Cadence:** records are stamped on the hour and appear to be republished roughly every 15 minutes with a newer
  `updatedTimestamp` (unconfirmed). The fetcher logs one line per poll with the fingerprint
  (`timestamp|updatedTimestamp`) and whether it was published; read those logs for a day or two to confirm.
