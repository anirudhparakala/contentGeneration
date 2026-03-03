# SPEC — STAGE 1: Ingestion (Discovery) — Manual Batch Run (Deterministic)

## Depends on
- Stage 0 only (docs/decisions.md, docs/config_schemas.md, config/pipeline.yaml, config/sources.yaml)

## Objective
Discover new content items from configured sources (newsletters via RSS/Atom feeds and YouTube via channel feeds) and emit only newly discovered "raw items" for downstream stages.

Manual batch run only. No scheduler.

## In Scope
- Read curated sources from `config/sources.yaml`.
- Read runtime defaults from `config/pipeline.yaml`:
  - `caps.max_entries_per_source`
  - `run_mode.recency_days` (mechanical filter only)
  - `http.*`
  - `paths.sqlite_db`
  - `paths.outputs_dir`
- Fetch and parse:
  - Newsletter RSS/Atom feeds from `newsletters[*].feed_url`
  - YouTube channel feeds constructed from `youtube[*].channel_id`:
    - `https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}`
- Normalize to RawItem schema (below).
- De-duplicate via SQLite `seen_items` table.
- Write outputs:
  - JSONL file of newly discovered RawItems for this run
  - JSON run report with per-source stats/errors

Output naming/defaults:
- Use `paths.outputs_dir` from `config/pipeline.yaml` for default output locations.
- Default filenames (when CLI overrides are not provided):
  - `raw_items_<YYYY-MM-DD>.jsonl`
  - `run_report_<YYYY-MM-DD>.json`
- If `--out` is provided, that exact path is used.
- If `--report` is provided, that exact path is used.

## Out of Scope
- Any semantic ranking, scoring, classification, topic extraction
- Any enrichment (article page fetch), transcript retrieval, ASR
- Any LLM usage
- Slack/email delivery
- Google Sheets/Airtable writes

## Repo Layout (Must Follow)
Implement code under:
- `app/ingest/models.py`   (RawItem model)
- `app/ingest/config.py`   (sources.yaml + pipeline.yaml parsing/validation)
- `app/ingest/fetch.py`    (HTTP fetch with timeouts + retries + size cap)
- `app/ingest/feeds.py`    (feedparser parsing to normalized entry fields)
- `app/ingest/state.py`    (SQLite seen registry)
- `app/ingest/runner.py`   (orchestrates ingestion per source, dedup, output, report)
- `app/ingest/cli.py`      (CLI entrypoint)
- `app/ingest/__init__.py`

## CLI Contract
Primary command:
- `python -m app.ingest.cli --config config/sources.yaml --pipeline config/pipeline.yaml --out data/outputs/raw_items.jsonl`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--report <path>` overrides default report path
- `--max-per-source <int>` overrides `caps.max_entries_per_source`
- `--recency-days <int>` overrides `run_mode.recency_days`
- `--log-level <LEVEL>` default INFO

Behavior:
- Overwrite `--out` each run.
- Emit only NEW items (not previously seen).
- Continue on per-source errors; do not fail entire run.
- Exit code:
  - 0 = completed (even if some sources failed)
  - 2 = fatal error (cannot read configs, cannot open DB, cannot write output)

Default report path:
- If `--report` not provided:
  - if `--out` is provided, write report to the same directory as `--out` with filename `run_report_<YYYY-MM-DD>.json`
  - else write report under `paths.outputs_dir` with filename `run_report_<YYYY-MM-DD>.json`

## Integration Hook (app.main)
- Daily run entrypoint `python -m app.main --run daily` must invoke Stage 1 ingestion with:
  - `config/sources.yaml`
  - `config/pipeline.yaml`
- Stage 1 should return run metadata needed by orchestration:
  - `run_id`
  - output artifact paths
  - top-level counts (`total_entries_parsed`, `total_new_items_emitted`, `sources_failed`)
- Per-source failures remain non-fatal.
- Fatal failures are limited to config read/validation, DB initialization, and output/report write failures.

## Source Config Contract (config/sources.yaml)
Schema:
- `newsletters`: list of:
  - `id` (string, unique globally)
  - `name` (string)
  - `feed_url` (http/https)
  - `tags` (optional list[str])
- `youtube`: list of:
  - `id` (string, unique globally)
  - `name` (string)
  - `channel_id` (non-empty)
  - `tags` (optional list[str])

Validation:
- `id` must be unique across newsletters + youtube.
- `feed_url` must start with http:// or https://.
- `channel_id` required and non-empty.

Also include in repo:
- `config/sources.example.yaml` (>=2 newsletters, >=2 youtube channels; public sources)

## RawItem Contract (JSONL output)
Each emitted line is a JSON object with:

Required fields:
- `source_type`: "newsletter" | "youtube"
- `source_id`: string
- `source_name`: string
- `creator`: string
  - newsletters: feed author if available else `source_name`
  - youtube: `source_name`
- `title`: string
- `url`: string (canonical)
- `published_at`: string (UTC ISO8601 with Z)
- `external_id`: string (stable id)
- `summary`: string (best-effort; may be empty)
- `fetched_at`: string (UTC ISO8601 with Z)

Deterministic rules:
- external_id:
  - use entry `id` or `guid` if present
  - else `sha256(url)` hex
- published_at precedence:
  - prefer entry published timestamp
  - else entry updated timestamp
  - else set to `fetched_at`
  - always normalize to UTC and serialize with `Z`
- url:
  - prefer entry.link
  - if missing, attempt alternate links
  - if still missing, skip entry and increment `entries_skipped_missing_url`
- title:
  - if missing/empty, skip entry and increment `entries_skipped_missing_title`
- creator:
  - if missing, fall back to `source_name`

## SQLite State (paths.sqlite_db)
Create/use SQLite DB. Must be idempotent across runs.

Table: `seen_items`
- `dedup_key` TEXT PRIMARY KEY
- `external_id` TEXT NOT NULL
- `url` TEXT NOT NULL
- `source_type` TEXT NOT NULL
- `source_id` TEXT NOT NULL
- `published_at` TEXT NOT NULL
- `first_seen_at` TEXT NOT NULL

Behavior:
- `dedup_key = sha256(f"{source_id}|{external_id}")` hex
- check by `dedup_key`
- if exists: skip + increment `entries_skipped_seen`
- if not exists: insert then emit item

## HTTP Fetch (requests) + Retries
Use settings from `config/pipeline.yaml` (CLI overrides allowed):
- headers:
  - `User-Agent: http.user_agent`
- timeouts:
  - connect: `http.connect_timeout_s`
  - read: `http.read_timeout_s`
- size cap:
  - max bytes = `http.max_response_mb * 1024 * 1024`
  - enforce via:
    - Content-Length if present AND
    - streaming read cap if Content-Length missing

Retries (tenacity) using `http.retries.max_attempts`:
- retry on network errors (ConnectionError/Timeout)
- retry on HTTP 429 and 5xx
- do not retry on other 4xx

## Parsing (feedparser)
- Parse RSS/Atom with feedparser for both newsletters and YouTube feeds.
- Apply mechanical recency filter if `recency_days` set:
  - ignore entries with `published_at` older than (now - recency_days)
- Ordering:
  - deterministic per-source sort: `published_at` desc, then `external_id` asc, then `url` asc
- Apply `max_entries_per_source` after ordering.

## Run Report (JSON)
Write JSON report with:
- `run_id` (uuid4)
- `started_at`, `finished_at` (UTC ISO8601 Z)
- `total_sources`, `sources_succeeded`, `sources_failed`
- `total_entries_parsed`, `total_new_items_emitted`
- `per_source`: array of:
  - `source_id`, `source_type`
  - `entries_parsed`
  - `entries_skipped_recency`
  - `entries_skipped_missing_url`
  - `entries_skipped_missing_title`
  - `entries_skipped_seen`
  - `new_items_emitted`
  - `error` (string|null)

Counter definitions:
- `entries_parsed`: total entries returned by feed parsing for that source before any Stage 1 filtering/skips.
- `entries_skipped_recency`: entries excluded by `recency_days`.
- `entries_skipped_missing_url`: entries skipped after normalization because canonical URL could not be resolved.
- `entries_skipped_missing_title`: entries skipped because title is missing/empty.
- `entries_skipped_seen`: entries skipped because `dedup_key` already exists in `seen_items`.
- `new_items_emitted`: items written to this run's JSONL output.
- `total_entries_parsed`: sum of `entries_parsed` over all sources.
- `total_new_items_emitted`: sum of `new_items_emitted` over all sources.

## Logging
Use Python logging.
- INFO: start/end run, per-source start/end, totals
- WARNING: aggregated skip counts per source
- ERROR: per-source failures with exception summary

## Tests (pytest) — Minimum
Add `tests/test_stage_1_ingest.py` with 3 tests:
1) Idempotency: same feed data ingested twice => second run emits 0 new items.
2) Failure isolation: one broken feed => other sources still succeed and output produced.
3) Schema: each emitted RawItem includes required fields, timestamps end with `Z`.

Network calls must be mocked (responses or requests-mock acceptable).

---

## Files Changed (Expected)
- `app/ingest/*` (new)
- `tests/test_stage_1_ingest.py` (new)
- `config/sources.example.yaml` (new)
- (No other stages)

## Commands to Run (Expected)
- `python -m app.ingest.cli --config config/sources.yaml --pipeline config/pipeline.yaml --out data/outputs/raw_items.jsonl`
- `pytest -q`

## Produced Artifacts
- `data/outputs/raw_items_<YYYY-MM-DD>.jsonl` (default if `--out` not provided)
- `data/outputs/run_report_<YYYY-MM-DD>.json` (default if `--report` not provided)
- `data/state.db`
