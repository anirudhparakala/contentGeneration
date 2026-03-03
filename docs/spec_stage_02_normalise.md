# SPEC - STAGE 2: Normalize (Canonical Item Model) - Manual Batch

## Depends on
- Stage 0 (decisions + config contracts)
- Stage 1 output exists and is provided via `--in`, OR discoverable under `paths.outputs_dir` as:
  - `raw_items_<YYYY-MM-DD>.jsonl` (preferred), or
  - `raw_items.jsonl` (fallback)

## Objective
Convert Stage 1 RawItems into a single canonical schema ("CanonicalItem") and persist them to SQLite.
Also emit a JSONL debug artifact of canonical items for this run.

Stage 2 is purely structural normalization + persistence. No LLMs, no enrichment, no ranking.

## In Scope
- Read runtime defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
- Read input RawItems from JSONL produced by Stage 1.
- Validate and normalize RawItems into CanonicalItems.
- Persist CanonicalItems into SQLite table `items` (idempotent).
- Write outputs:
  - `canonical_items_<YYYY-MM-DD>.jsonl`
  - `stage_2_report_<YYYY-MM-DD>.json`

## Out of Scope
- Semantic filtering (relevance), scoring, topic extraction
- Page fetch (newsletter content), transcript retrieval, ASR
- LLM usage
- Slack/email delivery
- Google Sheets writes

## Repo Layout (Must Follow)
Implement code under:
- `app/normalize/models.py`     (CanonicalItem model + validation helpers)
- `app/normalize/runner.py`     (load raw, normalize, persist, output, report)
- `app/normalize/state.py`      (SQLite helpers for items table; can reuse patterns from Stage 1)
- `app/normalize/cli.py`        (CLI entrypoint)
- `app/normalize/__init__.py`

NOTE:
- Stage 2 must NOT modify Stage 1 code except for shared helpers if strictly necessary.
- Prefer copying minimal SQLite helpers rather than refactoring Stage 1.

## CLI Contract
Primary command:
- `python -m app.normalize.cli --pipeline config/pipeline.yaml --in data/outputs/raw_items_<YYYY-MM-DD>.jsonl --out data/outputs/canonical_items_<YYYY-MM-DD>.jsonl`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--report <path>` overrides default report path
- `--log-level <LEVEL>` default INFO

Defaults:
- If `--in` not provided:
  - discover input using this deterministic order:
    1) latest file matching `{paths.outputs_dir}/raw_items_*.jsonl` by parsed date in filename (`YYYY-MM-DD`)
    2) if none found, `{paths.outputs_dir}/raw_items.jsonl`
  - files not matching the exact date pattern are ignored for dated discovery.
  - if no input is found by the above rules, fatal error with exit code 2.
- If `--out` not provided: use `{paths.outputs_dir}/canonical_items_<YYYY-MM-DD>.jsonl` where date is current UTC run date.
- If `--report` not provided: use `{paths.outputs_dir}/stage_2_report_<YYYY-MM-DD>.json` where date is current UTC run date.

Date basis note:
- Output/report filenames are always based on current UTC run date, not the discovered input filename date.
- Example: if input discovery selects `raw_items_2026-03-01.jsonl` and run date is `2026-03-03` UTC, defaults are:
  - `canonical_items_2026-03-03.jsonl`
  - `stage_2_report_2026-03-03.json`

Behavior:
- Overwrite `--out` each run.
- Continue on per-line errors in JSONL (skip bad lines; count them).
- Exit code:
  - 0 = completed (even if some lines were skipped)
  - 2 = fatal error (cannot read pipeline, cannot open DB, cannot read input, cannot write outputs)

## Input Contract: RawItem JSONL
Each line of input JSONL must be a JSON object with the following Stage 1 keys:
- `source_type` ("newsletter" | "youtube")
- `source_id` (string)
- `source_name` (string)
- `creator` (string)
- `title` (string)
- `url` (string)
- `published_at` (ISO8601 string)
- `external_id` (string)
- `summary` (string; may be empty)
- `fetched_at` (ISO8601 string)

Validation rules:
- Hard-required for item acceptance (missing or empty => skipped_invalid):
  - `external_id`, `url`, `title`, `published_at`, `fetched_at`
- Soft-required (if missing, normalize with defaults):
  - `source_type` default `"newsletter"`
  - `source_id` default `"unknown"`
  - `source_name` default `"unknown"`
  - `creator` default `source_name`
  - `summary` default `""`
- Timestamp validity:
  - `published_at` and `fetched_at` must be parseable ISO8601 values.
  - if either is unparseable, skip item as invalid timestamp.
- Type and structure rules:
  - blank lines are allowed and ignored (counted in `total_lines_read` only).
  - each non-empty line must parse as JSON object (not array/string/number/null).
  - all string fields are normalized by trimming surrounding whitespace before required/empty checks.
  - if a required field exists but is not a string, count as invalid field type and skip.
  - soft-required fields (`source_type`, `source_id`, `source_name`, `creator`, `summary`):
    - if missing: apply documented default.
    - if present but not a string: count as invalid field type and skip.
    - if present as string: trim, then apply any empty-string fallback rules below.
- Timestamp parsing/normalization:
  - Accept any ISO8601 string parseable by `python-dateutil` (`dateutil.parser.isoparse`).
  - If parsed datetime is timezone-aware, convert to UTC and serialize with trailing `Z`.
  - If parsed datetime is naive (no timezone), treat as UTC, then serialize with trailing `Z`.
  - If parse fails, count as `invalid_timestamps` and skip.

## CanonicalItem Contract
CanonicalItem is the normalized unit used for all later stages.

Fields:
- `item_id`: string
  - MUST equal `sha256("{source_id}|{external_id}")` hex after normalization.
  - Rationale: deterministic and delimiter-safe even if source values contain `|`.
- `external_id`: string
  - MUST equal RawItem.external_id after normalization
- `source_type`: "newsletter" | "youtube"
- `source_id`: string
- `source_name`: string
- `creator`: string
- `title`: string
- `url`: string
- `published_at`: string (ISO8601 UTC Z)
- `fetched_at`: string (ISO8601 UTC Z)
- `summary`: string
- `content_text`: string
  - default = `summary` (Stage 2 does not fetch full content)
- `raw_item_json`: object
  - store the original RawItem JSON object for traceability

Normalization rules:
- Trim whitespace for all string fields.
- Normalize accepted timestamps to UTC with trailing `Z`.
- `content_text` = `summary`.
- Normalize `source_type` to lowercase before validation.
- If `source_type` after normalization is not one of `newsletter|youtube`, set to `newsletter`.
- If `creator` is empty after trim, set `creator = source_name`.
- After defaults + trims, enforce non-empty normalized values:
  - `source_id`: if empty => `"unknown"`
  - `source_name`: if empty => `"unknown"`
  - `creator`: if empty => `source_name`

Deterministic output ordering:
- Canonical JSONL output ordering MUST match valid input encounter order.
- Do not sort canonical items before writing `--out`.
- For blank/invalid lines, preserve normal line processing order for counters, but only valid items are emitted.

`raw_item_json` semantics (trace payload):
- `raw_item_json` MUST store the exact parsed JSON object from the input line before any trimming/defaulting/canonical normalization.
- Do not mutate this stored object as part of normalization.
- Persist `raw_item_json` to SQLite as a JSON string of that exact pre-normalization object.

Timestamp canonicalization examples (normative):
- Input with offset:
  - `published_at = "2026-03-03T10:15:30+05:30"` -> canonical `"2026-03-03T04:45:30Z"`
- Naive datetime input (no timezone):
  - `fetched_at = "2026-03-03T10:15:30"` -> treat as UTC -> canonical `"2026-03-03T10:15:30Z"`
- Date-only input:
  - `published_at = "2026-03-03"` -> canonical `"2026-03-03T00:00:00Z"`
- Canonical serialization format MUST always be second precision UTC with trailing `Z`: `YYYY-MM-DDTHH:MM:SSZ`.

## SQLite Persistence
Use SQLite DB at `paths.sqlite_db` (or `--db` override).

Create table `items` if not exists:

Table: `items`
- `item_id` TEXT PRIMARY KEY
- `external_id` TEXT NOT NULL
- `source_type` TEXT NOT NULL
- `source_id` TEXT NOT NULL
- `source_name` TEXT NOT NULL
- `creator` TEXT NOT NULL
- `title` TEXT NOT NULL
- `url` TEXT NOT NULL
- `published_at` TEXT NOT NULL
- `fetched_at` TEXT NOT NULL
- `summary` TEXT NOT NULL
- `content_text` TEXT NOT NULL
- `raw_item_json` TEXT NOT NULL   (JSON string)
- `inserted_at` TEXT NOT NULL     (UTC ISO8601 Z)

Idempotency:
- If `item_id` already exists in `items`, do NOT insert again.
- Count as `items_skipped_already_present`.

Canonical JSONL emission vs DB insert:
- Emit every valid CanonicalItem to `--out` for this run, regardless of whether DB insert occurred.
- Therefore:
  - `canonical_items_emitted` = count of valid canonical rows written to output JSONL.
  - `items_inserted_db` = subset of emitted rows newly inserted into `items`.
  - `items_skipped_already_present` = subset of emitted rows not inserted due to existing `item_id`.

## Logging
Use Python logging.
- INFO: start/end run, totals
- WARNING: per-run aggregated counts for invalid lines / skipped reasons
- ERROR: fatal I/O or DB errors

## Stage 2 Report (JSON)
Write JSON report to `--report` (default `{outputs_dir}/stage_2_report_<YYYY-MM-DD>.json`).

Must include:
- `run_id` (uuid4)
- `started_at`, `finished_at` (UTC ISO8601 Z)
- `input_path`, `output_path`, `db_path`, `report_path`
- `total_lines_read`
- `total_raw_items_parsed`
- `canonical_items_emitted` (valid canonical items written to canonical JSONL)
- `items_inserted_db`
- `items_skipped_already_present`
- `items_skipped_invalid` (sum of invalid reasons)
- breakdown:
  - `invalid_json_lines`
  - `invalid_json_objects` (JSON parsed but not an object)
  - `missing_required_fields`
  - `invalid_field_types`
  - `invalid_timestamps`

Definitions:
- `canonical_items_emitted` counts only valid CanonicalItem rows written to output JSONL.
- Invalid items are not emitted to canonical JSONL; they are report-only.
- `total_lines_read` includes all lines, including blank lines.
- `total_raw_items_parsed` counts lines that successfully parse as JSON objects (before field validation).
- `items_skipped_invalid = invalid_json_lines + invalid_json_objects + missing_required_fields + invalid_field_types + invalid_timestamps`.

Invalid reason precedence (exactly one reason per skipped line):
1) `invalid_json_lines`: non-empty line that is not valid JSON.
2) `invalid_json_objects`: JSON parsed but top-level is not an object.
3) `invalid_field_types`: any required or soft-required field present with a non-string type.
4) `missing_required_fields`: after trim/default application, any hard-required field is missing/empty.
5) `invalid_timestamps`: all required fields present and typed, but `published_at` or `fetched_at` fails timestamp parse.

This precedence is mandatory to keep counters deterministic and to guarantee:
- each skipped line increments exactly one invalid breakdown counter;
- `items_skipped_invalid` equals the exact number of invalid lines skipped.

## Outputs
1) JSONL at `--out` containing only valid CanonicalItem objects processed in this run.
2) JSON report at `--report`.

## Tests (pytest) - Minimum
Add `tests/test_stage_2_normalize.py` with 3 tests using a small fixture JSONL:
1) Inserts new items into `items` table and emits canonical JSONL.
2) Idempotency: running twice does not create duplicates; second run counts `items_skipped_already_present`.
   - `canonical_items_emitted` remains equal to the number of valid input objects on both runs.
3) Invalid input handling:
   - bad JSON line
   - JSON non-object line
   - wrong type for required string field
   - missing required field
   - invalid timestamp
   All are skipped and counted correctly.

Tests must use a temporary SQLite db file and temporary output paths.

---

## Files Changed (Expected)
- `app/normalize/*` (new)
- `tests/test_stage_2_normalize.py` (new)

## Commands to Run (Expected)
- `python -m app.normalize.cli --pipeline config/pipeline.yaml --in data/outputs/raw_items_<YYYY-MM-DD>.jsonl --out data/outputs/canonical_items_<YYYY-MM-DD>.jsonl`
- `pytest -q`

## Produced Artifacts
- `data/outputs/canonical_items_<YYYY-MM-DD>.jsonl`
- `data/outputs/stage_2_report_<YYYY-MM-DD>.json`
- `{paths.sqlite_db}` (table `items` created/updated)
