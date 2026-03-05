# SPEC - STAGE 7: Persist (Google Sheets "Ideas CRM") - Manual Batch

## Depends on
- Stage 0
- Stage 2 (`items` table exists and is schema-compatible; table may be empty)
- Stage 5 (`ideas` table exists and is schema-compatible; table may be empty)
- Stage 6 (`scripts` table exists and is schema-compatible; table may be empty)

## Objective
Persist Stage 5 ideas plus Stage 6 script drafts into a Google Sheet worksheet for human review.

Required business columns in the worksheet are:
- item_id
- creator
- post_link
- topic
- viral_rating
- hook
- platform
- draft_script
- status

Stage 7 is a persistence/export stage. No LLM calls.

## In Scope
- Read defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - `sheets.*`
  - `stage_7_persist.*`
- Read inputs from SQLite `ideas`, `scripts`, and `items`.
- Select rows in deterministic order with max-row cap.
- Upsert rows into one worksheet using `item_id` as unique key.
- Preserve analyst-managed columns on update (`status`, `notes`).
- Emit Stage 7 report JSON.

## Out of Scope
- Stage 8 delivery (Slack/email)
- Any additional enrichment, scoring, or generation
- Any UI

## Repo Layout (Must Follow)
Implement code under:
- `app/sheets/models.py` (row mapping + validation helpers)
- `app/sheets/client.py` (Google Sheets auth/client wrapper)
- `app/sheets/runner.py` (DB load -> sheet upsert)
- `app/sheets/cli.py`
- `app/sheets/__init__.py`

## Required Pipeline Config (Stage 7)
Add:

```yaml
sheets:
  enabled: true
  spreadsheet_id: "<google_sheet_id>"
  worksheet_name: "Ideas"
  key_column: "item_id"
  header_row: 1

stage_7_persist:
  max_rows_default: 200
```

Current local config snapshot (provided):

```yaml
sheets:
  enabled: true
  spreadsheet_id: "1Sih2WSuDoW128gQTDmCyySj5wzgjbfOG7U0QtKT8yow"
  worksheet_name: "Ideas"
  key_column: "item_id"
  header_row: 1

stage_7_persist:
  max_rows_default: 200
```

Validation rules:
- Top-level config must be a mapping.
- Required mappings: `paths`, `sheets`, `stage_7_persist`.
- `paths.sqlite_db`: non-empty string after `strip()`.
- `paths.outputs_dir`: non-empty string after `strip()`.
- `sheets.enabled`: boolean.
- If `sheets.enabled` is `true`:
  - `sheets.spreadsheet_id`: non-empty string after `strip()`.
  - `sheets.worksheet_name`: non-empty string after `strip()`.
  - `sheets.key_column`: non-empty string after `strip()`.
  - `sheets.header_row`: non-boolean integer `>= 1`.
- If `sheets.enabled` is `false`:
  - `sheets.spreadsheet_id`, `sheets.worksheet_name`, `sheets.key_column`, and `sheets.header_row` are optional.
  - `--sheet-id` and `--worksheet` overrides may be provided but do not change runtime behavior while disabled.
- `stage_7_persist.max_rows_default`: non-boolean integer `>= 0`.

If invalid config is detected, fail run with exit code `2`.

## Auth (Google Service Account)
- Use environment variable:
  - `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json`
- Service account email must have edit access to the target spreadsheet.
- Missing credentials or auth/open errors are fatal run errors (exit code `2`).

## Manual Browser Setup (One-Time)
1. Open the target spreadsheet and ensure worksheet `Ideas` exists.
2. Set header row `1`.
3. Minimum required header cells (exact names, any order): `item_id`, `creator`, `post_link`, `topic`, `viral_rating`, `hook`, `platform`, `draft_script`, `status`.
4. Project sheet header row currently configured (recommended superset):
   `item_id, creator, post_link, topic, viral_rating, hook, platform, draft_script, status, monetization_angle, tools_mentioned, published_at, updated_at, notes`
5. In Google Cloud Console, enable Google Sheets API for the service account project.
6. Share the spreadsheet with the service account email as `Editor`.
7. Set `GOOGLE_APPLICATION_CREDENTIALS` to the downloaded service-account JSON key path before running Stage 7.

## Dependencies
Add one approach only:
- `gspread`
- `google-auth`

Do not add additional spreadsheet libraries.

## CLI Contract
Primary command:
- `python -m app.sheets.cli --pipeline config/pipeline.yaml`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--sheet-id <id>` overrides `sheets.spreadsheet_id`
- `--worksheet <name>` overrides `sheets.worksheet_name`
- `--max-rows <int>` overrides `stage_7_persist.max_rows_default`
- `--report <path>` overrides default report path
- `--log-level <LEVEL>` default `INFO`

Override validation:
- `--db`, `--sheet-id`, `--worksheet`, and `--report` must be non-empty strings after `strip()`.
- `--max-rows` must be a non-boolean integer `>= 0`.
- Override validation failures are fatal run errors.
- If override validation fails and `--report` was provided with a valid non-empty path, that value defines `report_path` and Stage 7 must attempt fatal report write to that path.
- If `--report` is provided but fails path validation, `report_path` is unresolved and fatal report write is skipped.

Defaults:
- DB: `paths.sqlite_db`
- report: `{paths.outputs_dir}/stage_7_report_<YYYY-MM-DD>.json` if `--report` omitted
- max-rows: `stage_7_persist.max_rows_default`

Date basis:
- Bind one `run_date_utc` at run start from `started_at` (UTC).
- Default report filename must use `run_date_utc` for the entire run.

Behavior:
- Process selected rows in deterministic order in a single-worker sequential loop.
- If `sheets.enabled` is `false`:
  - skip stage dependency table/schema validation
  - skip all Google Sheets auth and API calls
  - write completed report with `rows_available_total=0`, `rows_considered=0`, `rows_inserted=0`, `rows_updated=0`, `rows_skipped_missing_script=0`, `rows_skipped_invalid_payload=0`, `errors_count=0`, and `first_error=null`
  - emit `spreadsheet_id=""` and `worksheet_name=""` in the report
  - exit `0`
- If `sheets.enabled` is `true`:
  - validate stage dependency tables and schema compatibility
  - authenticate and open spreadsheet/worksheet
  - validate worksheet headers before any upsert
  - apply upsert rules below
- Continue on per-row mapping/data errors only (`rows_skipped_invalid_payload`); do not fail entire run for these row-level data issues.
- Fatal runtime policy (normative):
  - Any SQLite operation failure is fatal (`exit code 2`).
  - Any Google auth/open worksheet/header-contract failure is fatal (`exit code 2`).
  - Any worksheet write API failure (append/update/batch update) is fatal (`exit code 2`).
  - Any failure to write report JSON is fatal (`exit code 2`).
- Exit code:
  - `0` = completed
  - `2` = fatal error

## Stage Dependency Validation (SQLite)
On run start (when `sheets.enabled=true`), Stage 7 must verify compatibility:

### `ideas` table compatibility (required columns)
- `item_id` TEXT PRIMARY KEY
- `url` TEXT NOT NULL
- `title` TEXT NOT NULL
- `topic` TEXT NOT NULL
- `viral_rating` INTEGER NOT NULL
- `hooks` TEXT NOT NULL
- `platform` TEXT NOT NULL
- `monetization_angle` TEXT NOT NULL
- `tools_mentioned` TEXT NOT NULL
- `published_at` TEXT NOT NULL

### `scripts` table compatibility (required columns)
- `item_id` TEXT PRIMARY KEY
- `primary_hook` TEXT NOT NULL
- `script_sections` TEXT NOT NULL

### `items` table compatibility (required columns)
- `item_id` TEXT PRIMARY KEY
- `creator` TEXT NOT NULL
- `source_name` TEXT NOT NULL

Validation mechanics:
- Validate with `PRAGMA table_info(<table>)`.
- Declared types and PK/NOT NULL constraints must match above.
- Extra columns are allowed.
- Any compatibility check failure is fatal (`exit code 2`).

## Selection Query and Counters
Selection query (deterministic):

```sql
SELECT
  i.item_id,
  i.url,
  i.title,
  i.topic,
  i.viral_rating,
  i.hooks,
  i.platform,
  i.monetization_angle,
  i.tools_mentioned,
  i.published_at,
  s.item_id AS script_item_id,
  s.primary_hook,
  s.script_sections,
  it.creator,
  it.source_name
FROM ideas i
LEFT JOIN scripts s ON s.item_id = i.item_id
LEFT JOIN items it ON it.item_id = i.item_id
ORDER BY i.viral_rating DESC, i.published_at DESC, i.item_id ASC
LIMIT :max_rows;
```

Definitions:
- `rows_available_total`: count of rows in `ideas` before `LIMIT`.
- `rows_considered`: count of rows returned by selection query after `LIMIT`.
- `rows_skipped_missing_script`: selected rows where `script_item_id IS NULL`.
- `rows_skipped_invalid_payload`: selected rows with script row present but mapping/parsing fails.
- `rows_inserted`: selected rows inserted as new worksheet rows.
- `rows_updated`: selected rows matched by key and successfully written via update (including no-op value changes).

Counter invariants:
- `rows_considered <= max_rows`
- `rows_inserted + rows_updated + rows_skipped_missing_script + rows_skipped_invalid_payload == rows_considered`
- `errors_count == rows_skipped_invalid_payload`
- If `max_rows == 0`, then `rows_considered == 0`.

## Worksheet Header Contract
Required headers (must exist exactly once):
- `item_id`
- `creator`
- `post_link`
- `topic`
- `viral_rating`
- `hook`
- `platform`
- `draft_script`
- `status`

Recommended headers (optional):
- `monetization_angle`
- `tools_mentioned`
- `published_at`
- `updated_at`
- `notes`

Header rules:
- Read header row from `sheets.header_row`.
- Normalize header cells by `strip()` only.
- Required header names are case-sensitive after trim.
- Duplicate normalized header names are fatal (`exit code 2`).
- Missing required headers are fatal (`exit code 2`).
- Extra headers are allowed.
- `sheets.key_column` must resolve to exactly one header and must be `item_id` for Stage 7.

## Row Mapping Rules
Mapping uses canonicalized strings (`strip()` on scalar strings).

From selected DB row:
- `item_id` <- `ideas.item_id`
- `creator` <- first non-empty of:
  1) `items.creator`
  2) `items.source_name`
  3) literal `"unknown"`
- `post_link` <- `ideas.url`
- `topic` <- `ideas.topic`
- `viral_rating` <- `ideas.viral_rating`
- `platform` <- `ideas.platform`
- `monetization_angle` <- `ideas.monetization_angle`
- `published_at` <- `ideas.published_at`

`hook` resolution:
1. Use `scripts.primary_hook` when non-empty after trim.
2. Else parse `ideas.hooks` JSON array of strings and take first non-empty entry.
3. If neither is available, row is `rows_skipped_invalid_payload`.

`tools_mentioned` resolution:
- Parse `ideas.tools_mentioned` JSON array of strings.
- Trim each entry and drop empty entries.
- Join with `", "`.
- Parse/type errors => `rows_skipped_invalid_payload`.

`draft_script` resolution from `scripts.script_sections`:
- Parse JSON; must be list of exactly 4 objects with labels in order:
  - `hook`
  - `setup`
  - `steps`
  - `cta`
- Each section object must contain exactly `label` and `text`.
- Each `text` must be non-empty after trim.
- Parse/schema errors => `rows_skipped_invalid_payload`.
- Compose multiline text exactly:

```text
Hook: <hook text>

Setup: <setup text>

Steps:
<steps text>

CTA: <cta text>
```

`status` handling:
- New row insert -> set `status = "New"`.
- Existing row update -> preserve current sheet `status` value; never overwrite.

`notes` handling (if column exists):
- New row insert -> empty string.
- Existing row update -> preserve current sheet `notes` value; never overwrite.

`updated_at` handling (if column exists):
- Set to one run-scoped UTC second-precision timestamp for each inserted/updated row.

## Upsert Mechanics
- Read existing worksheet rows below header.
- Build key map from normalized `item_id` cell values:
  - empty keys ignored
  - duplicate non-empty keys are fatal (`exit code 2`)
- For each selected row in order:
  - if missing script row -> increment `rows_skipped_missing_script`
  - else map row fields
  - if mapping fails -> increment `rows_skipped_invalid_payload`
  - else upsert by `item_id`:
    - existing key -> update mapped columns except `status` and `notes`
    - missing key -> append row with `status="New"` and `notes=""` when column exists

## Stage 7 Report (JSON)
Write report to default `{outputs_dir}/stage_7_report_<YYYY-MM-DD>.json` unless overridden.

Required fields:
- `run_id` (UUID4 string)
- `run_status` (`completed` | `fatal`)
- `fatal_error` (string|null)
- `started_at` (UTC second-precision ISO8601 Z)
- `finished_at` (UTC second-precision ISO8601 Z)
- `db_path`
- `report_path`
- `sheets_enabled` (boolean)
- `spreadsheet_id`
- `worksheet_name`
- `max_rows`
- `rows_available_total`
- `rows_considered`
- `rows_inserted`
- `rows_updated`
- `rows_skipped_missing_script`
- `rows_skipped_invalid_payload`
- `errors_count`
- `first_error` (string|null)
Field semantics:
- `errors_count` equals `rows_skipped_invalid_payload`.
- `first_error` is the first row-level mapping/parsing error text; `null` when no row-level errors occurred.
- On fatal runs with no prior row-level error, `first_error` must equal `fatal_error`.
- On completed runs with `sheets_enabled=false`, `spreadsheet_id` and `worksheet_name` must be empty strings and all row/error counters must be `0`.
- On completed runs with `sheets_enabled=true`, `spreadsheet_id` and `worksheet_name` must be non-empty strings.

Run status semantics:
- completed run (`exit code 0`): `run_status = "completed"`, `fatal_error = null`
- fatal run (`exit code 2`) with report write success: `run_status = "fatal"`, `fatal_error` is non-empty summary

Fatal report behavior (normative):
- Stage 7 must attempt report write on fatal run once `report_path` is resolved.
- If fatal occurs before `report_path` is resolved, report write is skipped.
- If `--report` is provided with a valid non-empty path, that value defines `report_path` even when pipeline load fails.
- If `--report` is invalid, `report_path` is unresolved and fatal report write is skipped.
- For fatal reports, all required numeric counters must still be present as non-boolean integers `>= 0`; use `0` when unavailable due to early fatal termination.
- If fatal occurs before DB path resolution, `db_path` may be an empty string.
- If fatal occurs before sheet identifier resolution, `spreadsheet_id` and `worksheet_name` may be empty strings.
- If fatal occurs before `sheets.enabled` resolution, `sheets_enabled` must still be present and must be `false`.
- If fatal occurs before pipeline defaults are available, `max_rows` must still be present:
  - use validated `--max-rows` override when provided
  - otherwise emit `0`

Report counter invariants:
- `rows_considered <= max_rows`
- `rows_inserted + rows_updated + rows_skipped_missing_script + rows_skipped_invalid_payload == rows_considered`
- `errors_count == rows_skipped_invalid_payload`
- On completed run with `sheets_enabled=false`: all row/error counters are `0`.

## Outputs
- JSON report: `data/outputs/stage_7_report_<YYYY-MM-DD>.json`
- Google Sheet rows inserted/updated in configured worksheet

## Tests (pytest) - Minimum
- Mock Google Sheets client calls (no network).
- Validate config and override errors (exit code `2`).
- `sheets.enabled=false` path: no API calls, no table/schema checks, completed report with zero counters and empty `spreadsheet_id`/`worksheet_name`.
- Header contract failures (missing required header, duplicate header, key column mismatch) are fatal.
- New row insert sets `status="New"`.
- Existing row update preserves `status` and `notes`.
- Missing script row increments `rows_skipped_missing_script`.
- Invalid `script_sections` / invalid `tools_mentioned` JSON increments `rows_skipped_invalid_payload`.
- Deterministic ordering: `viral_rating DESC, published_at DESC, item_id ASC`.
- Report fields and invariants are satisfied.
- Fatal before pipeline/default resolution with valid `--report` still writes fatal report with fallback defaults (`max_rows=0`, numeric counters `0`, unresolved strings empty).

## Files Changed (Expected)
- `app/sheets/*` (new)
- `tests/test_stage_7_persist_sheet.py` (new)
- `config/pipeline.yaml` (add `sheets` + `stage_7_persist`)
- `docs/config_schemas.md` (add Stage 7 config contract)

Config-doc/pipeline sync gate (normative):
- Stage 7 code changes are not ready to merge unless `docs/config_schemas.md` includes `sheets.*` and `stage_7_persist.*` schema + validation rules used by this spec.
- Stage 7 code changes are not ready to merge unless `config/pipeline.yaml` defines both `sheets` and `stage_7_persist.max_rows_default`.

## Commands to Run (Expected)
- `python -m app.sheets.cli --pipeline config/pipeline.yaml`
- `pytest -q tests/test_stage_7_persist_sheet.py`

## Produced Artifacts
- `data/outputs/stage_7_report_<YYYY-MM-DD>.json`

