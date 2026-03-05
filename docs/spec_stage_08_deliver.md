# SPEC - STAGE 8: Deliver (Slack Digest) - Manual Batch

## Depends on
- Stage 0
- Stage 2 (`items` table exists and is schema-compatible; table may be empty)
- Stage 5 (`ideas` table exists and is schema-compatible; table may be empty)
- Stage 6 (`scripts` table exists and is schema-compatible; table may be empty)

Stage 7 is optional for Stage 8.

## Objective
Send a digest of the top N ready-to-post scripts to Slack.

"Ready" means:
- idea row exists (`ideas`)
- script row exists (`scripts`)

Do not send duplicates across runs for the same Slack webhook destination.

## In Scope
- Read defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - `deliver.*`
- Read from SQLite `ideas`, `scripts`, and `items`.
- Select deterministic top candidates by:
  - `viral_rating DESC`, then `published_at DESC`, then `item_id ASC`
- Post one Slack message per selected item.
- Track idempotent delivery in SQLite `deliveries`.
- Emit Stage 8 report JSON.

## Out of Scope
- Scheduling (manual run only)
- Email delivery
- Updating Google Sheet status values
- Concurrent multi-worker delivery orchestration

## Repo Layout (Must Follow)
Implement code under:
- `app/deliver/models.py`
- `app/deliver/slack.py`      (webhook sender)
- `app/deliver/state.py`      (SQLite helpers; `deliveries` table)
- `app/deliver/runner.py`
- `app/deliver/cli.py`
- `app/deliver/__init__.py`

## Required Pipeline Config (Stage 8)
Add:

```yaml
deliver:
  enabled: true
  channel: "slack"
  slack_webhook_url: "https://hooks.slack.com/services/T00000000/B00000000/REPLACE_ME"
  max_items_per_run: 10
  max_script_chars: 1200
  min_viral_rating: 5
  include_only_status: []
  dry_run: false
```

Secret handling policy:
- This project uses YAML file values (not env vars) for Stage 8 webhook config.
- Keep the real webhook only in `config/pipeline.local.yaml` (gitignored).
- Keep placeholders/non-secret defaults in `config/pipeline.yaml`.
- Pipeline loading is single-file by default:
  - Stage 8 reads exactly the file passed via `--pipeline`.
  - Stage 8 does not auto-merge `pipeline.yaml` + `pipeline.local.yaml` unless that merge is explicitly implemented in Stage 8 code.

Validation rules:
- Top-level config must be a mapping.
- Required mappings: `paths`, `deliver`.
- `paths.sqlite_db`: non-empty string after `strip()`.
- `paths.outputs_dir`: non-empty string after `strip()`.
- `deliver.enabled`: boolean.
- `deliver.channel`: non-empty string after `strip()`, must be `"slack"`.
- If `deliver.enabled=true` and `deliver.channel="slack"`:
  - `deliver.slack_webhook_url`: non-empty string after `strip()`, and must start with either:
    - `https://hooks.slack.com/services/`
    - `https://hooks.slack-gov.com/services/`
- `deliver.max_items_per_run`: non-boolean integer `>= 0`.
- `deliver.max_script_chars`: non-boolean integer `>= 80`.
- `deliver.min_viral_rating`: nullable; when provided, must be non-boolean integer in `[1, 10]`.
- `deliver.include_only_status`: list of strings; for Stage 8 it must be an empty list (`[]`).
  - Non-empty value is a fatal config error (reserved for future sheet-status integration).
- `deliver.dry_run`: boolean.

If invalid config is detected, fail run with exit code `2`.

## Manual Browser Setup (Slack Incoming Webhook, One-Time)
1. Open `https://api.slack.com/apps`.
2. Click `Create New App`.
3. Select `From scratch`.
4. Enter app name (for example `content-delivery-bot`) and choose your workspace.
5. Open left sidebar -> `Incoming Webhooks`.
6. Turn on `Activate Incoming Webhooks`.
7. Click `Add New Webhook to Workspace`.
8. Select the target channel and click `Allow`.
9. Copy the generated webhook URL shown under `Webhook URLs for Your Workspace`.
10. If using a private channel, invite the app in Slack (`/invite @<app-name>`).
11. Paste the webhook URL into `deliver.slack_webhook_url` in `config/pipeline.local.yaml`.

## Dependencies
Use `requests` only (already installed). Do not add Slack SDK.

## CLI Contract
Primary command:
- `python -m app.deliver.cli --pipeline config/pipeline.yaml`

Pipeline file requirement:
- The file passed to `--pipeline` must already be fully resolved for all required keys (`paths.*`, `deliver.*`).

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--max-items <int>` overrides `deliver.max_items_per_run`
- `--dry-run` forces `dry_run=true`
- `--report <path>` overrides default report path
- `--log-level <LEVEL>` default `INFO`

Override validation:
- `--db` and `--report` must be non-empty strings after `strip()`.
- `--max-items` must be a non-boolean integer `>= 0`.
- Override validation failures are fatal run errors.
- If override validation fails and `--report` was provided with a valid non-empty path, that value defines `report_path` and Stage 8 must attempt fatal report write to that path.
- If `--report` is provided but fails path validation, `report_path` is unresolved and fatal report write is skipped.

Defaults:
- DB: `paths.sqlite_db`
- report: `{paths.outputs_dir}/stage_8_report_<YYYY-MM-DD>.json` if `--report` omitted
- max-items: `deliver.max_items_per_run`

Date basis:
- Bind one `run_date_utc` at run start from `started_at` (UTC).
- Default report filename must use `run_date_utc` for the entire run.

Exit code:
- `0` = completed
- `2` = fatal error

## Runtime Behavior (Normative)
Processing model:
- Single-process, single-worker, sequential loop.
- Stage 8 does not support concurrent Stage 8 writers against the same DB file.

If `deliver.enabled=false`:
- Skip dependency table/schema checks.
- Skip all Slack network calls.
- Skip all `deliveries` table writes.
- Write completed report with all row/error counters set to `0`.
- Exit `0`.

If `deliver.enabled=true`:
- Validate dependency tables and schema compatibility.
- Ensure `deliveries` table exists and validate compatibility.
- Compute `webhook_hash` as SHA-256 hex digest of `deliver.slack_webhook_url`.
- Select ordered candidates from SQLite.
- Apply per-row eligibility/idempotency rules below.
- Build message text and send one Slack message per selected item.
- Persist one `deliveries` row per successful send.

If `dry_run=true`:
- Perform full config/DB/query/mapping/idempotency logic.
- Do not call Slack webhook.
- Do not write `deliveries` rows.
- `items_sent` must be `0`.

Fatal runtime policy (normative):
- Any config load/validation failure is fatal (`exit code 2`).
- Any SQLite open/query/compatibility failure is fatal (`exit code 2`).
- Any `deliveries` table ensure/compatibility failure is fatal (`exit code 2`).
- Any failure to write report JSON is fatal (`exit code 2`).

Non-fatal per-item policy:
- Invalid per-row payload/mapping errors are non-fatal; count in `errors_count` and continue.
- Slack send failures (network/non-2xx) are non-fatal; count in `errors_count` and continue.

## Stage Dependency Validation (SQLite)
On run start (when `deliver.enabled=true`), Stage 8 must verify compatibility:

### `ideas` table compatibility (required columns)
- `item_id` TEXT PRIMARY KEY
- `url` TEXT NOT NULL
- `topic` TEXT NOT NULL
- `viral_rating` INTEGER NOT NULL
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

## SQLite Persistence (`deliveries`)
Create table `deliveries` if not exists:

- `item_id` TEXT NOT NULL
- `channel` TEXT NOT NULL                # `"slack"`
- `webhook_hash` TEXT NOT NULL           # sha256 hex of webhook URL
- `sent_at` TEXT NOT NULL                # UTC second-precision ISO8601 Z
- PRIMARY KEY (`item_id`, `channel`, `webhook_hash`)

Compatibility check (normative):
- Validate with `PRAGMA table_info(deliveries)`.
- Required columns must exist with exact declared type/PK/NOT NULL constraints defined above.
- Extra columns are allowed.
- Compatibility failure is fatal (`exit code 2`).

## Selection Rules and Idempotency
Candidate query (deterministic):

```sql
SELECT
  i.item_id,
  i.url,
  i.topic,
  i.viral_rating,
  i.published_at,
  s.item_id AS script_item_id,
  s.primary_hook,
  s.script_sections,
  it.creator,
  it.source_name
FROM ideas i
LEFT JOIN scripts s ON s.item_id = i.item_id
LEFT JOIN items it ON it.item_id = i.item_id
WHERE (:min_viral_rating IS NULL OR i.viral_rating >= :min_viral_rating)
ORDER BY i.viral_rating DESC, i.published_at DESC, i.item_id ASC;
```

Counter definitions:
- `items_available_total`: count of `ideas` rows matching `min_viral_rating` filter before row-level skips and before max-items cap.
- `items_skipped_missing_script`: candidate rows where `script_item_id IS NULL`.
- `items_skipped_already_sent`: candidate rows skipped because `deliveries` already has (`item_id`, `channel='slack'`, `webhook_hash=current_webhook_hash`).
- `items_selected`: eligible rows accepted for delivery attempt in this run, capped by `max_items`.

Selection algorithm:
- Iterate rows in deterministic query order.
- Stop once `items_selected == max_items`.
- For each row in order:
  - if script missing: increment `items_skipped_missing_script`, continue.
  - else if already sent for same (`item_id`, `channel`, `webhook_hash`): increment `items_skipped_already_sent`, continue.
  - else: mark selected and process.

Idempotency scope:
- Idempotency is per Slack destination environment.
- "Already sent" means matching all three keys:
  - `item_id`
  - `channel='slack'`
  - `webhook_hash` of current webhook URL

## Row Mapping and Message Composition
`creator` resolution:
1. Use `items.creator` when non-empty after trim.
2. Else use `items.source_name` when non-empty after trim.
3. Else use literal `"unknown"`.

`hook` field:
- Use `scripts.primary_hook` (trimmed, must be non-empty).

`script` field construction from `scripts.script_sections`:
- Parse JSON.
- Must be a list of exactly 4 objects in exact order with labels:
  - `hook`, `setup`, `steps`, `cta`
- Each object must contain keys `label` and `text`.
- Each `text` must be non-empty after trim.
- Compose exact multiline script text:

```text
Hook: <hook text>

Setup: <setup text>

Steps:
<steps text>

CTA: <cta text>
```

If parsing/validation fails, row is non-fatal error:
- increment `errors_count`
- set `first_error` if null
- do not send Slack message for that row
- do not write `deliveries` row for that row

Truncation rule:
- If composed script text length exceeds `max_script_chars`, set:
  - `script_text = script_text[:max_script_chars] + "...(truncated)"`

`timestamp` field for message template:
- Use one run-scoped UTC second-precision ISO8601 Z timestamp equal to report `started_at`.
- Reuse the same value for every message sent in that run.

Message format (plain text):

```text
{viral_rating}/10 - {topic}
Creator: {creator}
Hook: {hook}
Link: {url}
Script:
{script_text}
Generated at {timestamp}. Item: {item_id}
```

## Slack Transport Contract
- Send one POST per selected row.
- Endpoint: `deliver.slack_webhook_url`
- JSON body:

```json
{"text": "<message text>"}
```

- Request timeout: 20 seconds.
- Success: any HTTP `2xx`.
- Failure: network exception or non-2xx response.

On successful send (`dry_run=false`):
- Insert one `deliveries` row with:
  - `item_id`
  - `channel='slack'`
  - `webhook_hash`
  - `sent_at` (UTC second-precision ISO8601 Z)

Persistence rule:
- Use plain `INSERT` (not `INSERT OR IGNORE`) for `deliveries` writes.
- Unexpected unique-conflict on insert is a fatal state error (`exit code 2`) because concurrent Stage 8 writers are out of scope.

## Stage 8 Report (JSON)
Write report to default `{outputs_dir}/stage_8_report_<YYYY-MM-DD>.json` unless overridden.

Required fields:
- `run_id` (UUID4 string)
- `run_status` (`completed` | `fatal`)
- `fatal_error` (string|null)
- `started_at` (UTC second-precision ISO8601 Z)
- `finished_at` (UTC second-precision ISO8601 Z)
- `db_path`
- `report_path`
- `enabled` (boolean)
- `dry_run` (boolean)
- `channel` (string)
- `max_items` (integer >= 0)
- `min_viral_rating` (integer|null)
- `items_available_total` (integer >= 0)
- `items_selected` (integer >= 0)
- `items_sent` (integer >= 0)
- `items_skipped_already_sent` (integer >= 0)
- `items_skipped_missing_script` (integer >= 0)
- `errors_count` (integer >= 0)
- `first_error` (string|null)

Field semantics:
- `items_sent` counts successfully sent Slack messages in this run.
- If `dry_run=true`, `items_sent` must be `0`.
- `errors_count` counts non-fatal per-row errors (invalid payload + Slack send failures).
- `first_error` is first non-fatal per-row error text; `null` when no non-fatal row errors.
- On fatal runs with no prior non-fatal row error, `first_error` must equal `fatal_error`.

Run status semantics:
- completed run (`exit code 0`): `run_status = "completed"`, `fatal_error = null`
- fatal run (`exit code 2`) with report write success: `run_status = "fatal"`, `fatal_error` is non-empty

Fatal report behavior (normative):
- Stage 8 must attempt report write on fatal run once `report_path` is resolved.
- If fatal occurs before `report_path` is resolved, report write is skipped.
- If `--report` is provided with a valid non-empty path, that value defines `report_path` even when pipeline load fails.
- If `--report` is invalid, `report_path` is unresolved and fatal report write is skipped.
- For fatal reports, all required numeric counters must still be present as non-boolean integers `>= 0`; use `0` when unavailable due to early fatal termination.
- If fatal occurs before DB path resolution, `db_path` may be an empty string.
- If fatal occurs before config resolution, the following fields must still be emitted:
  - `enabled=false`
  - `channel="slack"`
  - `min_viral_rating=null`
  - `dry_run=true` when `--dry-run` was provided; otherwise `dry_run=false`
  - `max_items` uses validated `--max-items` override when provided; otherwise `0`

Report invariants:
- `items_selected <= max_items`
- `items_sent <= items_selected`
- If `dry_run=true`, then `items_sent == 0`
- On completed run with `enabled=false`: all item/error counters are `0`

## Outputs
- JSON report: `data/outputs/stage_8_report_<YYYY-MM-DD>.json`
- SQLite table `deliveries` created/updated in configured DB

## Tests (pytest) - Minimum
- Mock `requests.post` (no network).
- Validate config and override errors (exit code `2`).
- `deliver.enabled=false` path: no DB dependency checks, no webhook calls, completed report with zero counters.
- `dry_run=true`: selection/mapping executes, no webhook calls, no `deliveries` inserts, `items_sent=0`.
- Only rows with scripts are selectable; missing-script rows increment `items_skipped_missing_script`.
- Idempotency skips already-sent rows for same webhook hash.
- Same `item_id` can be delivered to a different webhook hash (different environment).
- Truncation appends `...(truncated)` when script exceeds `max_script_chars`.
- Invalid `script_sections` mapping increments `errors_count` and sets `first_error`.
- Slack non-2xx/network failure increments `errors_count` and continues.
- Deterministic ordering: `viral_rating DESC, published_at DESC, item_id ASC`.
- Report fields and invariants are satisfied.
- Fatal before pipeline/default resolution with valid `--report` still writes fatal report; fallback fields are emitted with override precedence (`--max-items`, `--dry-run`) as defined above.

## Files Changed (Expected)
- `app/deliver/*` (new)
- `tests/test_stage_8_deliver.py` (new)
- `config/pipeline.yaml` (add `deliver` section with placeholder)
- `docs/config_schemas.md` (add Stage 8 config contract)

Config-doc/pipeline sync gate (normative):
- Stage 8 code changes are not ready to merge unless `docs/config_schemas.md` includes `deliver.*` schema + validation rules used by this spec.
- Stage 8 code changes are not ready to merge unless `config/pipeline.yaml` defines `deliver` with Stage 8 required keys.

## Commands to Run (Expected)
- `python -m app.deliver.cli --pipeline config/pipeline.yaml`
- `pytest -q tests/test_stage_8_deliver.py`

## Produced Artifacts
- `data/outputs/stage_8_report_<YYYY-MM-DD>.json`
