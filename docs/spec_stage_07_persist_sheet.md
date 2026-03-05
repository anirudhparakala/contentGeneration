# SPEC — STAGE 7: Persist (Google Sheets “Ideas CRM”) — Manual Batch

## Depends on
- Stage 0
- Stage 5 (ideas table)
- Stage 6 (scripts table)

## Objective
Write the latest ideas + script drafts into a Google Sheet with the required columns:
Creator, Post link, Topic, Viral Rating, Hook, Platform, Draft script, Status
Also include a few extra traceability columns (optional but recommended).

Stage 7 is a persistence/export stage. No LLMs.

## In Scope
- Read from SQLite:
  - `ideas` table (topic, rating, hooks, etc.)
  - `scripts` table (primary_hook, script_sections, etc.)
- Join by `item_id`.
- Upsert rows into a Google Sheet tab (worksheet).
- Add/update:
  - Status column (default "New" for newly inserted rows)
  - Last updated timestamp
- Emit Stage 7 report JSON.

## Out of Scope
- Slack/email delivery (Stage 8)
- Any additional enrichment or scoring
- Any UI

## Repo Layout (Must Follow)
Implement code under:
- `app/sheets/models.py`   (Row mapping helpers)
- `app/sheets/client.py`   (Google Sheets client auth)
- `app/sheets/runner.py`   (load DB rows -> upsert sheet)
- `app/sheets/cli.py`
- `app/sheets/__init__.py`

## Config Additions (config/pipeline.yaml)
Add section:

sheets:
  enabled: true
  spreadsheet_id: "<google_sheet_id>"
  worksheet_name: "Ideas"
  key_column: "item_id"
  header_row: 1

Auth:
- Use a Google service account JSON file path from env var:
  - `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json`
- The service account email must have Edit access to the Sheet.

## Dependencies
Add one dependency (pick one approach):
Option A (recommended): `gspread` + `google-auth`
- gspread
- google-auth

(Do not add more spreadsheet libs.)

## CLI Contract
Primary command:
- `python -m app.sheets.cli --pipeline config/pipeline.yaml --report data/outputs/stage_7_report_<YYYY-MM-DD>.json`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--sheet-id <id>` overrides `sheets.spreadsheet_id`
- `--worksheet <name>` overrides `sheets.worksheet_name`
- `--max-rows <int>` default 200
- `--log-level <LEVEL>`

Behavior:
- Upsert rows using `item_id` as the unique key.
- If a row exists, update all fields EXCEPT:
  - Status (do not overwrite if user changed it)
- If a row does not exist, insert it with Status="New".
- Exit code:
  - 0 = completed
  - 2 = fatal error (auth failure, cannot open sheet, cannot read DB)

## Sheet Columns (Required + Recommended)
Required columns (must exist in worksheet header):
- item_id
- creator
- post_link
- topic
- viral_rating
- hook
- platform
- draft_script
- status

Recommended additional columns:
- monetization_angle
- tools_mentioned
- published_at
- updated_at
- notes

## Row Mapping Rules
From `ideas`:
- creator: derive from `items.creator` if available; else `ideas.source_type` + `ideas.title` (fallback allowed)
- topic: ideas.topic
- viral_rating: ideas.viral_rating
- platform: ideas.platform
- hook: prefer scripts.primary_hook if present else ideas.hooks[0]
- monetization_angle: ideas.monetization_angle
- tools_mentioned: join list into comma-separated string
- published_at: ideas.published_at

From `scripts`:
- draft_script:
  - concatenate script sections into a readable block:
    - Hook: ...
    - Setup: ...
    - Steps: ...
    - CTA: ...
- status:
  - if new row: "New"
  - if existing row: keep current value

Key column:
- item_id

## Upsert Mechanics
- Read the sheet header row and build a column index map.
- Read existing rows’ item_id column into a map:
  - item_id -> row_number
- For each DB row (up to max_rows):
  - if item_id exists: update cells except status
  - else: append a new row with status="New"

## Stage 7 Report (JSON)
Must include:
- run_id, started_at, finished_at
- db_path
- spreadsheet_id, worksheet_name
- rows_considered
- rows_inserted
- rows_updated
- rows_skipped_missing_script (ideas with no script row)
- errors_count
- first_error (optional)

## Tests (pytest) — Minimum
- Mock Google Sheets client calls (no network).
- Test:
  - inserts new row with status="New"
  - updates existing row but preserves status

## Produced Artifacts
- data/outputs/stage_7_report_<YYYY-MM-DD>.json