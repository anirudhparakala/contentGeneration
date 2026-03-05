# SPEC - STAGE 9: Ops (End-to-End Run + Final Summary) - Manual Batch

## Depends on
- Stage 0
- Stage 1-8 implemented and importable:
  - `app.ingest.runner.run_ingestion`
  - `app.normalize.runner.run_normalize`
  - `app.filter.runner.run_filter`
  - `app.enrich.runner.run_enrich`
  - `app.intelligence.runner.run_intelligence`
  - `app.generate.runner.run_generate`
  - `app.sheets.runner.run_persist`
  - `app.deliver.runner.run_deliver`

## Objective
Provide one command that runs Stages 1-8 in strict order (manual batch), captures each stage report path, and emits one consolidated final report for the whole run.

Primary happy-path command remains:
- `python -m app.main --run daily`

## In Scope
- Implement Stage 9 orchestration in `app/main.py`.
- Invoke stages through Python runner functions (no subprocess shelling-out to stage CLIs).
- Produce final run artifact:
  - `final_report_<YYYY-MM-DD>.json`
- Optional human-readable artifact:
  - `final_report_<YYYY-MM-DD>.md`

## Out of Scope
- Scheduler/cron
- Hosting/deployment UI
- New per-stage business logic changes

## CLI Contract
Happy path:
- `python -m app.main --run daily`

Explicit path variant:
- `python -m app.main --run daily --pipeline config/pipeline.yaml --sources config/sources.yaml --report data/outputs/final_report_<YYYY-MM-DD>.json`

Optional flags:
- `--stop-after <N>` where `N` is integer in `[1, 8]`
- `--report-md <path>` optional markdown summary output
- `--log-level <LEVEL>` default `INFO`

Defaults:
- `--pipeline` default `config/pipeline.yaml`
- `--sources` default `config/sources.yaml`
- `--report` default `{paths.outputs_dir}/final_report_<YYYY-MM-DD>.json` when pipeline loads successfully
- if pipeline cannot be read and `--report` is omitted, fallback default is `data/outputs/final_report_<YYYY-MM-DD>.json`
- `--report-md` default omitted (no markdown output)

Validation:
- `--run` must be exactly `daily`.
- `--pipeline`, `--sources`, `--report`, `--report-md` (when provided) must be non-empty strings after `strip()`.
- `--stop-after` (when provided) must be a non-boolean integer in `[1, 8]`.
- Validation failures are fatal (`exit code 2`).
- If `--report` is provided but invalid, `final_report_path` is unresolved and fatal report write is skipped.

Exit codes:
- `0`: pipeline `completed` or `stopped` (intentional `--stop-after`)
- `2`: pipeline `failed` (fatal error)

## Stage Invocation Order and Binding (Normative)
Stage 9 must run sequentially in this exact order:
1. Stage 1 ingest:
   - `run_ingestion(sources_path=<resolved_sources>, pipeline_path=<resolved_pipeline>)`
2. Stage 2 normalize:
   - `run_normalize(pipeline_path=<resolved_pipeline>, in_path=<stage_1.raw_items_path>)`
3. Stage 3 filter:
   - `run_filter(pipeline_path=<resolved_pipeline>)`
4. Stage 4 enrich:
   - `run_enrich(pipeline_path=<resolved_pipeline>)`
5. Stage 5 intelligence:
   - `run_intelligence(pipeline_path=<resolved_pipeline>)`
6. Stage 6 generate:
   - `run_generate(pipeline_path=<resolved_pipeline>)`
7. Stage 7 persist:
   - `run_persist(pipeline_path=<resolved_pipeline>)`
8. Stage 8 deliver:
   - `run_deliver(pipeline_path=<resolved_pipeline>)`

Notes:
- Stage 9 must not pass stage-specific override knobs by default.
- Stage 9 must capture each runner return object and use its `report_path` for aggregation.
- Contract mismatch in returned fields required by this spec is fatal.

## Stop-After Semantics (Normative)
If `--stop-after N` is provided:
- Run exactly stages `1..N` and do not start stage `N+1`.
- Set `pipeline_status = "stopped"`.
- Mark stages `N+1..8` as `skipped`.
- `fatal_stage = null`, `fatal_error = null`.
- Exit code `0`.

## Failure Policy (Normative)
Stage fatal detection:
- A stage is fatal if its runner raises its stage fatal exception class.
- A stage is also fatal if runner result has `run_status == "fatal"` (defensive contract guard), even without exception.

On fatal:
- Stop immediately; do not run downstream stages.
- Mark current stage status as `failed`.
- Mark downstream stages as `skipped`.
- Set `pipeline_status = "failed"`.
- Set `fatal_stage` to the failed stage key (`stage_1` ... `stage_8`).
- Set `fatal_error` to non-empty message.
- Attempt final JSON report write once `final_report_path` is resolved.
- Exit code `2`.

Non-fatal per-item errors:
- Stage-level row/item failures from completed stages do not stop orchestration.
- They must be aggregated into `errors_summary.non_fatal_*` fields.

## Final Report Path and Write Rules
- Bind one run-scoped `started_at` timestamp at Stage 9 start (UTC, second precision ISO8601 `Z`).
- Default filename date uses Stage 9 run date from `started_at`.
- Final report JSON write is mandatory when `final_report_path` is resolved.
- Parent directories must be created as needed (`mkdir -p` behavior).
- JSON write failure is fatal (`exit code 2`).
- If `--report-md` is provided, markdown write is required; markdown write failure is fatal (`exit code 2`).

## Final Report JSON Schema (Normative)
```json
{
  "run_id": "uuid4",
  "started_at": "YYYY-MM-DDTHH:MM:SSZ",
  "finished_at": "YYYY-MM-DDTHH:MM:SSZ",
  "pipeline_status": "completed|stopped|failed",
  "fatal_stage": "stage_1|stage_2|stage_3|stage_4|stage_5|stage_6|stage_7|stage_8|null",
  "fatal_error": "string|null",
  "stop_after": "integer|null",
  "pipeline_path": "string",
  "sources_path": "string",
  "db_path": "string",
  "final_report_path": "string",
  "stage_reports": {
    "stage_1": "string|null",
    "stage_2": "string|null",
    "stage_3": "string|null",
    "stage_4": "string|null",
    "stage_5": "string|null",
    "stage_6": "string|null",
    "stage_7": "string|null",
    "stage_8": "string|null"
  },
  "stage_status": {
    "stage_1": "completed|failed|skipped",
    "stage_2": "completed|failed|skipped",
    "stage_3": "completed|failed|skipped",
    "stage_4": "completed|failed|skipped",
    "stage_5": "completed|failed|skipped",
    "stage_6": "completed|failed|skipped",
    "stage_7": "completed|failed|skipped",
    "stage_8": "completed|failed|skipped"
  },
  "stage_errors": {
    "stage_1": "string|null",
    "stage_2": "string|null",
    "stage_3": "string|null",
    "stage_4": "string|null",
    "stage_5": "string|null",
    "stage_6": "string|null",
    "stage_7": "string|null",
    "stage_8": "string|null"
  },
  "key_metrics": {
    "raw_items_emitted": 0,
    "canonical_items_inserted": 0,
    "candidates_inserted": 0,
    "enriched_inserted": 0,
    "ideas_inserted": 0,
    "scripts_inserted": 0,
    "sheet_rows_inserted": 0,
    "sheet_rows_updated": 0,
    "slack_sent": 0
  },
  "errors_summary": {
    "non_fatal_by_stage": {
      "stage_1": 0,
      "stage_2": 0,
      "stage_3": 0,
      "stage_4": 0,
      "stage_5": 0,
      "stage_6": 0,
      "stage_7": 0,
      "stage_8": 0
    },
    "non_fatal_errors_count": 0
  }
}
```

Field rules:
- `stop_after`: emit integer `N` when provided; else `null`.
- `db_path`: resolved from loaded pipeline `paths.sqlite_db`; if unavailable due early fatal, emit empty string.
- `stage_reports.stage_N`: report path from stage return object when stage ran and returned; else `null`.
- On a fatal stage exception before a runner return object is available, that stage's `stage_reports.stage_N` must be `null` (Stage 9 must not guess the path).
- `stage_errors.stage_N`: non-empty fatal message only for failed stage; `null` otherwise.

## Metric Mapping (Normative)
`key_metrics` must be derived from stage return objects using this exact mapping:
- `raw_items_emitted` <- Stage 1 `total_new_items_emitted`
- `canonical_items_inserted` <- Stage 2 `items_inserted_db`
- `candidates_inserted` <- Stage 3 `inserted_db`
- `enriched_inserted` <- Stage 4 `inserted_db`
- `ideas_inserted` <- Stage 5 `inserted_db`
- `scripts_inserted` <- Stage 6 `inserted_db`
- `sheet_rows_inserted` <- Stage 7 `rows_inserted`
- `sheet_rows_updated` <- Stage 7 `rows_updated`
- `slack_sent` <- Stage 8 `items_sent`

If a stage did not run, its mapped metrics contribute `0`.

## Non-Fatal Error Aggregation (Normative)
`errors_summary.non_fatal_by_stage` must use this exact mapping:
- `stage_1` <- Stage 1 `sources_failed`
- `stage_2` <- Stage 2 `items_skipped_invalid`
- `stage_3` <- always `0` (filter rejects are business outcomes, not errors)
- `stage_4` <- Stage 4 `failed_count`
- `stage_5` <- Stage 5 `failed_count`
- `stage_6` <- Stage 6 `failed_count`
- `stage_7` <- Stage 7 `errors_count`
- `stage_8` <- Stage 8 `errors_count`

And:
- `non_fatal_errors_count = sum(non_fatal_by_stage.values())`

## Report Invariants (Normative)
- All stage keys `stage_1..stage_8` must exist in `stage_reports`, `stage_status`, and `stage_errors`.
- All metrics and non-fatal counters must be non-boolean integers `>= 0`.
- If `pipeline_status == "completed"`:
  - all `stage_status` values are `completed`
  - `fatal_stage = null`
  - `fatal_error = null`
- If `pipeline_status == "stopped"`:
  - `stop_after` is non-null in `[1, 8]`
  - `stage_1..stage_N = completed`, `stage_(N+1)..stage_8 = skipped`
  - `fatal_stage = null`
  - `fatal_error = null`
- If `pipeline_status == "failed"`:
  - exactly one stage has `failed`
  - all downstream stages are `skipped`
  - `fatal_stage` matches the failed stage key
  - `fatal_error` is non-empty

## Markdown Summary (Optional Output)
When `--report-md` is provided, Stage 9 must emit a concise markdown summary containing:
- run header (`run_id`, start/finish, `pipeline_status`)
- stage table (`stage`, `status`, `report_path`, `error`)
- key metrics table
- non-fatal error table

## Outputs
- JSON report:
  - default: `{paths.outputs_dir}/final_report_<YYYY-MM-DD>.json` when pipeline is readable
  - fallback when pipeline is unreadable and `--report` omitted: `data/outputs/final_report_<YYYY-MM-DD>.json`
- Optional markdown report:
  - user-provided path via `--report-md`

## Tests (pytest) - Minimum
- Happy path: all stages mocked successful, pipeline status `completed`, exit `0`.
- `--stop-after` behavior: `stopped`, downstream stages `skipped`, exit `0`.
- Fatal stage stop: stage N fatal -> downstream skipped, `failed`, exit `2`.
- Invalid CLI overrides (`--run`, `--stop-after`, invalid path strings) exit `2`.
- Early fatal before pipeline load with valid `--report` still writes fatal final report.
- Invalid `--report` override leaves report path unresolved and skips fatal report write.
- Metric mappings use exact source fields listed above.
- `non_fatal_errors_count` equals exact sum of `non_fatal_by_stage`.
- Stage 2 `in_path` receives Stage 1 `raw_items_path`.
- Final report invariants enforced.

## Files Changed (Expected)
- `app/main.py`
- `tests/test_stage_9_ops.py`
- `docs/spec-stage_09_ops.md`

## Commands to Run (Expected)
- `python -m app.main --run daily`
- `pytest -q tests/test_stage_9_ops.py`

## Produced Artifacts
- `data/outputs/final_report_<YYYY-MM-DD>.json`
- optional markdown summary path provided via `--report-md`
