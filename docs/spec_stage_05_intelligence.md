# SPEC - STAGE 5: Intelligence (Extract + Score) - Manual Batch

## Depends on
- Stage 0
- Stage 4 (SQLite table `enriched_items` exists; table may be empty)

## Objective
Convert enriched content into structured "idea intelligence" using an LLM:
- Extract: topic, core claim, workflow steps, tools, monetization angle, metrics/claims, assumptions
- Score: viral/impact rating (1-10) and rationale
- Produce exactly 3 hook candidates (short)
Persist results to SQLite `ideas` table and emit JSONL for this run.

Stage 5 does NOT generate full scripts. That is Stage 6.

## In Scope
- Read defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - `llm.*` (defined below)
  - `stage_5_intelligence.*` (defined below)
- Read inputs from SQLite `enriched_items` table (success rows only).
- Select items not already present in `ideas` (idempotent).
- Run LLM extraction and scoring using prompt templates from `config/prompts/`:
  - `config/prompts/stage_5_extract.md`
  - `config/prompts/stage_5_score.md`
- Persist to SQLite `ideas` table.
- Emit:
  - JSONL of ideas inserted this run only
  - Stage 5 report JSON

## Out of Scope
- Transcript/page fetching (Stage 4)
- Full script drafting (Stage 6)
- Slack/email delivery (Stage 8)
- Google Sheets writes (Stage 7)

## Repo Layout (Must Follow)
Implement code under:
- `app/intelligence/models.py`     (Idea model + parsing/validation)
- `app/intelligence/prompts.py`    (load prompt files)
- `app/intelligence/llm.py`        (OpenAI client wrapper, retries, rate limit)
- `app/intelligence/state.py`      (SQLite helpers for ideas table)
- `app/intelligence/runner.py`
- `app/intelligence/cli.py`
- `app/intelligence/__init__.py`

## Required Pipeline Config (Stage 5)
Add:

```yaml
llm:
  provider: "openai"
  model: "gpt-4o-mini"
  temperature: 0.2
  max_output_tokens: 900
  requests_per_minute_soft: 30
  request_timeout_s: 60
  retry_max_attempts: 3
  retry_backoff_initial_s: 1.0
  retry_backoff_multiplier: 2.0
  retry_backoff_max_s: 8.0
  api_key_env_var: "OPENAI_API_KEY"
  api_key: ""

stage_5_intelligence:
  max_items_default: 25
  input_max_chars: 12000
```

Validation rules:
- top-level config must be a mapping.
- required mappings: `paths`, `llm`, `stage_5_intelligence`.
- `paths.sqlite_db`: non-empty string after `strip()`.
- `paths.outputs_dir`: non-empty string after `strip()`.
- `llm.provider`: non-empty string; currently only `openai` allowed.
- `llm.model`: non-empty string.
- `llm.temperature`: non-boolean number `>= 0` and `<= 2`.
- `llm.max_output_tokens`: non-boolean integer `>= 1`.
- `llm.requests_per_minute_soft`: non-boolean integer `>= 1`.
- `llm.request_timeout_s`: non-boolean integer `>= 1`.
- `llm.retry_max_attempts`: non-boolean integer `>= 1`.
- `llm.retry_backoff_initial_s`: non-boolean number `> 0`.
- `llm.retry_backoff_multiplier`: non-boolean number `>= 1`.
- `llm.retry_backoff_max_s`: non-boolean number `>= llm.retry_backoff_initial_s`.
- `llm.api_key_env_var`: non-empty string.
- `llm.api_key`: string (may be empty after `strip()`).
- `stage_5_intelligence.max_items_default`: non-boolean integer `>= 0`.
- `stage_5_intelligence.input_max_chars`: non-boolean integer `>= 1`.

If invalid config is detected, fail run with exit code `2`.

## Secrets (Local Workflow)
Use a local pipeline file for the API key.

Create `config/pipeline.local.yaml` locally as a full copy of `config/pipeline.yaml` (do not commit secrets). Keep `llm.api_key` there.

Pipeline file loading rule (normative):
- Stage 5 CLI accepts exactly one pipeline path via `--pipeline`.
- Stage 5 does not perform implicit multi-file merge/overlay (for example base `pipeline.yaml` + `pipeline.local.yaml`).

API key resolution order (normative):
1) Read environment variable named by `llm.api_key_env_var`; if value is non-empty after `strip()`, use it.
2) Else if `llm.api_key` exists and is non-empty after `strip()`, use it.
3) Else fatal run error (exit code `2`).

API key logging rule (normative):
- Never log secret values.
- Logging may include only key source metadata (`env` or `config`) without the secret contents.

Security rule:
- Never commit a real API key to tracked files.

## CLI Contract
Primary local command:
- `python -m app.intelligence.cli --pipeline config/pipeline.local.yaml`

Shared/CI command (when key is supplied via environment variable):
- `python -m app.intelligence.cli --pipeline config/pipeline.yaml`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--out <path>` overrides default output path
- `--report <path>` overrides default report path
- `--max-items <int>` overrides `stage_5_intelligence.max_items_default`
- `--model <name>` overrides `llm.model`
- `--log-level <LEVEL>` default `INFO`

Override validation:
- `--max-items` must be a non-boolean integer `>= 0`; otherwise fatal error (exit code `2`).
- `--model` must be a non-empty string after `strip()`; otherwise fatal error (exit code `2`).
- Override validation failures are fatal run errors.
- If override validation fails and `--report` was provided, `--report` defines `report_path` and Stage 5 must attempt fatal report write to that path.

Defaults:
- DB: `paths.sqlite_db`
- out: `{paths.outputs_dir}/ideas_<YYYY-MM-DD>.jsonl` if `--out` omitted
- report: `{paths.outputs_dir}/stage_5_report_<YYYY-MM-DD>.json` if `--report` omitted
- max-items: `stage_5_intelligence.max_items_default`

Date basis:
- Bind one `run_date_utc` at run start from `started_at` (UTC).
- Default output/report filenames must use `run_date_utc` for the entire run.

Behavior:
- Overwrite output JSONL each run.
- Always create/truncate output JSONL once `output_path` is resolved and before item processing begins; when fatal termination occurs before `output_path` is resolved, output file creation is skipped.
- Create parent directories for `output_path` and `report_path` when missing (`mkdir -p` semantics).
- Process selected rows in deterministic order in a single-worker sequential loop (no parallel item processing and no parallel LLM request execution).
- Continue on per-item failures; do not fail entire run for item-level failures.
- Fatal runtime policy (normative):
  - Any SQLite operation failure that is not an expected `INSERT OR IGNORE` conflict is a fatal run error (exit code `2`).
  - Any failure to write/truncate output JSONL or write report JSON is a fatal run error (exit code `2`).
  - Any failure to read/load prompt templates (`config/prompts/stage_5_extract.md`, `config/prompts/stage_5_score.md`) or render required prompt inputs is a fatal run error (exit code `2`).
  - Provider auth/resource fatal classes are fatal run errors (exit code `2`): HTTP `401`, HTTP `403`, and HTTP `404` from LLM calls (applies to both extract and score).
  - On fatal run error, stop processing immediately.
  - Fatal SQLite/artifact I/O errors are not per-item failures and must never be remapped into `fail_breakdown` reasons.
- Fatal artifact policy (normative):
  - Output JSONL is best-effort and may be partially written with rows emitted before a fatal run error.
  - No rollback/truncation of already-written JSONL rows is required after a fatal run error.
  - Stage 5 must attempt to write report JSON on any fatal run error once `report_path` is resolved.
  - Fatal classes covered by this rule include override validation failures, pipeline read/parse/validation failures, prompt load/render failures, missing API key, provider auth/resource fatal classes (HTTP `401`/`403`/`404`), DB open failures, and SQLite/artifact I/O failures.
  - Pre-config fatal clarification:
    - If fatal occurs before `report_path` is resolved (for example pipeline read/parse failure with no `--report` override), report write is skipped and run exits `2`.
    - If `--report` is provided, that value defines `report_path` even when pipeline load fails; Stage 5 must attempt fatal report write to that path.
  - If fatal report write succeeds, report fields `run_status` and `fatal_error` must indicate fatal termination.
  - If fatal report write fails, run exits `2` and report JSON may be absent.
- Exit code:
  - `0` = completed (even if some items failed)
  - `2` = fatal error (invalid config/overrides, prompt load/render failure, missing API key, provider auth/resource fatal class, cannot open DB, cannot write outputs)

## Input Contract (SQLite `enriched_items` table)
Stage 5 reads:
- `enriched_items.item_id`
- `enriched_items.source_type`
- `enriched_items.url`
- `enriched_items.title`
- `enriched_items.published_at`
- `enriched_items.enriched_text`
- `enriched_items.enrichment_method`
- `enriched_items.evidence_snippets` (JSON string)

Stage dependency validation (normative):
- On run start, Stage 5 must verify that table `enriched_items` exists and can be queried with required columns:
  - `item_id`, `source_type`, `url`, `title`, `published_at`, `enriched_text`, `enrichment_method`, `evidence_snippets`
- On run start, Stage 5 must execute `CREATE TABLE IF NOT EXISTS ideas (...)` using the schema defined in this spec.
- After table ensure, `ideas` must pass the explicit compatibility checks in the `Ideas table compatibility check` section below.
- Stage 5 must not auto-migrate incompatible existing `ideas` schemas.
- Non-empty `enriched_items` input is not required; zero available rows is a valid completed run (`exit code 0`) with zero item counters.
- If `enriched_items` is missing/unreadable/invalid, or if `ideas` ensure/check fails, fail the run with exit code `2`.

Selection query:

```sql
SELECT
  e.item_id,
  e.source_type,
  e.url,
  e.title,
  e.published_at,
  e.enriched_text,
  e.enrichment_method,
  e.evidence_snippets
FROM enriched_items e
LEFT JOIN ideas i ON i.item_id = e.item_id
WHERE i.item_id IS NULL
ORDER BY e.published_at DESC, e.item_id ASC
LIMIT :max_items;
```

Definitions:
- `items_available_total`: count of rows matching `WHERE i.item_id IS NULL` before `LIMIT`.
- `selected_rows_total`: count of rows returned by the selection query after `LIMIT`.
- `items_selected`: selected rows that reached a terminal outcome (`success` or exactly one mapped failure reason) before run end.
- On completed runs, `items_selected == selected_rows_total`.
- On fatal runs, `items_selected` may be lower than `selected_rows_total` because processing stops immediately.
- On fatal runs, selected rows not yet processed at stop time are non-terminal and must not be counted in `success_count`, `failed_count`, or `fail_breakdown`.
- If `max_items == 0`, both `selected_rows_total` and `items_selected` must be `0`.

### Per-row pre-LLM validation gate (normative)
For each selected row, validate and canonicalize before any LLM call:
- `item_id`, `source_type`, `url`, `title`, `published_at`, `enriched_text` must be strings and non-empty after `strip()`.
- Canonicalization for all fields above is `value = value.strip()`.
- `published_at` must match UTC second-precision ISO8601 `YYYY-MM-DDTHH:MM:SSZ`.
- Canonicalize source type with `source_type = source_type.lower()`.
- Supported canonical `source_type` values are exactly `newsletter` and `youtube`.
- If any check above fails, map item to `extract_validation_failed` and skip all LLM calls for that item.
- `evidence_snippets` is read as-is from DB and handled by the evidence preprocessing contract; malformed content there must not fail pre-LLM validation.

## Prompting Contract
Stage 5 runs up to two LLM calls per selected item:
- Pre-LLM validation gate runs first (defined above).
- Call 1 (extract) runs only for items that pass the pre-LLM validation gate.
- Call 2 (score) runs only if Call 1 succeeded and passed schema validation.

Prompt source-of-truth rule (normative):
- This spec is authoritative for Stage 5 runtime behavior and validation.
- Prompt templates must be aligned to this spec; implementation must not relax runtime validation rules to match stale prompt text.
- Pre-implementation note: prompt templates in repository HEAD may be stale; stale prompt text does not change this runtime contract and must be updated to match this spec in the same implementation change set.

Terminal call-flow rule (normative):
- First failing step wins.
- Once a selected item hits one mapped failure reason, no further processing/calls for that item are allowed.

Rate-limit rule (normative):
- Both call types (extract and score) count toward `llm.requests_per_minute_soft`.
- Every provider request attempt counts toward this cap, including retries.
- Apply deterministic throttle before each attempt: ensure at least `60.0 / llm.requests_per_minute_soft` seconds between consecutive attempt start times.

Attempt scheduling rule (normative):
- Let `rpm_spacing_s = 60.0 / llm.requests_per_minute_soft`.
- Let `last_global_attempt_start` be the most recent attempt start timestamp across all items/call types/retries.
- For any attempt, earliest start by RPM rule is `last_global_attempt_start + rpm_spacing_s` (or immediate if no prior attempt exists).
- For retry attempt `n >= 2` of the same call, earliest start by backoff rule is `previous_attempt_end + backoff_delay(n)`.
- Actual attempt start must satisfy both constraints:
  - `attempt_start >= earliest_rpm_start`
  - `attempt_start >= earliest_backoff_start` (retry attempts only)
- Equivalent deterministic implementation: `attempt_start = max(now, earliest_rpm_start, earliest_backoff_start_if_retry)`.

Structured output rule (normative):
- For provider `openai`, both calls must request strict JSON-schema output (schema-constrained response format).
- Do not request free-form text output for these calls.
- Even with schema-constrained output, runner must still parse and validate locally.

LLM request parameter binding rule (normative):
- For both call types (extract and score), every provider attempt must use:
  - provider: `llm.provider`
  - model: resolved model (`--model` override when provided; otherwise `llm.model`)
  - temperature: `llm.temperature`
  - max output tokens: `llm.max_output_tokens`
  - timeout: `llm.request_timeout_s`
- `llm.temperature` and `llm.max_output_tokens` are required runtime request parameters for both calls, not docs-only config.

Retry rule (normative):
- Retry policy is config-driven by `llm.retry_*` and `llm.request_timeout_s`.
- A request attempt includes provider call plus timeout enforcement.
- Retryable classes:
  - transport/network timeouts and connection failures
  - provider rate-limit responses (for example HTTP 429)
  - provider transient server errors (HTTP 5xx)
- Non-retryable classes:
  - provider auth/permission failures (HTTP `401`/`403`) and model/resource missing (HTTP `404`) -> fatal run error (exit code `2`)
  - other provider/client validation errors (non-429 4xx excluding `401`/`403`/`404`) -> per-item non-retryable call failure
  - locally detected schema/JSON parse failures -> no retry; map by the JSON/schema failure mapping for that call (`*_invalid_json` / `*_validation_failed`)
- Backoff schedule:
  - max attempts = `llm.retry_max_attempts`
  - delay before attempt `n` (`n >= 2`) is:
    - `min(llm.retry_backoff_max_s, llm.retry_backoff_initial_s * (llm.retry_backoff_multiplier ** (n - 2)))`
  - no jitter required.
- If retries are exhausted for a call, or a non-retryable per-item provider/client 4xx call failure occurs, map to `extract_llm_failed` or `score_llm_failed`.

### Source type and platform mapping (normative)
This mapping applies only after the pre-LLM validation gate has accepted the selected row.

Mapping:
- `newsletter` -> `platform_hint = "newsletter"`
- `youtube` -> `platform_hint = "youtube"`

### Call 1: Extract (structured JSON)
Prompt file: `config/prompts/stage_5_extract.md`

Inputs:
- title
- source_type
- url
- enriched_text truncated to first `stage_5_intelligence.input_max_chars` chars

Output MUST be valid JSON matching ExtractSchema.

ExtractSchema:
```json
{
  "topic": "string",
  "core_claim": "string",
  "workflow_steps": ["string", "..."],
  "tools_mentioned": ["string", "..."],
  "monetization_angle": "string",
  "metrics_claims": ["string", "..."],
  "assumptions": ["string", "..."],
  "content_type": "howto|case_study|tool_review|opinion|news|other"
}
```

Extract validation rules:
- All keys above are required.
- No additional keys are allowed.
- `workflow_steps` length must be between `0` and `8` (inclusive).
- List fields must contain only strings.
- `content_type` must match the enum exactly.
- Scalar extract string fields (`topic`, `core_claim`, `monetization_angle`) may be empty strings when source content does not support a value.
- Prompt alignment rule (merge-blocking): `config/prompts/stage_5_extract.md` must instruct `workflow_steps` length `0..8` and explicitly allow `[]` when no clear steps are present in source content.

Failure mapping:
- Retryable LLM transport/provider failure after retries, or non-retryable per-item provider/client failure class (non-429 4xx excluding fatal HTTP `401`/`403`/`404`) -> `extract_llm_failed`
- Response not parseable as JSON -> `extract_invalid_json`
- Parsed JSON fails schema validation -> `extract_validation_failed`

### Evidence preprocessing for score call (normative)
Input source field: `enriched_items.evidence_snippets` (JSON string).

Preprocessing steps:
1) Attempt `json.loads`.
2) If parse fails, or decoded value is not a list, use empty list `[]`.
3) From decoded list, take elements in order where element is an object/map and `element["text"]` is a string and non-empty after `strip()`.
4) Normalize each kept snippet text by collapsing internal whitespace runs and applying `strip()`.
5) Truncate each normalized snippet to max 240 chars.
6) Keep at most first 3 snippets.

This preprocessing must never create a new fail reason key.
Malformed `evidence_snippets` is treated as "no evidence available" for scoring, not as item failure.

### Call 2: Score + hooks (structured JSON)
Prompt file: `config/prompts/stage_5_score.md`

Inputs:
- `platform_hint` from source mapping above
- title
- extracted fields from Call 1
- short evidence snippets from preprocessing above

Output MUST be valid JSON matching ScoreSchema.

ScoreSchema:
```json
{
  "viral_rating": 1,
  "rating_rationale": "string",
  "hooks": ["string", "string", "string"],
  "platform": "youtube|newsletter",
  "recommended_format": "shorts|tweet|linkedin|reel|thread|other"
}
```

Score validation rules:
- All keys above are required.
- No additional keys are allowed.
- `viral_rating` must be a non-boolean integer in `[1, 10]`.
- `rating_rationale` must be a non-empty string after `strip()`.
- `hooks` must be exactly 3 strings; each hook must be non-empty after `strip()` and each hook length `<= 140`.
- `platform` must be `youtube` or `newsletter`.
- `platform` must match `platform_hint` exactly.
- `recommended_format` must match enum exactly.
- Prompt alignment rule (merge-blocking): `config/prompts/stage_5_score.md` must instruct exactly 3 hooks, each non-empty and `<= 140` chars, and must explicitly require output `platform` to equal input `platform_hint`.

Failure mapping:
- Retryable LLM transport/provider failure after retries, or non-retryable per-item provider/client failure class (non-429 4xx excluding fatal HTTP `401`/`403`/`404`) -> `score_llm_failed`
- Response not parseable as JSON -> `score_invalid_json`
- Parsed JSON fails schema validation -> `score_validation_failed`

Rating rubric (must be encoded in prompt):
- Specificity and operational detail
- Novelty and contrarian insight
- Proof signals (screenshots, real numbers) versus vague hype
- Replicability (clear steps)
- Fit for the niche "AI automations to make money"
- Generic/hype content with weak operational detail should score `<= 4`

## Per-Item Failure Taxonomy (Normative)
Fail reasons are fixed keys:
- `extract_llm_failed`
- `extract_invalid_json`
- `extract_validation_failed`
- `score_llm_failed`
- `score_invalid_json`
- `score_validation_failed`

Terminal outcome rule:
- Each processed selected item must end in exactly one terminal outcome:
  - one success, or
  - exactly one fail reason key above.
- On fatal runs, selected rows not yet processed are allowed and are explicitly non-terminal.
- First failing step wins.
- Failures must not increment multiple fail keys for a single item.

Unexpected per-item exception fallback (normative):
- Runner must guard each selected item with per-item exception handling and continue processing subsequent selected items.
- Scope limitation: this fallback applies only to per-item row validation/canonicalization and per-item extract/score execution paths.
- Excluded fatal classes must bypass this fallback and remain fatal run errors (exit code `2`):
  - SQLite operation failures and artifact I/O failures
  - prompt template load/read/render failures
  - provider auth/resource fatal classes (HTTP `401`/`403`/`404`)
- Any uncaught exception within fallback scope must map to exactly one existing fail key:
  - before first extract provider attempt starts (row validation/canonicalization and pre-LLM input prep) -> `extract_validation_failed`
  - after first extract provider attempt starts and before score path begins -> `extract_llm_failed`
  - after score path begins (including evidence preprocessing and score call path) -> `score_llm_failed`
- Uncaught per-item exceptions in fallback scope must not terminate the run.

## SQLite Persistence (`ideas` table)
Create table `ideas` if not exists:

- `item_id` TEXT PRIMARY KEY
- `source_type` TEXT NOT NULL
- `url` TEXT NOT NULL
- `title` TEXT NOT NULL
- `published_at` TEXT NOT NULL
- `topic` TEXT NOT NULL
- `core_claim` TEXT NOT NULL
- `workflow_steps` TEXT NOT NULL            # JSON string list
- `tools_mentioned` TEXT NOT NULL           # JSON string list
- `monetization_angle` TEXT NOT NULL
- `metrics_claims` TEXT NOT NULL            # JSON string list
- `assumptions` TEXT NOT NULL               # JSON string list
- `content_type` TEXT NOT NULL
- `viral_rating` INTEGER NOT NULL
- `rating_rationale` TEXT NOT NULL
- `hooks` TEXT NOT NULL                     # JSON string list, exactly 3
- `platform` TEXT NOT NULL                  # youtube|newsletter
- `recommended_format` TEXT NOT NULL
- `llm_provider` TEXT NOT NULL
- `llm_model` TEXT NOT NULL
- `created_at` TEXT NOT NULL                # UTC ISO8601 Z

### Ideas table compatibility check (normative)
Validate compatibility at run start using SQLite metadata (`PRAGMA table_info(ideas)`):
- Required columns must exist exactly as listed in this section.
- Declared type must be exactly `TEXT` for all required columns except `viral_rating`.
- Declared type must be exactly `INTEGER` for `viral_rating`.
- `item_id` must be the primary key column (`pk = 1`).
- Every required column except `item_id` must be declared `NOT NULL`.
- Extra columns are allowed.
- Any compatibility check failure is fatal (exit code `2`).

Insert rules:
- Use `INSERT OR IGNORE`.
- If insert succeeds:
  - `inserted_db += 1`
  - emit row to JSONL
- If insert ignored:
  - `skipped_already_present += 1`
  - do not emit row

Idempotency:
- If `item_id` exists, skip and count `skipped_already_present`.
- `skipped_already_present` counts only `INSERT OR IGNORE` conflicts for rows selected in this run (for example concurrent writer races).
- In normal single-process sequential reruns, expected `skipped_already_present` is `0` because selection excludes existing `ideas` rows.

Timestamp binding rule:
- For each successful item, compute one UTC timestamp string at second precision (`YYYY-MM-DDTHH:MM:SSZ`).
- Use that same value for JSONL `created_at` and DB `created_at`.

## Idea JSONL Contract
Each emitted JSONL line is one successfully inserted idea row.

Required fields:
- `item_id` (string)
- `source_type` (`newsletter` | `youtube`)
- `url` (string)
- `title` (string)
- `published_at` (UTC second-precision ISO8601 Z string)
- `topic` (string)
- `core_claim` (string)
- `workflow_steps` (list[string], length `0..8`)
- `tools_mentioned` (list[string])
- `monetization_angle` (string)
- `metrics_claims` (list[string])
- `assumptions` (list[string])
- `content_type` (`howto|case_study|tool_review|opinion|news|other`)
- `viral_rating` (non-boolean integer in `[1, 10]`)
- `rating_rationale` (string)
- `hooks` (list[string], exactly 3, each non-empty after `strip()`, each `<= 140` chars)
- `platform` (`youtube` | `newsletter`)
- `recommended_format` (`shorts|tweet|linkedin|reel|thread|other`)
- `llm_provider` (string)
- `llm_model` (string)
- `created_at` (UTC second-precision ISO8601 Z string)

JSONL serialization rule (normative):
- JSONL list fields (`workflow_steps`, `tools_mentioned`, `metrics_claims`, `assumptions`, `hooks`) must be emitted as JSON arrays, not JSON-encoded strings.
- SQLite persists these list fields as `TEXT` JSON strings per the `ideas` table schema.

Output ordering:
- JSONL row order must match deterministic processing order from the selection query.

## Stage 5 Report (JSON)
Write report to default `{outputs_dir}/stage_5_report_<YYYY-MM-DD>.json` unless overridden.

Required fields:
- `run_id` (UUID4 string)
- `run_status` (`completed` | `fatal`)
- `fatal_error` (string|null)
- `started_at` (UTC second-precision ISO8601 Z)
- `finished_at` (UTC second-precision ISO8601 Z)
- `db_path`
- `output_path`
- `report_path`
- `items_available_total`
- `selected_rows_total`
- `items_selected`
- `success_count`
- `failed_count`
- `inserted_db`
- `skipped_already_present`
- `max_items`
- `llm_provider`
- `llm_model`
- `fail_breakdown` map with keys:
  - `extract_llm_failed`
  - `extract_invalid_json`
  - `extract_validation_failed`
  - `score_llm_failed`
  - `score_invalid_json`
  - `score_validation_failed`

Run status semantics:
- completed run (`exit code 0`): `run_status = "completed"` and `fatal_error = null`
- fatal run (`exit code 2`) with report write success: `run_status = "fatal"` and `fatal_error` is a non-empty string summary

Fatal report behavior (normative):
- Fatal report attempt policy applies to all fatal classes listed in this spec once `report_path` is known.
- For fatal reports, all required numeric counters and all `fail_breakdown` keys must still be present with non-boolean integer values `>= 0`; use `0` when unavailable due to early fatal termination.
- If fatal occurs before non-report paths are known, `db_path` and `output_path` may be empty strings.
- If fatal occurs before pipeline defaults are available, `max_items` must still be present:
  - use validated `--max-items` override when provided
  - otherwise emit `0`
- If fatal occurs before pipeline defaults are available:
  - `llm_provider` must still be present; emit `"openai"` (current only supported provider)
  - `llm_model` must still be present; use validated `--model` override when provided, else empty string

Counter invariants:
- `items_selected = success_count + failed_count`
- `success_count = inserted_db + skipped_already_present`
- `failed_count = sum(fail_breakdown.values())`
- `items_selected <= selected_rows_total`
- `selected_rows_total <= max_items`
- `items_selected <= items_available_total`
- on completed run: `items_selected == selected_rows_total`

## Outputs
- JSONL: `data/outputs/ideas_<YYYY-MM-DD>.jsonl` (inserted successes only)
- JSON: `data/outputs/stage_5_report_<YYYY-MM-DD>.json`
- SQLite: `ideas` table created/updated in configured DB

## Tests (pytest) - Minimum
Add `tests/test_stage_5_intelligence.py` with mocked LLM responses (no network):

1) Extract/score success path
- valid mocked JSON for both calls inserts DB row and emits JSONL row
- emitted JSONL list fields are arrays (not JSON-encoded strings)

2) Call gating (extract failure short-circuits score)
- extract failure maps correctly and score call is not attempted for that item

3) Hook contract
- hooks must be exactly 3, each non-empty after `strip()`, and each `<= 140` chars
- `rating_rationale` must be non-empty after `strip()`

4) JSON/schema failure mapping
- extract invalid JSON -> `extract_invalid_json`
- extract schema violation -> `extract_validation_failed`
- score invalid JSON -> `score_invalid_json`
- score schema violation -> `score_validation_failed`

5) LLM failure mapping + retries
- transient errors exhausted on extract -> `extract_llm_failed`
- transient errors exhausted on score -> `score_llm_failed`
- non-retryable per-item 4xx (for example HTTP `400`/`422`) map to `extract_llm_failed`/`score_llm_failed` without retry
- fatal provider auth/resource classes (HTTP `401`/`403`/`404`) terminate run with exit code `2`
- retry attempt count/backoff path respects configured max attempts
- attempt scheduling respects both constraints per retry: global RPM spacing and per-call backoff delay

6) Evidence preprocessing fallback
- malformed `evidence_snippets` JSON is coerced to empty evidence list and does not create a new fail reason

7) Pre-LLM validation gate
- malformed selected-row required fields map to `extract_validation_failed` and skip all LLM calls
- unsupported `source_type` maps to `extract_validation_failed` and skips all LLM calls

8) Platform mapping enforcement
- score output `platform` mismatch against `platform_hint` maps to `score_validation_failed`

9) Idempotency
- pre-seed `ideas`, rerun, ensure pre-seeded rows are not selected and not emitted

10) Stage dependency validation
- missing `enriched_items` table or missing required columns exits with code `2`

11) Ideas table compatibility validation
- missing `ideas` table is created via `CREATE TABLE IF NOT EXISTS` and then validated
- incompatible pre-existing `ideas` schema exits with code `2`

12) Report invariants
- assert all required fields and counter invariants

13) Fatal report behavior
- fatal output/report I/O path exits `2`
- when fatal report write succeeds, `run_status = "fatal"` and non-empty `fatal_error`
- pre-config fatal with `--report` override attempts report write
- override validation failure with `--report` override attempts fatal report write (invalid `--max-items` and invalid `--model`)

14) Deterministic selection/order
- same seed data always processes `published_at DESC, item_id ASC`

15) `max_items == 0` edge case
- no rows selected, no LLM calls, empty JSONL, counters consistent

16) API key resolution
- uses env var named by `llm.api_key_env_var` when env value is non-empty after `strip()`
- falls back to `llm.api_key` when non-empty after `strip()`
- missing/empty both exits with code `2`

17) Prompt file failure behavior
- missing/unreadable `config/prompts/stage_5_extract.md` or `config/prompts/stage_5_score.md` exits with code `2`
- prompt render/input-template failure exits with code `2`

18) Unexpected per-item exception fallback
- uncaught exception in pre-LLM validation/input-prep path maps to `extract_validation_failed` and run continues
- uncaught exception in extract execution path maps to `extract_llm_failed` and run continues
- uncaught exception in score execution path (including evidence preprocessing) maps to `score_llm_failed` and run continues

## Files Changed (Expected)
- `app/intelligence/*` (new)
- `config/prompts/stage_5_extract.md` (existing, may be refined)
- `config/prompts/stage_5_score.md` (existing, may be refined)
- `tests/test_stage_5_intelligence.py` (new)
- `config/pipeline.yaml` (add `llm` + `stage_5_intelligence`)
- `config/pipeline.local.yaml` (local secret-bearing pipeline file)
- `docs/config_schemas.md` (add Stage 5 config contract)

Config-doc sync gate (normative):
- Stage 5 implementation is not ready to merge unless `docs/config_schemas.md` includes `llm.*` and `stage_5_intelligence.*` schema + validation rules used by this spec.

Prompt-spec sync gate (normative):
- Stage 5 implementation is not ready to merge unless prompt templates under `config/prompts/` are aligned with this spec.
- This spec is the contract source of truth; if prompt templates conflict, templates must be updated in the same change set.
- Spec-readiness clarification: this document remains implementation-ready even when current prompt files are stale; merge-readiness for Stage 5 code requires prompt updates that satisfy the constraints below.
- Required prompt constraints:
  - `config/prompts/stage_5_extract.md` explicitly allows `workflow_steps` length `0..8` and allows `[]` when source evidence has no clear steps.
  - `config/prompts/stage_5_score.md` explicitly requires exactly 3 non-empty hooks (`<= 140` chars each) and requires output `platform == platform_hint`.
- Any prompt/spec contract conflict is merge-blocking and must be resolved in the same change set.

## Commands to Run (Expected)
- `python -m app.intelligence.cli --pipeline config/pipeline.local.yaml`
- `python -m app.intelligence.cli --pipeline config/pipeline.yaml` (shared/CI style; run with `OPENAI_API_KEY` set in environment)
- `pytest -q tests/test_stage_5_intelligence.py`

## Produced Artifacts
- `data/outputs/ideas_<YYYY-MM-DD>.jsonl`
- `data/outputs/stage_5_report_<YYYY-MM-DD>.json`
- SQLite table `ideas` in configured DB
