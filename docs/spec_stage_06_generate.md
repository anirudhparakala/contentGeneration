# SPEC - STAGE 6: Generate (Short-form Scripts) - Manual Batch

## Depends on
- Stage 0
- Stage 5 (`ideas` table exists and is schema-compatible; table may be empty)

## Objective
Generate short-form script drafts from Stage 5 idea rows for each row's `platform` and `recommended_format`.
Persist generated scripts to SQLite `scripts` and emit run JSONL/report artifacts.

Stage 6 uses an LLM. It does not deliver to Slack/email and does not write to Google Sheets.

## In Scope
- Read defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - `llm.*`
  - `stage_6_generate.*`
- Read inputs from SQLite `ideas`.
- Select rows not already present in `scripts` (idempotent).
- For each selected row, run one LLM generation call and validate result.
- Persist successful rows to SQLite `scripts`.
- Emit:
  - JSONL of rows inserted this run only
  - Stage 6 report JSON

## Out of Scope
- Stage 4 enrichment/transcription
- Stage 5 extraction/scoring
- Stage 7 Sheets write
- Stage 8 Slack/email delivery

## Repo Layout (Must Follow)
Implement code under:
- `app/generate/models.py` (script payload models + validation)
- `app/generate/prompts.py` (prompt loader/render)
- `app/generate/llm.py` (LLM client wrapper; same policy class as Stage 5)
- `app/generate/state.py` (SQLite helpers for ideas/scripts tables)
- `app/generate/runner.py`
- `app/generate/cli.py`
- `app/generate/__init__.py`

## Required Pipeline Config (Stage 6)
Add:

```yaml
stage_6_generate:
  max_items_default: 25
```

Validation rules:
- top-level config must be a mapping.
- required mappings: `paths`, `llm`, `stage_6_generate`.
- `paths.sqlite_db`: non-empty string after `strip()`.
- `paths.outputs_dir`: non-empty string after `strip()`.
- `llm.*` validation rules are the same as Stage 5 (`docs/spec_stage_05_intelligence.md`).
- `stage_6_generate.max_items_default`: non-boolean integer `>= 0`.

If invalid config is detected, fail run with exit code `2`.

## Secrets (Local Workflow)
Use a local pipeline file for API key material.

Create `config/pipeline.local.yaml` locally as a full copy of `config/pipeline.yaml` (do not commit secrets). Keep `llm.api_key` there.

Pipeline file loading rule (normative):
- Stage 6 CLI accepts exactly one pipeline path via `--pipeline`.
- Stage 6 does not perform implicit multi-file merge/overlay.

API key resolution order (normative):
1. Read environment variable named by `llm.api_key_env_var`; if value is non-empty after `strip()`, use it.
2. Else if `llm.api_key` exists and is non-empty after `strip()`, use it.
3. Else fatal run error (exit code `2`).

API key logging rule (normative):
- Never log secret values.
- Logs may include only key source metadata (`env` or `config`).

Security rule:
- Never commit a real API key to tracked files.

## CLI Contract
Primary local command:
- `python -m app.generate.cli --pipeline config/pipeline.local.yaml`

Shared/CI command (API key supplied via environment variable):
- `python -m app.generate.cli --pipeline config/pipeline.yaml`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--out <path>` overrides default output path
- `--report <path>` overrides default report path
- `--max-items <int>` overrides `stage_6_generate.max_items_default`
- `--model <name>` overrides `llm.model`
- `--log-level <LEVEL>` default `INFO`

Override validation:
- `--db`, `--out`, and `--report` must be non-empty strings after `strip()`; otherwise fatal error (exit code `2`).
- `--max-items` must be a non-boolean integer `>= 0`; otherwise fatal error (exit code `2`).
- `--model` must be a non-empty string after `strip()`; otherwise fatal error (exit code `2`).
- Override validation failures are fatal run errors.
- If override validation fails and `--report` was provided with a valid non-empty path, that value defines `report_path` and Stage 6 must attempt fatal report write to that path.
- If `--report` is provided but fails path validation, `report_path` is unresolved and fatal report write is skipped.

Defaults:
- DB: `paths.sqlite_db`
- out: `{paths.outputs_dir}/scripts_<YYYY-MM-DD>.jsonl` if `--out` omitted
- report: `{paths.outputs_dir}/stage_6_report_<YYYY-MM-DD>.json` if `--report` omitted
- max-items: `stage_6_generate.max_items_default`

Date basis:
- Bind one `run_date_utc` at run start from `started_at` (UTC).
- Default output/report filenames must use `run_date_utc` for the entire run.

Behavior:
- Overwrite output JSONL each run.
- Always create/truncate output JSONL once `output_path` is resolved and before item processing begins; when fatal termination occurs before `output_path` is resolved, output file creation is skipped.
- Create parent directories for `output_path` and `report_path` when missing (`mkdir -p` semantics).
- Process selected rows in deterministic order in a single-worker sequential loop (no parallel row processing and no parallel LLM request execution).
- Continue on per-item failures; do not fail entire run for item-level failures.
- Fatal runtime policy (normative):
  - Any SQLite operation failure that is not an expected `INSERT OR IGNORE` conflict is a fatal run error (exit code `2`).
  - Any failure to write/truncate output JSONL or write report JSON is a fatal run error (exit code `2`).
  - Any failure to read/load prompt template (`config/prompts/stage_6_script.md`) or render required prompt inputs is a fatal run error (exit code `2`).
  - Provider auth/resource fatal classes are fatal run errors (exit code `2`): HTTP `401`, HTTP `403`, and HTTP `404` from generation calls.
  - On fatal run error, stop processing immediately.
  - Fatal SQLite/artifact I/O errors are not per-item failures and must never be remapped into `fail_breakdown` reasons.
- Fatal artifact policy (normative):
  - Output JSONL is best-effort and may be partially written with rows emitted before a fatal run error.
  - No rollback/truncation of already-written JSONL rows is required after fatal run error.
  - If fatal termination occurs before output truncate/open succeeds, output JSONL may be absent even when `output_path` is already known.
  - Stage 6 must attempt to write report JSON on any fatal run error once `report_path` is resolved.
  - If fatal occurs before `report_path` is resolved (for example pipeline read/parse failure with no `--report` override), report write is skipped and run exits `2`.
  - If `--report` is provided with a valid non-empty path, that value defines `report_path` even when pipeline load fails; Stage 6 must attempt fatal report write to that path.
  - If `--report` is invalid, `report_path` remains unresolved and report write is skipped.
- Exit code:
  - `0` = completed (even if some rows failed)
  - `2` = fatal error (invalid config/overrides, prompt load/render failure, missing API key, provider auth/resource fatal class, cannot open DB, cannot write outputs)

## Input Contract (SQLite `ideas` table)
Stage 6 reads:
- `ideas.item_id`
- `ideas.platform`
- `ideas.recommended_format`
- `ideas.url`
- `ideas.title`
- `ideas.published_at`
- `ideas.topic`
- `ideas.core_claim`
- `ideas.workflow_steps` (JSON string)
- `ideas.tools_mentioned` (JSON string)
- `ideas.monetization_angle`
- `ideas.metrics_claims` (JSON string)
- `ideas.assumptions` (JSON string)
- `ideas.hooks` (JSON string)
- `ideas.viral_rating`

Stage dependency validation (normative):
- On run start, Stage 6 must verify that table `ideas` exists.
- On run start, `ideas` must pass explicit compatibility checks in the `Ideas table compatibility check` section.
- On run start, Stage 6 must execute `CREATE TABLE IF NOT EXISTS scripts (...)` using schema defined in this spec.
- After table ensure, `scripts` must pass explicit compatibility checks in the `Scripts table compatibility check` section.
- Stage 6 must not auto-migrate incompatible existing `scripts` schemas.
- Non-empty `ideas` input is not required; zero available rows is a valid completed run (`exit code 0`) with zero row counters.
- If `ideas` is missing/unreadable/invalid, or if `scripts` ensure/check fails, fail run with exit code `2`.

### Ideas table compatibility check (normative)
Validate compatibility at run start using SQLite metadata (`PRAGMA table_info(ideas)`):
- Required columns must exist by name:
  - `item_id`
  - `platform`
  - `recommended_format`
  - `url`
  - `title`
  - `published_at`
  - `topic`
  - `core_claim`
  - `workflow_steps`
  - `tools_mentioned`
  - `monetization_angle`
  - `metrics_claims`
  - `assumptions`
  - `hooks`
  - `viral_rating`
- Declared type must be exactly `TEXT` for all required columns except `viral_rating`.
- Declared type must be exactly `INTEGER` for `viral_rating`.
- `item_id` must be the primary key column (`pk = 1`).
- Every required column except `item_id` must be declared `NOT NULL`.
- Extra columns are allowed.
- Any compatibility check failure is fatal (exit code `2`).

Selection query:

```sql
SELECT
  i.item_id,
  i.platform,
  i.recommended_format,
  i.url,
  i.title,
  i.published_at,
  i.topic,
  i.core_claim,
  i.workflow_steps,
  i.tools_mentioned,
  i.monetization_angle,
  i.metrics_claims,
  i.assumptions,
  i.hooks,
  i.viral_rating
FROM ideas i
LEFT JOIN scripts s ON s.item_id = i.item_id
WHERE s.item_id IS NULL
ORDER BY i.viral_rating DESC, i.published_at DESC, i.item_id ASC
LIMIT :max_items;
```

Definitions:
- `items_available_total`: count of rows matching `WHERE s.item_id IS NULL` before `LIMIT`.
- `selected_rows_total`: count of rows returned by selection query after `LIMIT`.
- `items_selected`: selected rows that reached terminal outcome (`success` or exactly one mapped failure reason) before run end.
- On completed runs, `items_selected == selected_rows_total`.
- On fatal runs, `items_selected` may be lower than `selected_rows_total` because processing stops immediately.
- On fatal runs, selected rows not yet processed at stop time are non-terminal and must not be counted in `success_count`, `failed_count`, or `fail_breakdown`.
- If `max_items == 0`, both `selected_rows_total` and `items_selected` must be `0`.

### Per-row pre-LLM validation gate (normative)
For each selected row, validate/canonicalize before any LLM call:
- `item_id`, `platform`, `recommended_format`, `url`, `title`, `published_at` must be strings and non-empty after `strip()`.
- `topic`, `core_claim`, `monetization_angle` must be strings (may be empty after `strip()`).
- Canonicalization for all scalar strings above: `value = value.strip()`.
- `published_at` must match UTC second-precision ISO8601 `YYYY-MM-DDTHH:MM:SSZ`.
- `platform` must be exactly `youtube` or `newsletter`.
- `recommended_format` must be one of `shorts|tweet|linkedin|reel|thread|other`.
- `viral_rating` must be non-boolean integer in `[1, 10]`.
- JSON string fields `workflow_steps`, `tools_mentioned`, `metrics_claims`, `assumptions`, `hooks` must each decode to a JSON array of strings.
- For each decoded JSON-array field above:
  - Trim each string element with `strip()`.
  - Remove elements that are empty after trim.
- After trim normalization:
  - `workflow_steps` length must be `0..8`.
  - `hooks` length must be exactly `3`.
- If any check above fails, map row to `script_validation_failed` and skip all LLM calls for that row.

## Prompting Contract
Stage 6 runs one LLM generation call per selected row that passes the pre-LLM validation gate.

Prompt source-of-truth rule (normative):
- This spec is authoritative for Stage 6 runtime behavior and validation.
- Prompt template must be aligned to this spec; implementation must not relax runtime validation to match stale prompt text.

Terminal call-flow rule (normative):
- First failing step wins.
- Once a selected row hits one mapped failure reason, no further processing/calls for that row are allowed.

Rate-limit rule (normative):
- Generation calls count toward `llm.requests_per_minute_soft`.
- Every provider request attempt counts toward this cap, including retries.
- Apply deterministic throttle before each attempt: ensure at least `60.0 / llm.requests_per_minute_soft` seconds between consecutive attempt start times.

Attempt scheduling rule (normative):
- Let `rpm_spacing_s = 60.0 / llm.requests_per_minute_soft`.
- Let `last_global_attempt_start` be the most recent attempt start timestamp across all rows/retries.
- For any attempt, earliest start by RPM rule is `last_global_attempt_start + rpm_spacing_s` (or immediate if no prior attempt exists).
- For retry attempt `n >= 2` of the same row call, earliest start by backoff rule is `previous_attempt_end + backoff_delay(n)`.
- Actual attempt start must satisfy both constraints:
  - `attempt_start >= earliest_rpm_start`
  - `attempt_start >= earliest_backoff_start` (retry attempts only)
- Equivalent deterministic implementation: `attempt_start = max(now, earliest_rpm_start, earliest_backoff_start_if_retry)`.

Structured output rule (normative):
- For provider `openai`, call must request strict JSON-schema output.
- Do not request free-form text output.
- Even with schema-constrained output, runner must still parse and validate locally.

LLM request parameter binding rule (normative):
- Every provider attempt must use:
  - provider: `llm.provider`
  - model: resolved model (`--model` override when provided; otherwise `llm.model`)
  - temperature: `llm.temperature`
  - max output tokens: `llm.max_output_tokens`
  - timeout: `llm.request_timeout_s`

Retry rule (normative):
- Retry policy is config-driven by `llm.retry_*` and `llm.request_timeout_s`.
- Retryable classes:
  - transport/network timeouts and connection failures
  - provider rate-limit responses (for example HTTP `429`)
  - provider transient server errors (HTTP `5xx`)
- Non-retryable per-item classes:
  - other provider/client validation errors (non-429 `4xx` excluding fatal `401`/`403`/`404`)
- Fatal provider classes:
  - HTTP `401`/`403`/`404` are fatal run errors (exit code `2`)
- Backoff schedule:
  - max attempts = `llm.retry_max_attempts`
  - delay before attempt `n` (`n >= 2`):
    - `min(llm.retry_backoff_max_s, llm.retry_backoff_initial_s * (llm.retry_backoff_multiplier ** (n - 2)))`
  - no jitter required.
- If retries are exhausted for a row call, or non-retryable per-item provider/client `4xx` occurs, map row to `script_llm_failed`.

## Prompt Contract (`config/prompts/stage_6_script.md`)
Use existing prompt template file:
- `config/prompts/stage_6_script.md`

Required injected inputs:
- `platform`
- `recommended_format`
- `title`
- `url`
- `topic`
- `core_claim`
- `workflow_steps`
- `tools_mentioned`
- `monetization_angle`
- `metrics_claims`
- `assumptions`
- `prior_hooks`

Prompt input serialization contract (normative):
- Prompt rendering must fail-fast on placeholder set mismatch; this is a fatal run error (exit code `2`).
- Placeholder tokens must be extracted from template text using pattern `{{[A-Z0-9_]+}}`.
- Required placeholder set is exactly:
  - `PLATFORM`
  - `RECOMMENDED_FORMAT`
  - `TITLE`
  - `URL`
  - `TOPIC`
  - `CORE_CLAIM`
  - `WORKFLOW_STEPS`
  - `TOOLS_MENTIONED`
  - `MONETIZATION_ANGLE`
  - `METRICS_CLAIMS`
  - `ASSUMPTIONS`
  - `PRIOR_HOOKS`
- Any missing required placeholder or any extra placeholder is a fatal run error (exit code `2`).
- Renderer must bind exactly these template placeholders:
  - `PLATFORM` <- selected-row `platform`
  - `RECOMMENDED_FORMAT` <- selected-row `recommended_format`
  - `TITLE` <- selected-row `title`
  - `URL` <- selected-row `url`
  - `TOPIC` <- selected-row `topic`
  - `CORE_CLAIM` <- selected-row `core_claim`
  - `WORKFLOW_STEPS` <- selected-row `workflow_steps`
  - `TOOLS_MENTIONED` <- selected-row `tools_mentioned`
  - `MONETIZATION_ANGLE` <- selected-row `monetization_angle`
  - `METRICS_CLAIMS` <- selected-row `metrics_claims`
  - `ASSUMPTIONS` <- selected-row `assumptions`
  - `PRIOR_HOOKS` <- selected-row `hooks`
- Scalar placeholders (`PLATFORM`, `RECOMMENDED_FORMAT`, `TITLE`, `URL`, `TOPIC`, `CORE_CLAIM`, `MONETIZATION_ANGLE`) must be injected as trimmed strings.
- Array placeholders must be injected as JSON text produced via `json.dumps(value, ensure_ascii=True)`:
  - `WORKFLOW_STEPS`
  - `TOOLS_MENTIONED`
  - `METRICS_CLAIMS`
  - `ASSUMPTIONS`
  - `PRIOR_HOOKS` (maps from selected-row `hooks`)

Required semantic guidance in prompt:
- Use only provided fields; do not invent tools, steps, metrics, or outcomes.
- If metrics are referenced, phrase as claims unless explicitly verified by input evidence.
- Hooks must avoid clickbait and reference concrete mechanism/tool/workflow.
- Must include 3 to 6 workflow bullets in plain language; each bullet line must start with `- `.
- Should mention relevant tools when available.

Format policy (normative):
- For `recommended_format in {"shorts", "reel"}`:
  - `computed_word_count` must be `120..170`
  - `estimated_seconds` must be `45..70`
- For `recommended_format in {"tweet", "thread", "linkedin", "other"}`:
  - `computed_word_count` must be `180..260`
  - `estimated_seconds` must be `70..110`

Prompt alignment rule (merge-blocking):
- `config/prompts/stage_6_script.md` must explicitly enforce the same format buckets used by this spec:
  - `shorts` and `reel` use `word_count 120..170` and `estimated_seconds 45..70`
  - `tweet`, `thread`, `linkedin`, and `other` use `word_count 180..260` and `estimated_seconds 70..110`
- Prompt format logic must use the explicit enum sets above; a generic fallback branch like `otherwise` is non-compliant because it can misbucket `reel`.
- Any prompt/spec contract conflict is merge-blocking and must be resolved in the same change set.

## LLM Output Schema and Validation (Normative)
Model output must parse as JSON object with exact keys and no additional keys:

```json
{
  "primary_hook": "string",
  "alt_hooks": ["string", "string"],
  "script": {
    "sections": [
      {"label": "hook", "text": "string"},
      {"label": "setup", "text": "string"},
      {"label": "steps", "text": "string"},
      {"label": "cta", "text": "string"}
    ],
    "word_count": 0,
    "estimated_seconds": 0
  },
  "cta": "string",
  "disclaimer": "string"
}
```

Validation rules:
- Top-level object must contain exactly these keys (no missing/extra keys):
  - `primary_hook`
  - `alt_hooks`
  - `script`
  - `cta`
  - `disclaimer`
- `script` must be an object containing exactly keys:
  - `sections`
  - `word_count`
  - `estimated_seconds`
- Output canonicalization rule (normative):
  - After JSON parse and before validation/persistence, trim with `strip()`:
    - `primary_hook`
    - each entry in `alt_hooks`
    - each `script.sections[*].label`
    - each `script.sections[*].text`
    - `cta`
    - `disclaimer`
  - Validation checks, CTA equality, steps section parsing, `computed_word_count` computation, JSONL emission, and SQLite persistence must use canonicalized values.
  - Persisted values in SQLite and emitted values in JSONL must be canonicalized values.
- `primary_hook`: non-empty string after `strip()`, length `<= 140`.
- `alt_hooks`: list of exactly 2 strings; each non-empty after `strip()`, each length `<= 140`.
- `script.sections`: list of exactly 4 objects in this exact order and labels:
  - `hook`
  - `setup`
  - `steps`
  - `cta`
- Each section object must contain exactly keys `label` and `text`.
- Each section `text`: non-empty string after `strip()`.
- `steps` section parsing/validation algorithm (normative):
  - normalize line endings to `\n`
  - split by `\n`
  - trim each line with `strip()`
  - discard empty lines
  - remaining lines must be exactly `3..6`
  - every remaining line must start with `- ` and must contain non-empty text after the prefix
- `script.word_count`: non-boolean integer (model-reported value; informational only).
- `computed_word_count` (authoritative runtime value):
  - token regex is exactly `[A-Za-z0-9]+(?:['-][A-Za-z0-9]+)*`
  - use canonicalized section texts in required order (`hook`, `setup`, `steps`, `cta`)
  - compute per section, then sum:
    - `section_word_count = len(re.findall(TOKEN_RE, section_text))`
    - `computed_word_count = sum(section_word_count for each section in order)`
  - section boundaries must not change the result (do not rely on concatenation separators)
  - format policy word-count ranges are enforced against `computed_word_count` (not model-reported `script.word_count`)
  - persisted `word_count` in SQLite and JSONL must equal `computed_word_count`
- `script.estimated_seconds`: non-boolean integer matching format policy range above.
- `cta`: non-empty string after `strip()`.
- `cta` must equal `script.sections[label=cta].text` after `strip()`.
- `disclaimer`: string (may be empty after `strip()`).

Failure mapping:
- Retryable LLM transport/provider failure after retries, or non-retryable per-item provider/client failure class (non-429 `4xx` excluding fatal `401`/`403`/`404`) -> `script_llm_failed`
- Response not parseable as JSON -> `script_invalid_json`
- Parsed JSON fails schema/validation rules -> `script_validation_failed`

## Per-Item Failure Taxonomy (Normative)
Fail reasons are fixed keys:
- `script_llm_failed`
- `script_invalid_json`
- `script_validation_failed`

Terminal outcome rule:
- Each processed selected row must end in exactly one terminal outcome:
  - one success, or
  - exactly one fail reason key above.
- On fatal runs, selected rows not yet processed are non-terminal.
- First failing step wins.

Unexpected per-item exception fallback (normative):
- Runner must guard each selected row with per-item exception handling and continue processing subsequent selected rows.
- Scope limitation: fallback applies only to row validation/canonicalization and per-row generation execution paths.
- Excluded fatal classes must bypass fallback and remain fatal run errors (exit code `2`):
  - SQLite operation failures and artifact I/O failures
  - prompt template load/read/render failures
  - provider auth/resource fatal classes (HTTP `401`/`403`/`404`)
- Any uncaught exception within fallback scope must map to exactly one existing fail key:
  - before first provider attempt starts -> `script_validation_failed`
  - after first provider attempt starts -> `script_llm_failed`
- Uncaught per-item exceptions in fallback scope must not terminate run.

## SQLite Persistence (`scripts` table)
Create table `scripts` if not exists:

- `item_id` TEXT PRIMARY KEY
- `platform` TEXT NOT NULL
- `recommended_format` TEXT NOT NULL
- `primary_hook` TEXT NOT NULL
- `alt_hooks` TEXT NOT NULL              # JSON string list (exactly 2)
- `script_sections` TEXT NOT NULL        # JSON string list of 4 labeled sections
- `word_count` INTEGER NOT NULL
- `estimated_seconds` INTEGER NOT NULL
- `cta` TEXT NOT NULL
- `disclaimer` TEXT NOT NULL
- `llm_provider` TEXT NOT NULL
- `llm_model` TEXT NOT NULL
- `created_at` TEXT NOT NULL             # UTC second-precision ISO8601 Z

### Scripts table compatibility check (normative)
Validate compatibility at run start using SQLite metadata (`PRAGMA table_info(scripts)`):
- Required columns must exist by name with the exact declared type/primary-key/NOT NULL constraints defined in this section; column order is not significant.
- Declared type must be exactly `TEXT` for all required columns except `word_count` and `estimated_seconds`.
- Declared type must be exactly `INTEGER` for `word_count` and `estimated_seconds`.
- `item_id` must be the primary key column (`pk = 1`).
- Every required column except `item_id` must be declared `NOT NULL`.
- Extra columns are allowed.
- Any compatibility check failure is fatal (exit code `2`).

Insert rules:
- Use `INSERT OR IGNORE`.
- Field mapping for persistence:
  - `alt_hooks` column stores canonicalized output `alt_hooks` as JSON text via `json.dumps(value, ensure_ascii=True)`.
  - `script_sections` column stores canonicalized output `script.sections` as JSON text via `json.dumps(value, ensure_ascii=True)`.
- If insert succeeds:
  - `inserted_db += 1`
  - emit row to JSONL
- If insert ignored:
  - `skipped_already_present += 1`
  - do not emit row

Idempotency:
- If `item_id` exists, skip and count `skipped_already_present`.
- `skipped_already_present` counts only `INSERT OR IGNORE` conflicts for rows selected in this run (for example concurrent writer races).
- In normal single-process sequential reruns, expected `skipped_already_present` is `0` because selection excludes existing `scripts` rows.

Timestamp binding rule:
- For each successful row, compute one UTC timestamp string at second precision (`YYYY-MM-DDTHH:MM:SSZ`).
- Use that same value for JSONL `created_at` and DB `created_at`.

## Script JSONL Contract
Each emitted JSONL line is one successfully inserted scripts row.

Required fields:
- `item_id` (string)
- `platform` (`youtube` | `newsletter`)
- `recommended_format` (`shorts|tweet|linkedin|reel|thread|other`)
- `primary_hook` (string)
- `alt_hooks` (list[string], exactly 2)
- `script_sections` (list[object], exactly 4 labeled section objects in required order)
- `word_count` (non-boolean integer, authoritative locally computed value)
- `estimated_seconds` (non-boolean integer)
- `cta` (string)
- `disclaimer` (string, may be empty)
- `llm_provider` (string)
- `llm_model` (string)
- `created_at` (UTC second-precision ISO8601 Z string)
- Field mapping: emitted JSONL `script_sections` is the canonicalized model output `script.sections`.

JSONL serialization rule (normative):
- JSONL list fields (`alt_hooks`, `script_sections`) must be emitted as JSON arrays, not JSON-encoded strings.
- SQLite persists these list fields as `TEXT` JSON strings per table schema.

Output ordering:
- JSONL row order must match deterministic processing order from the selection query.

## Stage 6 Report (JSON)
Write report to default `{outputs_dir}/stage_6_report_<YYYY-MM-DD>.json` unless overridden.

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
  - `script_llm_failed`
  - `script_invalid_json`
  - `script_validation_failed`

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
- JSONL: `data/outputs/scripts_<YYYY-MM-DD>.jsonl` (inserted successes only)
- JSON: `data/outputs/stage_6_report_<YYYY-MM-DD>.json`
- SQLite: `scripts` table created/updated in configured DB

## Tests (pytest) - Minimum
Add `tests/test_stage_6_generate.py` with mocked LLM responses (no network):

1. Success path
- valid mocked JSON inserts DB row and emits JSONL row
- emitted JSONL list fields are arrays (not JSON-encoded strings)

2. Pre-LLM validation gate
- malformed selected-row fields map to `script_validation_failed` and skip LLM call
- malformed JSON list fields from `ideas` map to `script_validation_failed` and skip LLM call

3. JSON/schema failure mapping
- invalid JSON response -> `script_invalid_json`
- parsed JSON schema/rule violation -> `script_validation_failed`

4. LLM failure mapping + retries
- transient errors exhausted -> `script_llm_failed`
- non-retryable per-item `4xx` (for example HTTP `400`/`422`) -> `script_llm_failed` without retry
- fatal provider auth/resource classes (HTTP `401`/`403`/`404`) terminate run with exit code `2`
- retry attempt count/backoff path respects configured max attempts and RPM spacing

5. CTA coherence
- mismatch between top-level `cta` and section `cta` maps to `script_validation_failed`

6. Format policy enforcement
- `shorts`/`reel` and non-video formats enforce their required `computed_word_count`/`estimated_seconds` ranges
- persisted `word_count` equals locally computed authoritative word count from section text (not model self-report)

7. Idempotency
- pre-seed `scripts`, rerun, ensure pre-seeded rows are not selected and not emitted

8. Stage dependency validation
- missing `ideas` table or missing required columns exits with code `2`
- incompatible `ideas` schema (type/PK/NOT NULL mismatch for required Stage 6 input columns) exits with code `2`

9. Scripts table compatibility validation
- missing `scripts` table is created via `CREATE TABLE IF NOT EXISTS` and then validated
- incompatible pre-existing `scripts` schema exits with code `2`

10. Report invariants
- assert all required fields and counter invariants

11. Fatal report behavior
- fatal output/report I/O path exits `2`
- when fatal report write succeeds, `run_status = "fatal"` and non-empty `fatal_error`
- pre-config fatal with `--report` override attempts report write
- override validation failure with `--report` override attempts fatal report write (invalid `--max-items` and invalid `--model`)
- invalid `--db`/`--out`/`--report` override values fail with exit code `2`
- when `--report` override itself is invalid, fatal report write is skipped because `report_path` is unresolved

12. Deterministic selection/order
- same seed data always processes `viral_rating DESC, published_at DESC, item_id ASC`

13. `max_items == 0` edge case
- no rows selected, no LLM calls, empty JSONL, counters consistent

14. API key resolution
- uses env var named by `llm.api_key_env_var` when env value is non-empty after `strip()`
- falls back to `llm.api_key` when non-empty after `strip()`
- missing/empty both exits with code `2`

15. Prompt file failure behavior
- missing/unreadable `config/prompts/stage_6_script.md` exits with code `2`
- prompt render/input-template failure exits with code `2`

16. Prompt input binding/serialization
- missing or extra prompt placeholder/input is fatal (`exit code 2`)
- array inputs (`workflow_steps`, `tools_mentioned`, `metrics_claims`, `assumptions`, `prior_hooks`) are injected as JSON text via `json.dumps(..., ensure_ascii=True)`

17. Unexpected per-item exception fallback
- uncaught exception before provider attempt maps to `script_validation_failed` and run continues
- uncaught exception after provider attempt starts maps to `script_llm_failed` and run continues

## Files Changed (Expected)
- `app/generate/*` (new)
- `config/prompts/stage_6_script.md` (existing, may be refined for sync)
- `tests/test_stage_6_generate.py` (new)
- `config/pipeline.yaml` (add `stage_6_generate`)
- `config/pipeline.local.yaml` (local secret-bearing pipeline file)
- `docs/config_schemas.md` (add Stage 6 config contract)

Config-doc/pipeline sync gate (normative):
- Stage 6 code changes are not ready to merge unless `docs/config_schemas.md` includes `stage_6_generate.*` schema + validation rules used by this spec.
- Stage 6 code changes are not ready to merge unless `config/pipeline.yaml` defines `stage_6_generate.max_items_default`.
- Spec-readiness clarification: this document remains implementation-ready as the Stage 6 contract even when repository HEAD is stale; merge-readiness requires updating both files above in the same change set.

Prompt-spec sync gate (normative):
- Stage 6 code changes are not ready to merge unless `config/prompts/stage_6_script.md` is aligned with this spec.
- This spec is contract source of truth; if prompt conflicts, prompt must be updated in the same change set.
- Spec-readiness clarification: this document remains implementation-ready even if current prompt text is stale; Stage 6 merge-readiness requires prompt updates that satisfy the constraints below.
- Required prompt constraints:
  - `recommended_format in {"shorts", "reel"}` must enforce `word_count 120..170` and `estimated_seconds 45..70`
  - `recommended_format in {"tweet", "thread", "linkedin", "other"}` must enforce `word_count 180..260` and `estimated_seconds 70..110`
  - for all formats, `steps` must be exactly `3..6` non-empty bullet lines and each line must start with `- `
  - prompt must require the same JSON key set and section label/order defined in this spec

## Commands to Run (Expected)
- `python -m app.generate.cli --pipeline config/pipeline.local.yaml`
- `python -m app.generate.cli --pipeline config/pipeline.yaml` (shared/CI style; run with `OPENAI_API_KEY` set in environment)
- `pytest -q tests/test_stage_6_generate.py`

## Produced Artifacts
- `data/outputs/scripts_<YYYY-MM-DD>.jsonl`
- `data/outputs/stage_6_report_<YYYY-MM-DD>.json`
- SQLite table `scripts` in configured DB
