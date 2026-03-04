# SPEC — STAGE 5: Intelligence (Extract + Score) — Manual Batch

## Depends on
- Stage 0
- Stage 4 (SQLite table `enriched_items` exists and is populated)

## Objective
Convert enriched content into structured "idea intelligence" using an LLM:
- Extract: topic, core claim, workflow steps, tools, monetization angle, metrics/claims, assumptions
- Score: viral/impact rating (1–10) and rationale
- Produce 2–3 hook candidates (short)
Persist results to SQLite `ideas` table and emit JSONL for this run.

Stage 5 does NOT generate full scripts. That is Stage 6.

## In Scope
- Read defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - new: `llm.provider`, `llm.model`, `llm.max_output_tokens`, `llm.temperature`
- Read inputs from SQLite `enriched_items` table (success rows only).
- Select items not already present in `ideas` (idempotent).
- Run LLM extraction and scoring using prompt templates from `config/prompts/`:
  - `config/prompts/stage_5_extract.md`
  - `config/prompts/stage_5_score.md`
- Persist to SQLite `ideas` table.
- Output:
  - JSONL of ideas processed this run
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
- `app/intelligence/llm.py`        (OpenAI client wrapper, retries)
- `app/intelligence/state.py`      (SQLite helpers for ideas table)
- `app/intelligence/runner.py`
- `app/intelligence/cli.py`
- `app/intelligence/__init__.py`

## Config Additions (config/pipeline.yaml)
Add section (values can be changed later):
llm:
  provider: "openai"
  model: "gpt-4o-mini"
  temperature: 0.2
  max_output_tokens: 900
  requests_per_minute_soft: 30

Secrets:
- Use environment variable `OPENAI_API_KEY`.

## CLI Contract
Primary command:
- `python -m app.intelligence.cli --pipeline config/pipeline.yaml --out data/outputs/ideas_<YYYY-MM-DD>.jsonl --report data/outputs/stage_5_report_<YYYY-MM-DD>.json`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--max-items <int>` cap items processed (default 25)
- `--model <name>` overrides `llm.model`
- `--log-level <LEVEL>` default INFO

Behavior:
- Overwrite `--out` each run.
- Only process enriched items not already in `ideas`.
- Continue on per-item LLM errors; do not fail entire run.
- Exit code:
  - 0 = completed
  - 2 = fatal error (cannot read pipeline, cannot open DB, cannot write outputs)

## Input Contract (SQLite enriched_items table)
Stage 5 reads:
- `enriched_items.item_id`
- `enriched_items.source_type`
- `enriched_items.url`
- `enriched_items.title`
- `enriched_items.published_at`
- `enriched_items.enriched_text`
- `enriched_items.enrichment_method`
- `enriched_items.evidence_snippets` (JSON)

Selection rule:
- item_id NOT IN ideas.item_id
- order by published_at desc
- apply `--max-items`

## Prompting Contract
Stage 5 runs TWO LLM calls per item (keeps it simple and debuggable):

### Call 1: Extract (structured JSON)
Prompt file: `config/prompts/stage_5_extract.md`

Inputs:
- title
- source_type
- url
- enriched_text (truncate to first N chars if too long, default N=12000)
Output MUST be valid JSON matching ExtractSchema.

ExtractSchema:
{
  "topic": "string",
  "core_claim": "string",
  "workflow_steps": ["string", "..."],          # 3–8 steps
  "tools_mentioned": ["string", "..."],         # normalized names
  "monetization_angle": "string",               # how money is made
  "metrics_claims": ["string", "..."],          # revenue/time/conversion claims verbatim if present
  "assumptions": ["string", "..."],             # what must be true
  "content_type": "howto|case_study|tool_review|opinion|news|other"
}

Validation:
- If JSON invalid or missing required fields, mark item failed and continue.

### Call 2: Score + hooks (structured JSON)
Prompt file: `config/prompts/stage_5_score.md`

Inputs:
- title
- extracted fields from Call 1
- short evidence snippets (from enriched_items.evidence_snippets)
Output MUST be valid JSON matching ScoreSchema.

ScoreSchema:
{
  "viral_rating": 1-10,                         # integer
  "rating_rationale": "string",
  "hooks": ["string", "string", "string"],      # 3 hooks, <= 140 chars each
  "platform": "youtube|newsletter",             # derived from source_type
  "recommended_format": "shorts|tweet|linkedin|reel|thread|other"
}

Rating rubric (must be encoded in prompt):
- Specificity and operational detail
- Novelty and contrarian insight
- Proof signals (screenshots, real numbers) versus vague hype
- Replicability (clear steps)
- Fit for the niche "AI automations to make money"

## SQLite Persistence (ideas table)
Create table `ideas`:

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
- `hooks` TEXT NOT NULL                     # JSON string list
- `recommended_format` TEXT NOT NULL
- `llm_model` TEXT NOT NULL
- `created_at` TEXT NOT NULL                # UTC ISO8601 Z

Idempotency:
- if item_id exists, skip and count `skipped_already_present`.

## Outputs
- JSONL: `data/outputs/ideas_<YYYY-MM-DD>.jsonl` (successes only)
- JSON: `data/outputs/stage_5_report_<YYYY-MM-DD>.json`
- SQLite: `ideas` table updated

## Stage 5 Report (JSON)
Must include:
- run_id, started_at, finished_at
- db_path, output_path, report_path
- items_available_total (enriched_items rows)
- items_selected
- success_count
- failed_count
- inserted_db
- skipped_already_present
- fail_breakdown:
  - llm_extract_failed
  - llm_score_failed
  - invalid_json_output
  - validation_failed

## Tests (pytest) — Minimum
Add `tests/test_stage_5_intelligence.py`:
- Use mocked LLM responses (do not call network)
- Test JSON validation and DB insert
- Test idempotency behavior

## Files Changed (Expected)
- `app/intelligence/*` (new)
- `config/prompts/stage_5_extract.md` (new)
- `config/prompts/stage_5_score.md` (new)
- `tests/test_stage_5_intelligence.py` (new)
- `config/pipeline.yaml` (add llm section)

## Commands to Run (Expected)
- `set OPENAI_API_KEY=...` (Windows) or `export OPENAI_API_KEY=...`
- `python -m app.intelligence.cli --pipeline config/pipeline.yaml --out data/outputs/ideas_<YYYY-MM-DD>.jsonl --report data/outputs/stage_5_report_<YYYY-MM-DD>.json`
- `pytest -q`

## Produced Artifacts
- `data/outputs/ideas_<YYYY-MM-DD>.jsonl`
- `data/outputs/stage_5_report_<YYYY-MM-DD>.json`
- SQLite table `ideas` in your configured db