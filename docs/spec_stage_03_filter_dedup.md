# SPEC - STAGE 3: Pre-Filter + De-dup (Cheap Gates) - Manual Batch

## Depends on
- Stage 0
- Stage 2 (SQLite table `items` exists and is populated)

## Objective
Select a subset of CanonicalItems that are worth enriching in Stage 4 by applying cheap, deterministic filters:
- mechanical quality gates (required fields, minimum text length)
- cheap niche relevance scoring (keyword-based, config-driven)
- idempotent candidate registry in SQLite for pass items (do not reselect the same passed item on later runs)

No LLMs. No network calls.

## In Scope
- Read runtime defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - `stage_3_filter.*` (defined below)
- Read canonical items from SQLite table `items`.
- Select only items not already present in `candidates`.
- Compute `relevance_score` from deterministic keyword matching.
- Apply pass/fail gates in a fixed order.
- Persist pass items to SQLite table `candidates`.
- Emit pass items to JSONL for this run.
- Emit Stage 3 report JSON.
- Failed items are intentionally not persisted in `candidates` and may be re-evaluated on later runs.

## Out of Scope
- Any enrichment (article fetch, transcript retrieval, ASR)
- Any semantic/LLM scoring
- Any embeddings/vector DB
- Slack/email delivery
- Google Sheets writes

## Repo Layout (Must Follow)
Implement code under:
- `app/filter/models.py` (CandidateItem model + scoring helpers)
- `app/filter/state.py` (SQLite helpers for `candidates` table + deterministic selection query)
- `app/filter/runner.py` (select -> score -> filter -> persist -> output/report)
- `app/filter/cli.py` (CLI entrypoint)
- `app/filter/__init__.py`

## Required Pipeline Config (Stage 3)
Stage 3 behavior must be configurable via `config/pipeline.yaml` (no hardcoded thresholds/keyword lists).

Add:

```yaml
stage_3_filter:
  min_content_chars: 120
  min_relevance_score: 3
  max_candidates_default: 100
  keyword_groups:
    ai_automation:
      weight: 1
      terms:
        - ai
        - agent
        - agents
        - automation
        - automated
        - workflow
        - workflows
        - zapier
        - n8n
        - make.com
        - make
        - integromat
        - webhook
        - api
        - llm
        - gpt
        - openai
        - prompt
        - prompts
        - tool
        - tools
    monetization:
      weight: 2
      terms:
        - monetize
        - monetization
        - revenue
        - profit
        - income
        - side hustle
        - hustle
        - agency
        - client
        - clients
        - lead
        - leads
        - outreach
        - funnel
        - funnels
        - sales
        - offer
        - offers
        - retainer
        - pricing
        - cold email
        - cold outreach
        - freelanc
        - consulting
```

Validation rules:
- `min_content_chars`: integer >= 0
- `min_relevance_score`: integer >= 0
- `max_candidates_default`: integer >= 0
- `keyword_groups`: non-empty mapping
- For each group:
  - `weight`: integer >= 1
  - `terms`: non-empty list of strings, with at least one term that is non-empty after `strip()`
  - normalized term preprocessing order is normative and reused for both validation and matching:
    - `strip()` -> lowercase -> collapse internal whitespace runs to single spaces.
  - wildcard grammar for normalized terms:
    - `*` is allowed only as one trailing character.
    - terms containing `*` must not contain spaces.
    - the base token before trailing `*` must match token regex `[a-z0-9]+(?:\.[a-z0-9]+)*`.
    - any other `*` placement (leading/middle/repeated) is invalid config.
  - terms without `*` are valid only if one of:
    - token term: contains no spaces and matches token regex `[a-z0-9]+(?:\.[a-z0-9]+)*`
    - phrase term: contains at least one space and each space-separated token matches token regex
  - any non-`*` term outside the above grammar (example `ai-tools`) is invalid config.
  - backward compatibility rule: normalized term `freelanc` is canonicalized to `freelanc*`.
    - if both `freelanc` and `freelanc*` are present, they represent one canonical term (`freelanc*`) for scoring/output.

If invalid config is detected, fail run with exit code 2.

Implementation note:
- Update `docs/config_schemas.md` with these keys as part of Stage 3 implementation.

## CLI Contract
Primary command:
- `python -m app.filter.cli --pipeline config/pipeline.yaml --out data/outputs/candidate_items_2026-03-03.jsonl`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--report <path>` overrides default report path
- `--max-candidates <int>` overrides `stage_3_filter.max_candidates_default`
- `--log-level <LEVEL>` default INFO

Override validation:
- `--max-candidates` must be an integer >= 0; otherwise fatal error (exit code 2).

Defaults:
- DB: `paths.sqlite_db`
- out: `{paths.outputs_dir}/candidate_items_<YYYY-MM-DD>.jsonl` if `--out` omitted
- report: `{paths.outputs_dir}/stage_3_report_<YYYY-MM-DD>.json` if `--report` omitted
- max-candidates: `stage_3_filter.max_candidates_default`

Date basis:
- Output/report default filenames use current UTC run date.

Behavior:
- Overwrite `--out` each run.
- Process selected items in deterministic order (defined below).
- Stop processing once `passed_count == max_candidates`.
- Exit code:
  - 0 = completed
  - 2 = fatal error (invalid config, cannot open DB, cannot write outputs)

## Input Contract (SQLite `items` table)
Stage 3 reads from `items` table created in Stage 2.

Columns used:
- `item_id` (TEXT PK)
- `source_type`
- `source_id`
- `source_name`
- `creator`
- `title`
- `url`
- `published_at`
- `fetched_at`
- `summary`
- `content_text`

Assumption guaranteed by Stage 2:
- `published_at` and `fetched_at` are UTC `YYYY-MM-DDTHH:MM:SSZ` strings.
- `source_type` is one of `newsletter|youtube`.
- `item_id`, `source_id`, `source_name`, `creator`, `title`, `url`, `summary`, `content_text` are strings.

Trust boundary (normative):
- Stage 3 does not re-parse timestamps or re-validate `source_type` enum.
- Stage 3 relies on Stage 2 guarantees above and only applies Gate 1 string/emptiness checks.

## Deterministic Selection Query
Create `candidates` table first (if needed), then select unprocessed items:

```sql
SELECT
  i.item_id,
  i.source_type,
  i.source_id,
  i.source_name,
  i.creator,
  i.title,
  i.url,
  i.published_at,
  i.fetched_at,
  i.summary,
  i.content_text
FROM items i
LEFT JOIN candidates c ON c.item_id = i.item_id
WHERE c.item_id IS NULL
ORDER BY i.published_at DESC, i.item_id ASC;
```

`items_available_total` is the total row count returned by this query before gate evaluation.

## CandidateItem Contract (JSONL output)
Each emitted line is a JSON object for a pass item only.

Required fields:
- `item_id` (string)
- `source_type` ("newsletter" | "youtube")
- `source_id` (string)
- `source_name` (string)
- `creator` (string)
- `title` (string)
- `url` (string)
- `published_at` (ISO8601 UTC Z)
- `fetched_at` (ISO8601 UTC Z)
- `content_text` (string)
- `relevance_score` (integer)
- `matched_keywords` (list[string], lowercased, unique, sorted ascending)
- `scored_at` (ISO8601 UTC Z)

Output ordering:
- Candidate JSONL row order must match deterministic processing order.

## Cheap Gates (Deterministic)
Apply gates in this exact order; stop at first failing gate per item.

### Gate 1: Required fields present
Fail if any required field is missing, not a string, or empty after `strip()`:
- `item_id`, `source_type`, `source_id`, `source_name`, `creator`, `title`, `url`, `published_at`, `fetched_at`

Fail reason: `missing_required_fields`

Normalization rule for required fields (normative):
- For fields listed above, use the trimmed value (`value.strip()`) for all subsequent gates and for emitted/persisted values.

Input robustness rule for body fields (normative):
- If `summary` or `content_text` is non-string or null, treat it as `""` before Gate 2 logic.

### Gate 2: Minimum content length
Define `body_text`:
- `content_text` if non-empty after `strip()`, else `summary` if non-empty after `strip()`, else `""`
- `body_text` MUST be the stripped value of whichever field is selected above:
  - if selecting `content_text`, use `content_text.strip()`
  - if selecting `summary`, use `summary.strip()`
  - else `""`

Fail if `len(body_text) < min_content_chars`.

Fail reason: `content_too_short`

`body_text` persistence rule (normative):
- For pass items, emitted/persisted `content_text` MUST equal `body_text` used for gating/scoring in this stage.
- Rationale: avoids scoring one text while persisting another and keeps downstream stages reproducible.

### Gate 3: Relevance scoring
Scoring text:
- `score_text = (title + " " + body_text).lower()`

Matching rules (normative):
- Normalize all config terms before matching with the same preprocessing used in config validation:
  - `strip()` -> lowercase -> collapse internal whitespace runs to single spaces.
- Ignore empty terms after normalization.
- If the same normalized term appears more than once in a group, treat it once.
- Derive `score_text_ws` by collapsing all whitespace in `score_text` to single spaces.
- Tokenization for token/prefix matching is normative:
  - extract tokens from `score_text` with regex `[a-z0-9]+(?:\.[a-z0-9]+)*`.
- Terms containing a space (for example `side hustle`) use case-insensitive substring match against `score_text_ws`.
- Terms without spaces use exact token match against extracted tokens.
- Prefix terms ending in `*` are treated as token prefix match. Stage 3 uses this for `freelanc*` semantics.
  - Config value `freelanc` must be interpreted as prefix match `freelanc*` for backward compatibility.
  - Canonical matched-term key for output/scoring in this case is `freelanc*`.
- Each matched term contributes at most once, even if it appears multiple times.
- If a term appears in multiple groups, score it once using the highest group weight.
  - For this case, `matched_keywords` MUST include the term once (using the canonical matched-term key), and score uses the highest group weight.

Scoring:
- `relevance_score = sum(weight(term) for each unique matched term)`

Pass rule:
- pass if `relevance_score >= min_relevance_score`

Fail reason: `low_relevance_score`

### Gate 4: Max candidates per run
- Continue evaluating items in order until `passed_count == max_candidates`.
- If `max_candidates == 0`, no items are evaluated; output JSONL is empty.

## SQLite Persistence (`candidates` table)
Create table if not exists:

Table: `candidates`
- `item_id` TEXT PRIMARY KEY
- `relevance_score` INTEGER NOT NULL
- `matched_keywords` TEXT NOT NULL (JSON string list)
- `source_type` TEXT NOT NULL
- `source_id` TEXT NOT NULL
- `source_name` TEXT NOT NULL
- `creator` TEXT NOT NULL
- `title` TEXT NOT NULL
- `url` TEXT NOT NULL
- `published_at` TEXT NOT NULL
- `fetched_at` TEXT NOT NULL
- `content_text` TEXT NOT NULL
- `created_at` TEXT NOT NULL (UTC ISO8601 Z)

Insert rule:
- For each pass item, insert into `candidates` and emit to JSONL in the same iteration.
- Use `INSERT OR IGNORE` for safety; if ignored due to concurrent race, do not emit to JSONL and increment `candidates_skipped_already_present`.
- Timestamp binding rule (normative):
  - For each pass item, compute one UTC timestamp string at second precision `YYYY-MM-DDTHH:MM:SSZ`.
  - Use that same value for JSONL `scored_at` and DB `created_at`.

Idempotency guarantee (single-process sequential runs):
- A second run over unchanged `items` inserts zero new rows and emits zero candidate JSONL rows.
- Scope clarification: idempotency applies to pass items (rows inserted into `candidates`). Items that fail gates are not marked as processed and may be re-evaluated in later runs.

## Logging
- INFO: run start/end + totals
- WARNING: aggregated fail counts by reason
- ERROR: fatal DB/I/O/config errors

## Stage 3 Report (JSON)
Write report to default `{outputs_dir}/stage_3_report_<YYYY-MM-DD>.json` unless overridden.

Must include:
- `run_id`, `started_at`, `finished_at`
  - `run_id` must be UUID4 string.
  - `started_at` and `finished_at` must be UTC second-precision `YYYY-MM-DDTHH:MM:SSZ`.
- `db_path`, `output_path`, `report_path`
- `items_available_total` (count of unprocessed items from selection query)
- `items_considered` (count of items actually gate-evaluated this run)
- `passed_count` (count passed gates before insert attempt)
- `failed_count` (count failed gates)
- `inserted_db` (new rows inserted into `candidates`)
- `candidate_items_emitted` (rows written to JSONL)
- `candidates_skipped_already_present` (insert-ignore conflicts; expected 0 in normal sequential runs)
- `max_candidates`
- `reached_max_candidates` (boolean)
- `fail_breakdown` map with keys:
  - `missing_required_fields`
  - `content_too_short`
  - `low_relevance_score`

Counter invariants:
- `items_considered = passed_count + failed_count`
- `inserted_db = candidate_items_emitted`
- `items_considered <= items_available_total`
- `candidate_items_emitted <= max_candidates`
- `failed_count = fail_breakdown.missing_required_fields + fail_breakdown.content_too_short + fail_breakdown.low_relevance_score`
- `passed_count = inserted_db + candidates_skipped_already_present`

`reached_max_candidates` definition (normative):
- `reached_max_candidates = (passed_count == max_candidates)`
- Therefore when `max_candidates == 0`, `reached_max_candidates` MUST be `true`.

## Outputs
- JSONL: `data/outputs/candidate_items_<YYYY-MM-DD>.jsonl` (pass items emitted this run)
- JSON: `data/outputs/stage_3_report_<YYYY-MM-DD>.json`
- SQLite: `candidates` table created/updated in configured DB

## Tests (pytest) - Minimum
Add `tests/test_stage_3_filter.py` with temp SQLite DB and seeded `items` rows:

1) Pass/fail behavior + fail reason precedence
- missing required field fails with `missing_required_fields`
- short content fails with `content_too_short`
- sufficient content + score >= threshold passes

2) Scoring correctness
- unique term counting
- phrase matching (`side hustle`, `cold email`)
- prefix matching (`freelanc*` semantics)
- no duplicate counting for repeated same term
- same term across multiple groups uses highest weight once
- `matched_keywords` canonical output (lowercased, unique, sorted; includes canonical prefix key like `freelanc*`)

3) Deterministic ordering + cap
- same seed data always emits same ordered top-N by `published_at DESC, item_id ASC`

4) Idempotency
- run twice: first inserts/emits expected rows; second inserts/emits zero

5) Report invariant checks
- verify all counter invariants and fail_breakdown totals

6) Max-candidates edge case
- with `max_candidates == 0`: no items evaluated, no rows emitted/inserted, `reached_max_candidates == true`

7) Invalid config handling
- invalid `stage_3_filter` shape/values exits with code 2 (for example missing keyword_groups, negative thresholds, empty terms)

8) Input robustness for body fields
- non-string/null `summary` and `content_text` are treated as `""` before Gate 2
- Gate 2 then evaluates length on the defined `body_text` fallback logic

9) Persisted/Emitted body text consistency
- for pass items, emitted JSONL `content_text` and DB `candidates.content_text` equal the exact `body_text` used in Gate 2 and Gate 3

Tests must not use network calls.

---

## Files Changed (Expected)
- `app/filter/*` (new)
- `tests/test_stage_3_filter.py` (new)
- `config/pipeline.yaml` (Stage 3 keys)
- `docs/config_schemas.md` (Stage 3 schema)

## Commands to Run (Expected)
- `python -m app.filter.cli --pipeline config/pipeline.yaml --out data/outputs/candidate_items_<YYYY-MM-DD>.jsonl`
- `pytest -q tests/test_stage_3_filter.py`

## Produced Artifacts
- `data/outputs/candidate_items_<YYYY-MM-DD>.jsonl`
- `data/outputs/stage_3_report_<YYYY-MM-DD>.json`
- `{paths.sqlite_db}` with table `candidates` created/updated
