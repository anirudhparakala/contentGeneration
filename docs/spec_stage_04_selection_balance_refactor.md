# SPEC - STAGE 4 REFACTOR: Balanced Candidate Selection + Source Diversity + Retry Cooldown

## Depends on
- Stage 0
- Stage 3 (`candidates` table populated with at least: `item_id`, `source_type`, `source_id`, `url`, `title`, `published_at`, `relevance_score`)
- Existing Stage 4 baseline:
  - `docs/spec_stage_04_enrich.md`
  - `app/enrich/runner.py`
  - `app/enrich/state.py`
  - `app/enrich/models.py`

## Objective
Keep Stage 4 enrichment logic unchanged while redesigning candidate selection so that:
- selection is item-aware and source-aware (not source-count confusion)
- newsletters are guaranteed representation when eligible newsletters exist
- both source types can be represented in the same run
- one/few heavy sources do not dominate the run
- repeatedly failing items do not block future runs

Primary business goals:
- avoid `49 youtube / 1 newsletter` style Stage 4 batches when eligible newsletters exist
- preserve deterministic, idempotent enrichment behavior

Non-goals:
- no change to newsletter extraction method
- no change to transcript/ASR enrichment logic
- no change to Stage 3 scoring algorithm

## Problem Statement (Current Behavior)
Current Stage 4 selection query is:
- unenriched candidates ordered by `relevance_score DESC, published_at DESC, item_id ASC`
- then `LIMIT max_items` (default `50`)

Consequences:
- `max_items` is item count, not source count (so `50` items is valid even with 24 configured sources)
- high-score YouTube rows dominate top-`N`
- newsletters admitted by Stage 3 can still be nearly absent in Stage 4
- failed candidates are retried repeatedly because there is no cooldown state

## Scope
### In Scope
- Stage 4 selection refactor in `app/enrich/state.py` and `app/enrich/runner.py`
- Stage 4 config extension in `config/pipeline.yaml`
- Stage 4 report schema extension for mix/diversity/cooldown observability
- Stage 4 retry-state table for cooldown filtering
- Stage 4 tests in `tests/test_stage_4_enrich.py`
- Stage 4 docs sync:
  - `docs/spec_stage_04_enrich.md`
  - `docs/config_schemas.md`

### Out of Scope
- Stage 1 ingest source discovery behavior
- Stage 3 candidate scoring internals
- Stage 5+ downstream processing

## Required Config Changes (`config/pipeline.yaml`)
Add a new required mapping:

```yaml
stage_4_enrich:
  max_items_default: 50
  selection_policy:
    min_newsletters_per_run: 10
    min_youtube_per_run: 10
    max_items_per_source: 2
    source_diversity_first_pass: true
  cooldown_policy:
    enabled: true
    after_consecutive_failures: 1
    skip_for_hours: 24
    reasons:
      - newsletter_fetch_failed
      - newsletter_extract_failed
      - newsletter_text_too_short
      - youtube_video_id_parse_failed
      - youtube_transcript_unavailable
      - youtube_transcript_failed
      - youtube_asr_failed
      - youtube_text_too_short
```

### Validation Rules
All existing Stage 4 config validation remains.

Additional required rules:
- `stage_4_enrich`: required mapping
- `stage_4_enrich.max_items_default`: non-boolean integer `>= 0`
- `selection_policy`: required mapping
- `selection_policy.min_newsletters_per_run`: non-boolean integer `>= 0`
- `selection_policy.min_youtube_per_run`: non-boolean integer `>= 0`
- `selection_policy.max_items_per_source`: non-boolean integer `>= 1`
- `selection_policy.source_diversity_first_pass`: boolean
- `selection_policy.min_newsletters_per_run + selection_policy.min_youtube_per_run <= stage_4_enrich.max_items_default`
- `selection_policy.max_items_per_source <= stage_4_enrich.max_items_default` when `max_items_default > 0`
- `cooldown_policy`: required mapping
- `cooldown_policy.enabled`: boolean
- `cooldown_policy.after_consecutive_failures`: non-boolean integer `>= 1`
- `cooldown_policy.skip_for_hours`: non-boolean integer `>= 0`
- `cooldown_policy.reasons`: non-empty list of strings
- each `cooldown_policy.reasons` value must be one of Stage 4 fail reasons:
  - `newsletter_fetch_failed`
  - `newsletter_extract_failed`
  - `newsletter_text_too_short`
  - `youtube_video_id_parse_failed`
  - `youtube_transcript_unavailable`
  - `youtube_transcript_failed`
  - `youtube_asr_failed`
  - `youtube_text_too_short`
  - `transcript_cap_reached`
  - `asr_cap_reached`
- canonical reason uniqueness rule: values are unique after `strip()` (case-sensitive exact enum after strip)
- `invalid_candidate_row` is not allowed in `cooldown_policy.reasons` (rows with invalid canonical identity are never cooldown-tracked)
- policy note: including `transcript_cap_reached` and/or `asr_cap_reached` in cooldown reasons is allowed but discouraged, because these are run-capacity outcomes, not content-quality failures

Runtime override validation:
- `max_items` effective value comes from:
  - CLI `--max-items` when provided
  - else `stage_4_enrich.max_items_default`
- no runtime fatal check is performed for `effective max_items` versus floor sums; floor targets are clamped by Phase B definitions and the run must proceed deterministically

Failure behavior:
- any validation failure is fatal (exit code `2`)

## Data Contract Changes
### Candidate Input Contract Extension
Stage 4 now requires `source_id` in candidate selection and validation pre-checks.

Required columns in `candidates` table for Stage 4:
- `item_id`
- `source_type`
- `source_id`
- `url`
- `title`
- `published_at`
- `relevance_score`

Selection identity canonicalization (normative):
- `source_id` must be a string and non-empty after `strip()`
- canonical per-source identity key is `(source_type.strip().lower(), source_id.strip().lower())`
- rows that fail canonical selection identity validation are `invalid_pool` rows:
  - they are excluded from floor and diversity phases
  - they may still be selected in remainder phase and must map to `invalid_candidate_row`
  - they must never be cooldown-blocked, and cooldown state must never be written for them

### New Stage 4 Retry-State Table
Create table if not exists:

Table: `enrich_retry_state`
- `item_id` TEXT PRIMARY KEY
- `source_type` TEXT NOT NULL
- `source_id` TEXT NOT NULL
- `attempts_total` INTEGER NOT NULL
- `consecutive_failures` INTEGER NOT NULL
- `last_outcome` TEXT NOT NULL (`success` | `failed`)
- `last_fail_reason` TEXT NULL
- `last_attempt_at` TEXT NOT NULL (UTC ISO8601 Z)
- `next_eligible_at` TEXT NULL (UTC ISO8601 Z)
- `updated_at` TEXT NOT NULL (UTC ISO8601 Z)

Update rules:
- retry-state upsert eligibility:
  - write retry-state only for rows with canonical identity available:
    - canonical `item_id` (non-empty string after `strip()`)
    - canonical `source_type` in `{newsletter, youtube}`
    - canonical `source_id = source_id.strip().lower()` (non-empty)
  - rows that fail candidate parsing/identity validation (`invalid_candidate_row`) do not write retry-state
- upsert initialization (when `item_id` row does not already exist):
  - start from:
    - `attempts_total = 0`
    - `consecutive_failures = 0`
    - `last_outcome = "success"` (placeholder overwritten by terminal outcome update below)
    - `last_fail_reason = null`
    - `next_eligible_at = null`
    - `last_attempt_at = now`
    - `updated_at = now`
- upsert identity write rules:
  - persist canonical `source_type` and canonical `source_id` on every upsert
  - `item_id` remains PK identity and is canonicalized via `strip()`
- on terminal per-item success:
  - `attempts_total += 1`
  - `consecutive_failures = 0`
  - `last_outcome = "success"`
  - `last_fail_reason = null`
  - `last_attempt_at = now`
  - `updated_at = now`
  - `next_eligible_at = null`
- on terminal per-item failure:
  - `attempts_total += 1`
  - `consecutive_failures += 1`
  - `last_outcome = "failed"`
  - `last_fail_reason = failure_reason`
  - `last_attempt_at = now`
  - `updated_at = now`
  - if `cooldown_policy.enabled == false`:
    - `next_eligible_at = null`
  - else if failure reason in configured cooldown reasons AND
    `consecutive_failures >= after_consecutive_failures` AND `skip_for_hours > 0`:
    - `next_eligible_at = now + skip_for_hours`
  - else:
    - `next_eligible_at = null`

Eligibility rule before selection:
- cooldown filtering is enabled only when `cooldown_policy.enabled == true`
- if `cooldown_policy.enabled == false`, all valid-pool rows are treated as eligible for this run regardless of stored `next_eligible_at`
- item is cooldown-blocked iff:
  - retry-state row exists AND
  - `next_eligible_at` is non-null AND
  - `started_at < next_eligible_at`
- cooldown filtering applies only to valid-pool rows (invalid-pool rows are never cooldown-filtered)
- with cooldown disabled, `cooldown_blocked_total = 0`
- timestamp parse safety rule:
  - if retry-state `next_eligible_at` is present but not parseable as UTC ISO8601 Z, treat the row as eligible (not blocked) for this run and continue
  - malformed `next_eligible_at` must not fail the run

## Selection Algorithm Changes (Normative)
Replace current SQL `LIMIT max_items` top-N selection with deterministic multi-phase selection.

### Cap-0 rule
- if effective `max_items == 0`:
  - select/process zero items
  - `selected_rows_total = 0`
  - `items_selected = 0`
- cap-0 is never a validation error due to floor settings; Phase B floor targets clamp to `0` under `max_items == 0`

### Phase A: Build eligible pool
1. Query all unenriched candidates in deterministic base order:
   - `relevance_score DESC, published_at DESC, item_id ASC`
2. Partition queried rows deterministically into:
   - `valid_pool`: rows that pass candidate parsing/selection validation, including canonical `source_type` and canonical `source_id`
   - `invalid_pool`: rows that fail candidate parsing/selection validation
3. From `valid_pool`, exclude cooldown-blocked rows.
4. Remaining ordered valid rows are `eligible_pool`.
5. Build per-source ordered queues from `eligible_pool` using canonical source key:
   - `(source_type.strip().lower(), source_id.strip().lower())`

Definitions:
- `candidates_available_total = count(unenriched rows queried in step 1)`
- `invalid_pool_total = len(invalid_pool)`
- `eligible_pool_total = len(eligible_pool)`
- `eligible_newsletters_total = count(source_type == "newsletter")`
- `eligible_youtube_total = count(source_type == "youtube")`
- `cooldown_blocked_total = len(valid_pool) - eligible_pool_total`
- invariant: `cooldown_blocked_total + eligible_pool_total + invalid_pool_total = candidates_available_total`

### Phase B: Enforce source-type floors
Let:
- `newsletter_floor_target = min(min_newsletters_per_run, max_items)`
- `youtube_floor_target = min(min_youtube_per_run, max_items - newsletter_floor_target)`

Selection procedure (deterministic):
1. Select newsletters up to `newsletter_floor_target` from newsletter subset in base order,
   honoring `max_items_per_source`.
2. Select YouTube up to `youtube_floor_target` from YouTube subset in base order,
   honoring `max_items_per_source`.
3. No item may be selected twice.

Quota semantics:
- Floors are minimum targets, not hard caps.
- If a floor cannot be met due to insufficient eligible items or per-source cap, select all possible and continue.

### Phase C: Source-diversity first pass (optional)
If `source_diversity_first_pass == true` and capacity remains:
- consider sources with `selected_count_for_source == 0`
- for each such source, candidate is that source queue head
- choose candidates by head-row base order (same deterministic comparator), one per source
- continue until:
  - capacity exhausted OR
  - no unseen source has remaining eligible row

This phase ensures that when `max_items >= active_eligible_sources`, the run can include at least one item per eligible source.

### Phase D: Fill remainder by score
If capacity remains:
- iterate deterministic global remainder stream in base order containing:
  - remaining `eligible_pool` rows not yet selected
  - `invalid_pool` rows
- for valid `eligible_pool` rows, enforce `max_items_per_source`
- for `invalid_pool` rows, do not apply per-source cap
- stop at `max_items`
- `invalid_pool` rows selected in this phase must map to `invalid_candidate_row` terminal outcomes during processing

### Output order contract
- processing/emission order must match final selected sequence:
  1) floor-selected newsletters
  2) floor-selected YouTube
  3) source-diversity picks
  4) remainder fill (valid and/or invalid rows)
- deterministic tie-breakers must preserve repeatable order with identical DB + config state

## Per-item Processing and Cooldown State
Existing enrichment logic and fail-reason mapping remain unchanged.

Additional requirement:
- every terminal per-item outcome (success or exactly one failure reason) must upsert retry-state for that `item_id` using update rules above.
- exception: `invalid_candidate_row` outcomes must not write retry-state (identity may be incomplete/non-canonical).

## Report Schema Changes (`stage_4_report_*.json`)
Keep existing fields and add:
- `selected_rows_total` (int)
- `invalid_pool_total` (int)
- `eligible_pool_total` (int)
- `eligible_newsletters_total` (int)
- `eligible_youtube_total` (int)
- `cooldown_blocked_total` (int)
- `selected_newsletter_count` (int)
- `selected_youtube_count` (int)
- `selected_invalid_count` (int)
- `selected_unique_sources` (int)
- `newsletter_floor_target` (int)
- `youtube_floor_target` (int)
- `newsletter_floor_met` (bool)
- `youtube_floor_met` (bool)
- `source_diversity_first_pass_applied` (bool)
- `selected_phase_breakdown` map with keys:
  - `floor_newsletter`
  - `floor_youtube`
  - `source_diversity`
  - `remainder`

### Definitions (normative)
- `selected_rows_total = len(final_selected_rows)` before per-item enrichment attempts
- `items_selected` keeps existing meaning (terminal outcomes reached)
- `selected_newsletter_count` and `selected_youtube_count` are counts over valid selected rows by canonical `source_type`
- `selected_invalid_count` is count of selected rows that fail candidate parsing/selection validation and map to `invalid_candidate_row`
- `selected_unique_sources = count(distinct canonical (source_type, source_id) across valid selected rows only)`
- `newsletter_floor_met = (selected_newsletter_count >= newsletter_floor_target)`
- `youtube_floor_met = (selected_youtube_count >= youtube_floor_target)`
- `selected_phase_breakdown` counts rows assigned in selection phases; keys always present with integers `>= 0`
- `source_diversity_first_pass_applied = (selection_policy.source_diversity_first_pass == true AND selected_phase_breakdown.source_diversity > 0)`

### New invariants
- `selected_rows_total <= max_items`
- `selected_rows_total <= eligible_pool_total + invalid_pool_total`
- `selected_newsletter_count + selected_youtube_count + selected_invalid_count = selected_rows_total`
- `selected_phase_breakdown.floor_newsletter + selected_phase_breakdown.floor_youtube + selected_phase_breakdown.source_diversity + selected_phase_breakdown.remainder = selected_rows_total`
- `eligible_newsletters_total + eligible_youtube_total = eligible_pool_total`
- `selected_invalid_count <= invalid_pool_total`
- `cooldown_blocked_total + eligible_pool_total + invalid_pool_total = candidates_available_total`
- Existing Stage 4 invariants remain in force.

### Fatal-report requirements for new fields
For fatal runs, all newly added fields in this spec are still required in the report payload.

Defaulting rules when values are unavailable due to early fatal stop:
- integer fields default to `0`:
  - `selected_rows_total`
  - `invalid_pool_total`
  - `eligible_pool_total`
  - `eligible_newsletters_total`
  - `eligible_youtube_total`
  - `cooldown_blocked_total`
  - `selected_newsletter_count`
  - `selected_youtube_count`
  - `selected_invalid_count`
  - `selected_unique_sources`
  - `newsletter_floor_target`
  - `youtube_floor_target`
- boolean fields default to `false`:
  - `newsletter_floor_met`
  - `youtube_floor_met`
  - `source_diversity_first_pass_applied`
- `selected_phase_breakdown` must always be present as:
  - `floor_newsletter: 0`
  - `floor_youtube: 0`
  - `source_diversity: 0`
  - `remainder: 0`

## CLI Contract Changes
- `--max-items` override behavior remains, but default now uses:
  - `stage_4_enrich.max_items_default` (instead of hardcoded 50)
- No additional CLI flags required.

## Determinism and Idempotency
- Determinism: identical DB state + identical config => identical final selected item sequence.
- Idempotency: unchanged for `enriched_items` inserts (`INSERT OR IGNORE`).
- Cooldown adds deterministic eligibility filtering based on persisted UTC timestamps.

## Test Plan (Required)
Extend `tests/test_stage_4_enrich.py` with at minimum:

1. Item-count vs source-count clarity
- seed multiple items per source
- assert `max_items` limits items, not sources

2. Newsletter floor guarantee
- create eligible pool where score-only ordering would starve newsletters
- assert `selected_newsletter_count >= newsletter_floor_target` when enough eligible newsletters exist

3. YouTube floor guarantee
- symmetric check for `min_youtube_per_run`

4. Floor shortfall behavior
- insufficient eligible newsletters and/or YouTube
- assert floor-met booleans are false and all available eligible rows are used

5. Per-source cap enforcement
- heavy single source cannot exceed `max_items_per_source`

6. Source-diversity first pass
- with capacity >= eligible source count, assert one-per-source coverage
- with capacity < eligible source count, assert deterministic source choice

7. Cooldown block behavior
- failed item with configured reason is skipped until `next_eligible_at`
- report `cooldown_blocked_total` increments

8. Cooldown expiry
- once simulated time passes `next_eligible_at`, item becomes eligible again

9. Cooldown updates on success/failure
- success resets `consecutive_failures` and clears `next_eligible_at`
- failure updates fields deterministically

10. Selection determinism
- repeated runs with same seeded state produce identical selected sequence and phase counts

11. Report invariants
- assert all new fields and invariants plus existing invariants
- include fatal-report path assertions for new required counters/booleans/maps

12. Config validation failures
- invalid floors, invalid max-items-per-source, invalid cooldown reasons, and
  invalid `stage_4_enrich` structure must fail with exit code `2`
- include explicit failure when `invalid_candidate_row` is present in `cooldown_policy.reasons`

13. Invalid-pool behavior
- seed malformed candidates and valid candidates together
- assert malformed candidates are excluded from floor/diversity phases
- assert malformed candidates can appear only in remainder and map to `invalid_candidate_row`
- assert malformed candidates do not write/update `enrich_retry_state`

14. Cross-type source-id collision behavior
- seed newsletter and YouTube rows sharing the same raw `source_id`
- assert per-source cap and diversity treat `(source_type, source_id)` as distinct source keys
- assert `selected_unique_sources` counts these as two distinct valid sources

15. Malformed cooldown timestamp tolerance
- seed retry-state row with non-parseable `next_eligible_at`
- assert candidate is treated as eligible (not cooldown-blocked)
- assert run is not fatal and processing continues deterministically

16. Cooldown disabled semantics
- set `cooldown_policy.enabled = false` with retry-state rows containing future `next_eligible_at`
- assert those rows are not cooldown-blocked (`cooldown_blocked_total = 0`)
- assert failure updates keep `next_eligible_at = null` while disabled

## Implementation File Targets
- `app/enrich/state.py`
- `app/enrich/runner.py`
- `app/enrich/models.py` (only if selection/cooldown model helpers are needed)
- `app/enrich/cli.py` (only for max-items default source)
- `tests/test_stage_4_enrich.py`
- `config/pipeline.yaml` (add `stage_4_enrich`)
- `docs/config_schemas.md`
- `docs/spec_stage_04_enrich.md` (sync base Stage 4 spec to new semantics)

## Acceptance Criteria
- Stage 4 selection is item-aware, source-aware, and deterministic.
- Effective `max_items` is configurable via `stage_4_enrich.max_items_default`.
- Stage 4 guarantees newsletter and YouTube floor representation when eligible pools permit.
- Per-source cap prevents single-source domination.
- Optional source-diversity first pass provides one-per-source coverage when capacity allows.
- Cooldown suppresses repeated failing candidates and is observable in reports.
- `cooldown_policy.enabled = false` bypasses cooldown blocking/scheduling deterministically.
- Invalid candidate rows are isolated from floor/diversity selection and cooldown state writes.
- Existing enrichment method behavior and failure mapping remain intact.

---

## Files Changed (Spec Authoring)
- `docs/spec_stage_04_selection_balance_refactor.md`

## Commands to Run (Spec Authoring)
- Not run (spec-only authoring task)

## Produced Artifacts
- None (no runtime pipeline execution in spec-only task)
