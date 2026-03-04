# SPEC - STAGE 3 REFACTOR: Newsletter Quota + Source-Aware Thresholds

## Depends on
- Stage 0
- Stage 2 (`items` table populated)
- Existing Stage 3 implementation/spec baseline:
  - `docs/spec_stage_03_filter_dedup.md`
  - `app/filter/runner.py`
  - `app/filter/models.py`
  - `app/filter/state.py`

## Objective
Increase newsletter representation in Stage 3 without changing Stage 4 by adding:
- guaranteed minimum newsletter quota per run
- newsletter-specific relevance threshold
- optional trusted-newsletter score bypass

Primary business goal:
- newsletters must be guaranteed representation in each run when eligible newsletter items exist
- YouTube must not fully dominate selected candidates

Non-goals:
- no change to Stage 4 balancing/ranking
- no change to Stage 3 keyword scoring algorithm
- no change to `min_content_chars` gate

## Problem Statement (Current Behavior)
Current Stage 3 applies one global relevance threshold (`stage_3_filter.min_relevance_score`, currently `3`) to all source types.

Observed in run artifacts:
- `data/outputs/candidate_items_2026-03-03.jsonl`: `newsletter=18`, `youtube=82`
- `data/outputs/stage_3_report_2026-03-03.json`: `low_relevance_score=80`

This demonstrates newsletter under-selection relative to available newsletter inputs.

## Scope
### In Scope
- Stage 3 config extension in `config/pipeline.yaml`
- Stage 3 selection/gating refactor in `app/filter/runner.py`
- Stage 3 report schema extension for newsletter observability
- Stage 3 tests in `tests/test_stage_3_filter.py`
- Stage 3 docs sync:
  - `docs/spec_stage_03_filter_dedup.md`
  - `docs/config_schemas.md`

### Out of Scope
- Stage 1 ingest logic changes (except temporary tuning value)
- Stage 4 balancing/ranking logic
- Network/enrichment behavior

## Required Config Changes (`config/pipeline.yaml`)
This is an additive extension under `stage_3_filter`.
Existing required keys remain required (`min_content_chars`, `min_relevance_score`, `max_candidates_default`, `keyword_groups`).

Add:

```yaml
stage_3_filter:
  min_content_chars: 120
  min_relevance_score: 3
  max_candidates_default: 100
  keyword_groups: ...

  newsletter_policy:
    min_candidates_per_run: 20
    min_relevance_score: 2
    trusted_source_ids:
      - zapier_blog_workflows_1
      - n8n_blog_workflows_1
      - hubspot_growth_marketing_1
    trusted_sources_bypass_score: true
```

### Validation Rules
All existing Stage 3 validation rules remain in force.

Additional required rules:
- `newsletter_policy`: required mapping
- `newsletter_policy.min_candidates_per_run`: non-boolean integer `>= 0`
- `newsletter_policy.min_relevance_score`: non-boolean integer `>= 0`
- `newsletter_policy.trusted_source_ids`: list of strings whose canonical form is unique and non-empty
  - canonical form for each entry is `strip().lower()`
  - bypass matching must use the canonicalized set, not raw strings
- `newsletter_policy.trusted_sources_bypass_score`: boolean
- `newsletter_policy.min_candidates_per_run <= stage_3_filter.max_candidates_default`

Failure behavior:
- any validation failure is a fatal config error (CLI exit code `2`)

## Gate Behavior Changes (Normative)
Gate order remains:
1. required fields
2. min content length (`min_content_chars`)
3. relevance gate (source-type aware)

### Source-Type-Aware Relevance Rule
After computing `relevance_score` and `matched_keywords`:

For `source_type != "newsletter"`:
- pass Gate 3 iff `relevance_score >= stage_3_filter.min_relevance_score`

For `source_type == "newsletter"`:
- if `newsletter_policy.trusted_sources_bypass_score == true` and canonical `source_id` is in canonical `newsletter_policy.trusted_source_ids`, pass Gate 3 regardless of score
- else pass Gate 3 iff `relevance_score >= newsletter_policy.min_relevance_score`

Notes:
- canonical `source_id` for trusted matching is `source_id.strip().lower()` (after Gate 1 required-field normalization)
- `min_content_chars` still applies to all source types, including trusted newsletters
- scoring algorithm and term matching are unchanged
- bypassed newsletters persist the actual computed `relevance_score` and `matched_keywords` (no score rewriting)

## Selection Algorithm Changes (Normative)
Current implementation stops as soon as `passed_count == max_candidates`, which can starve newsletters.

Replace with two-phase deterministic selection.

### Cap-0 rule (explicit)
- if `max_candidates == 0`:
  - do not evaluate gates for any item
  - select nothing
  - keep existing Stage 3 semantics: `items_considered=0`, `passed_count=0`, `failed_count=0`, `reached_max_candidates=true`

### Phase A: evaluate candidates (for `max_candidates > 0`)
Evaluate all unprocessed items in deterministic order:
- order: `published_at DESC, item_id ASC`
- build:
  - `passed_all` (all items passing gates, in evaluation order)
  - `passed_newsletters` (subset of `passed_all` where `source_type=="newsletter"`)
  - `failed_count` and `fail_breakdown` (existing reasons unchanged)

Definitions:
- `evaluated_pass_total = len(passed_all)` (pre-selection passes)
- `evaluated_newsletter_pass_total = len(passed_newsletters)`

### Phase B: enforce newsletter quota and select
- `effective_newsletter_quota = min(newsletter_policy.min_candidates_per_run, max_candidates)`
- `selected_newsletters = first N items of passed_newsletters`, where `N = effective_newsletter_quota`
- `remaining_capacity = max_candidates - len(selected_newsletters)`
- `selected_remainder = first remaining_capacity items from passed_all excluding selected_newsletters`
- `selected_final = selected_newsletters + selected_remainder`

Minimum-quota semantics (normative):
- newsletter quota is a floor, not a ceiling
- `selected_remainder` may include additional newsletters that were not part of `selected_newsletters`

Output ordering contract (normative):
- emit/insert in `selected_final` order
- newsletters selected for quota appear first
- remainder keeps original deterministic evaluation order

Rationale:
- this intentionally prioritizes newsletter visibility in output order so newsletters consistently pass through and are not crowded out by YouTube-heavy ordering

### Determinism and Idempotency
- determinism: same input DB state + same config => identical `selected_final` order
- idempotency: unchanged, already-present `item_id` must not be emitted again

## Report Schema Changes (`stage_3_report_*.json`)
Keep existing fields and add:
- `evaluated_pass_total` (int)
- `evaluated_newsletter_pass_total` (int)
- `selected_newsletter_count` (int)
- `selected_non_newsletter_count` (int)
- `newsletter_quota_target` (int, equals `effective_newsletter_quota`)
- `newsletter_quota_met` (bool)
- `newsletter_pass_breakdown` (map with keys):
  - `passed_standard_threshold`
  - `passed_relaxed_threshold`
  - `passed_trusted_source_bypass`

### Counter/field definitions (normative)
- `passed_count = len(selected_final)` (post-selection)
- `newsletter_quota_target = effective_newsletter_quota`
- `selected_newsletter_count = count(source_type=="newsletter" in selected_final)`
- `selected_non_newsletter_count = passed_count - selected_newsletter_count`
- `newsletter_quota_met = (selected_newsletter_count >= newsletter_quota_target)`

`newsletter_pass_breakdown` scope:
- counts are over `passed_newsletters` from Phase A (pre-selection), not only selected rows
- all keys MUST always be present with non-boolean integer values `>= 0`:
  - `passed_standard_threshold`
  - `passed_relaxed_threshold`
  - `passed_trusted_source_bypass`

`newsletter_pass_breakdown` categories are mutually exclusive, evaluated in this order:
1. `passed_standard_threshold`: `relevance_score >= stage_3_filter.min_relevance_score`
2. `passed_relaxed_threshold`: `newsletter_policy.min_relevance_score <= relevance_score < stage_3_filter.min_relevance_score`
3. `passed_trusted_source_bypass`: `relevance_score < newsletter_policy.min_relevance_score` and trusted-bypass condition applied

### Updated invariants
- `items_considered = evaluated_pass_total + failed_count`
- `items_considered <= items_available_total`
- `passed_count <= evaluated_pass_total`
- `selected_newsletter_count + selected_non_newsletter_count = passed_count`
- `evaluated_newsletter_pass_total = newsletter_pass_breakdown.passed_standard_threshold + newsletter_pass_breakdown.passed_relaxed_threshold + newsletter_pass_breakdown.passed_trusted_source_bypass`
- `selected_newsletter_count <= evaluated_newsletter_pass_total`
- `inserted_db = candidate_items_emitted`
- `candidate_items_emitted <= max_candidates`
- `passed_count = inserted_db + candidates_skipped_already_present`
- `newsletter_quota_met = (selected_newsletter_count >= newsletter_quota_target)`

`reached_max_candidates` definition remains:
- `reached_max_candidates = (passed_count == max_candidates)`
- therefore when `max_candidates == 0`, `reached_max_candidates` is `true`

## CLI Contract
No new CLI flags.

Existing contract remains:
- `python -m app.filter.cli --pipeline config/pipeline.yaml --out ...`

## Development Tuning Requirement
For refactor tuning runs, keep:
- `run_mode.recency_days: 30`

If lower in local environment, set to `30` during development runs.

## Test Plan (Required)
Extend `tests/test_stage_3_filter.py` with at minimum:

1. Newsletter threshold override
- newsletter score `2` passes with global `3`, newsletter `2`
- non-newsletter score `2` fails

2. Trusted source bypass
- trusted newsletter source score `0` passes when min content is met
- untrusted newsletter source score `0` fails

3. Quota guarantee (anti-domination)
- seed data where non-newsletters would otherwise fill cap
- assert selected newsletters are at least `newsletter_quota_target` when enough passed newsletters exist
- assert `newsletter_quota_met == true` when `selected_newsletter_count >= newsletter_quota_target` (including cases where selected newsletters exceed target)
- assert selected output count is still `<= max_candidates`

4. Quota shortfall
- when passed newsletters are fewer than quota target, all passed newsletters are selected
- assert `newsletter_quota_met == false`

5. Output ordering under quota
- assert output order is newsletters-first (`selected_newsletters`), then remainder in deterministic evaluation order
- assert repeated runs with same seed produce identical selected order

6. Report counter invariants
- assert all new fields and invariants, including `evaluated_pass_total` and `evaluated_newsletter_pass_total`
- assert `newsletter_pass_breakdown` totals equal `evaluated_newsletter_pass_total`

7. Config validation
- invalid newsletter_policy shape/type fails with exit code `2`
- duplicate/blank `trusted_source_ids` fails
- `min_candidates_per_run > max_candidates_default` fails

8. `max_candidates == 0` behavior
- no evaluation, no emission, no inserts
- `reached_max_candidates == true`
- new counters are zero-consistent (`evaluated_pass_total==0`, `evaluated_newsletter_pass_total==0`)

## Implementation File Targets
- `app/filter/runner.py`
- `app/filter/models.py` (only if helpers/model types are needed)
- `tests/test_stage_3_filter.py`
- `config/pipeline.yaml` (add `newsletter_policy`)
- `docs/config_schemas.md`
- `docs/spec_stage_03_filter_dedup.md` (sync Stage 3 base spec to new semantics)

## Acceptance Criteria
- Stage 3 enforces newsletter minimum representation when eligible newsletters exist.
- Newsletter eligibility precedence is exact and deterministic:
  - required + min-length gates always apply
  - trusted-source bypass (when enabled and source trusted) overrides newsletter threshold
  - otherwise newsletter threshold applies
  - non-newsletter uses global threshold
- Selection enforces quota and emits in newsletters-first order.
- YouTube cannot fully dominate selected results when quota-eligible newsletters exist.
- Report fields are fully populated and invariant-safe.
- Existing Stage 3 behavior outside this refactor (scoring algorithm, idempotency, deterministic query order) remains intact.

---

## Files Changed (Spec Authoring)
- `docs/spec_stage_03_newsletter_quota_refactor.md`

## Commands to Run (Spec Authoring)
- Not run (spec-only authoring task)

## Produced Artifacts
- None (no runtime pipeline execution in spec-only task)
