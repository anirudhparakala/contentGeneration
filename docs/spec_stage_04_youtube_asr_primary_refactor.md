# SPEC - STAGE 4 REFACTOR: YouTube ASR-Primary Enrichment (CUDA faster-whisper + yt-dlp audio-only)

## Depends on
- Stage 0
- Stage 3 (`candidates` table available with selection-balance required columns, including `source_id`)
- Existing Stage 4 baseline:
  - `docs/spec_stage_04_enrich.md`
  - `docs/spec_stage_04_selection_balance_refactor.md`
  - `app/enrich/runner.py`
  - `app/enrich/youtube.py`

## Objective
Replace Stage 4 YouTube transcript retrieval with an ASR-primary flow that is reliable without transcript API/proxy dependencies:
- use `yt-dlp` to acquire audio only
- transcribe with `faster-whisper` on CUDA (RTX 4070 target)
- keep newsletter extraction implementation unchanged (fetch/extract/normalize/threshold/evidence), while allowing run-level terminal behavior changes via `require_full_success`
- enforce full-success semantics so selected rows are not allowed to end as per-item failures

Primary business goals:
- remove dependency on `youtube-transcript-api` for Stage 4 YouTube enrichment
- avoid transcript/proxy failure classes blocking YouTube coverage
- maximize YouTube text quality with a speed/quality-balanced model
- ensure Stage 4 selected rows are enriched successfully or the run is fatal

Non-goals:
- no change to Stage 3 scoring/ranking
- no change to newsletter extraction implementation
- no change to Stage 5+ downstream logic

## Precedence / Supersession
This spec supersedes conflicting Stage 4 YouTube behavior in prior Stage 4 specs.

Normative precedence rules:
- for Stage 4 YouTube enrichment behavior, this spec overrides:
  - `docs/spec_stage_04_enrich.md`
  - `docs/spec_stage_04_selection_balance_refactor.md`
- this refactor updates Stage 4 YouTube behavior previously recorded in `docs/decisions.md`; if `docs/decisions.md` contains conflicting Stage 4 YouTube enrichment statements (for example transcript-first flow or transcript/ASR caps controlling YouTube control flow), this spec governs implementation until `docs/decisions.md` is updated in the same change set
- the selection-balance refactor remains in force for Stage 4 selection and cooldown behavior, except where this spec explicitly changes full-success terminal behavior and YouTube failure mapping
- when conflicts exist, implementers must follow this document for Stage 4 YouTube path semantics

## Scope
### In Scope
- Stage 4 YouTube enrichment flow in `app/enrich/youtube.py` and `app/enrich/runner.py`
- Stage 4 config extension in `config/pipeline.yaml`
- Stage 4 report extension in `stage_4_report_*.json`
- Stage 4 test updates in `tests/test_stage_4_enrich.py`
- Docs sync:
  - `docs/spec_stage_04_enrich.md`
  - `docs/config_schemas.md`
  - `docs/decisions.md` (library choice note for Stage 4 YouTube path)

### Out of Scope
- ingest/normalize/filter stage behavior
- candidate ranking formula changes
- introducing remote ASR services

## Required Config Changes (`config/pipeline.yaml`)
Add required YouTube ASR configuration under `stage_4_enrich`:

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
      - youtube_asr_failed
      - youtube_text_too_short
  youtube_enrichment:
    mode: asr_only
    require_full_success: true
    audio:
      format_selector: "bestaudio"
      extract_format: "wav"
      download_timeout_s: 180
      download_retries: 2
      retry_backoff_s: 2.0
    asr:
      model: "distil-large-v3"
      device: "cuda"
      compute_type: "float16"
      language: "en"
      beam_size: 5
      temperature: 0.0
      condition_on_previous_text: false
      vad_filter: true
      max_audio_seconds: 7200
      min_chars: 800
      allow_cpu_fallback: false
```

### Validation Rules
All existing Stage 4 config validation remains.

Additional required rules:
- `stage_4_enrich.youtube_enrichment`: required mapping
- `stage_4_enrich.youtube_enrichment.mode`: required string enum; only `asr_only` supported in this refactor
- `stage_4_enrich.youtube_enrichment.require_full_success`: required boolean
- `stage_4_enrich.youtube_enrichment.audio`: required mapping
- `audio.format_selector`: required string enum; only `bestaudio` supported in `mode=asr_only`
- `audio.extract_format`: non-empty string
- `audio.download_timeout_s`: non-boolean integer `>= 30`
- `audio.download_retries`: non-boolean integer `>= 0`
- `audio.retry_backoff_s`: number `> 0`
- `stage_4_enrich.youtube_enrichment.asr`: required mapping
- `asr.model`: non-empty string
- `asr.device`: string enum `cuda|cpu`
- `asr.compute_type`: non-empty string (validated against faster-whisper/ctranslate2 accepted values at runtime)
- `asr.language`: required non-empty string
- `asr.beam_size`: non-boolean integer `>= 1`
- `asr.temperature`: number `>= 0`
- `asr.condition_on_previous_text`: boolean
- `asr.vad_filter`: boolean
- `asr.max_audio_seconds`: non-boolean integer `>= 60`
- `asr.min_chars`: non-boolean integer `>= 1`
- `asr.allow_cpu_fallback`: boolean
- legacy cap compatibility requirement:
  - `caps.max_transcripts_per_run` and `caps.max_asr_fallbacks_per_run` remain required pipeline keys and must pass existing validation
  - missing/invalid values remain fatal (exit code `2`)
  - in `mode=asr_only`, these values are compatibility/observability fields only and do not affect YouTube control flow

Failure behavior:
- config validation errors are fatal (exit code `2`)

## YouTube Enrichment Behavior Changes (Normative)

### 1) Transcript path removal
For `source_type == "youtube"` in Stage 4:
- do not call `youtube-transcript-api`
- do not run transcript language policy/manual-vs-generated logic
- do not execute transcript-cap logic (`max_transcripts_per_run`)
- do not execute ASR-fallback-cap logic (`max_asr_fallbacks_per_run`)

In `asr_only` mode, every valid selected YouTube row goes directly to ASR.

### 2) Video ID parse
Keep existing deterministic URL parsing and validation rules from Stage 4 baseline.
- parse failure maps to `youtube_video_id_parse_failed`

### 3) Audio acquisition (audio-only, no full video)
Acquire YouTube media using `yt-dlp` with audio-only intent:
- required command characteristics:
  - include `-f <format_selector>` where `<format_selector>` must be exactly `bestaudio` in `mode=asr_only`
  - include extract-audio mode (`-x` / `--extract-audio`)
  - include `--audio-format <extract_format>` default `wav`
  - include `--no-playlist`
  - include `--ffmpeg-location <ffmpeg_bin>` (required after dependency preflight resolution)
- required metadata precheck (before first download attempt):
  - run `yt-dlp` metadata probe for the same URL with `--dump-single-json --no-playlist`
  - parse `duration` as numeric seconds
  - if probe fails, `duration` is missing/invalid, or `duration > asr.max_audio_seconds`, fail item as `youtube_asr_failed` without launching ASR
- download retry/timeout policy:
  - attempt budget is `1 + audio.download_retries`
  - each attempt must enforce subprocess timeout `audio.download_timeout_s`
  - between failed attempts, wait `audio.retry_backoff_s * (2 ** retry_index)` seconds where `retry_index` starts at `0` for the first retry
  - if all attempts fail or time out, fail item as `youtube_asr_failed`
- output must be written to a temporary file path and deleted after use
- downloading muxed full video formats is not allowed by default behavior
- `audio.format_selector` values that could permit full-video/muxed fallback are disallowed in `mode=asr_only`; treat as fatal config validation error (exit `2`)

Audio acquisition failures map to `youtube_asr_failed`.

### 4) ASR execution
Use `faster-whisper` with run-configured parameters:
- model default: `distil-large-v3`
- device default: `cuda`
- compute_type default: `float16`
- decoding parameters are fully config-driven; recommended baseline values in default pipeline config:
  - language `en`
  - beam_size `5`
  - temperature `0.0`
  - condition_on_previous_text `False`
  - vad_filter `True`

Model lifecycle requirements:
- instantiate Whisper model once per run (after YouTube preflight) and reuse for all YouTube items
- avoid per-item model re-initialization

Text normalization and threshold classification:
- join segment texts in model output order
- `normalize_text(" ".join(collected_text_parts))`
- let `n = len(normalized_text)`
- if `n == 0`: terminal fail reason is `youtube_asr_failed`
- if `1 <= n < asr.min_chars`: terminal fail reason is `youtube_text_too_short`
- if `n >= asr.min_chars`: item is enrichment-success

Enrichment payload rules:
- set `enrichment_method = "asr_faster_whisper"` for successful YouTube ASR path
- ASR evidence snippets must use deterministic windows from normalized text:
  - `[0:240]`, `[240:480]`, `[480:720]` (include only non-empty)
  - `meta.type = "transcript"`, `meta.timestamp = null`, `meta.offset = window_start`

ASR counters:
- ASR pipeline boundary (normative): in `asr_only`, ASR pipeline starts at yt-dlp metadata probe and includes metadata probe, audio acquisition, and transcription
- `youtube_asr_attempted` increments exactly once per selected valid YouTube row after successful video-id parse and immediately before metadata probe starts
- metadata probe, audio acquisition, and transcription failures after this increment map to `youtube_asr_failed`
- retries within a single item (probe/download/transcribe) must not increment `youtube_asr_attempted` more than once for that item
- `youtube_asr_succeeded` increments exactly once when ASR returns non-empty normalized text (`len(normalized_text) >= 1`), even if item later fails `youtube_text_too_short`
- `youtube_text_too_short` is valid only for items where `youtube_asr_succeeded` incremented for that item

ASR execution failures map to `youtube_asr_failed`.

### 5) CUDA policy
When `asr.device == "cuda"`:
- Stage 4 must validate CUDA availability during YouTube preflight before item processing
- if CUDA unavailable and `allow_cpu_fallback == false`: fatal run error (exit `2`)
- if CUDA unavailable and `allow_cpu_fallback == true`: downgrade to `cpu` for this run, force effective compute type to `int8`, and log warning

When `asr.device == "cpu"`:
- run proceeds without CUDA checks

Model initialization preflight:
- if selected rows contain any YouTube items, Stage 4 must initialize WhisperModel once before first item processing using:
  - configured `model`
  - effective device after CUDA fallback policy
  - effective compute type (configured value unless forced to `int8` by CUDA fallback)
- compute-type incompatibility for the effective device is a fatal run error (exit `2`)
- if model initialization fails for effective settings, run is fatal (exit `2`)

### 6) Full-success requirement
`require_full_success` controls per-item terminal behavior after startup/preflight has succeeded.

Scope boundary (normative):
- `require_full_success` does not modify run-level fatal checks
- the following remain fatal (exit `2`) regardless of this flag:
  - config validation failures
  - required dependency preflight failures (including version-detection failures defined in this spec)
  - CUDA policy failures / model initialization failures / effective compute-type incompatibility
  - fatal DB/report/artifact I/O failures

Per-item behavior:
- if `false`: continue processing after per-item failures using existing fail mapping and retry-state writes; run may complete with `exit 0` when no run-level fatal occurs
- if `true` (default in this refactor):
  - selection compatibility rule: exclude `invalid_pool` rows from final selected sequence (they remain counted in `invalid_pool_total` for observability)
  - deterministic selection-fill override:
    - `invalid_pool` rows are not selectable in any selection phase
    - during remainder filling, skipped invalid rows do not consume capacity; selection continues scanning later ordered rows to select additional valid eligible rows while preserving deterministic order and per-source caps
    - if no further valid eligible rows exist, selection may terminate below `max_items`
  - any selected item that reaches a mapped failure reason must terminate the run as fatal after recording that failure in report counters
  - fail-fast ordering (normative):
    1) record failure reason counters/report state for the selected item
    2) perform retry-state upsert per selection-balance retry rules (except `invalid_candidate_row`)
    3) terminate run as fatal
  - processing stops immediately after this terminal outcome is fully recorded

Normative result under `require_full_success=true`:
- completed runs (`exit 0`) must satisfy:
  - `failed_count == 0`
  - `selected_invalid_count == 0`
  - all processed selected rows are successful enrichments
- fatal runs under this flag must also satisfy:
  - `selected_invalid_count == 0`
- runs with any per-item failure terminate with `exit 2`
- `max_items` remains an upper bound; completed runs may have `selected_rows_total < max_items` when invalid-pool exclusion or eligibility limits apply

Fatal payload requirement for full-success termination:
- when termination is caused by first per-item failure under `require_full_success=true`, report payload must set:
  - `run_status = "fatal"`
  - non-empty `fatal_error`
  - `fatal_error` string must include both `reason=<fail_reason>` and `item_id=<item_id>` for the terminal failed item

This requirement enforces: selected rows are not allowed to remain unenriched in a completed run.

## Fail Reasons and Counter Semantics
Keep existing fail keys for backward compatibility:
- `invalid_candidate_row`
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

In `asr_only` mode:
- transcript-related fail keys (`youtube_transcript_unavailable`, `youtube_transcript_failed`, `transcript_cap_reached`, `asr_cap_reached`) must stay present but should remain `0`
- transcript counters remain present but are fixed to:
  - `youtube_transcripts_attempted = 0`
  - `youtube_transcripts_succeeded = 0`
- legacy ASR fallback counter remains present but is fixed to:
  - `asr_fallbacks_used = 0`
- legacy caps `max_transcripts` and `max_asr` remain present in report for backward compatibility but do not control YouTube enrichment in `asr_only`
- unexpected per-item exception mapping override (normative):
  - this refactor supersedes Stage 4 baseline unexpected YouTube fallback mapping
  - in `mode=asr_only`, uncaught exceptions in the YouTube branch after candidate validation must map to `youtube_asr_failed` (not `youtube_transcript_failed`)

Add ASR-first counters:
- `youtube_asr_attempted` (int)
- `youtube_asr_succeeded` (int)
- invariant: `youtube_asr_succeeded <= youtube_asr_attempted`
- implementation invariant (not a report field):
  - `inserted_db_youtube_asr <= youtube_asr_succeeded`, where `inserted_db_youtube_asr` means inserted rows with `enrichment_method = "asr_faster_whisper"`

### Definitions used in this refactor
- `selected_youtube_count`:
  - count of final selected valid rows whose canonical `source_type == "youtube"`
  - computed after any `require_full_success` invalid-pool exclusion is applied
- `selected_newsletter_count`:
  - count of final selected valid rows whose canonical `source_type == "newsletter"`
- `selected_invalid_count`:
  - count of selected rows that fail candidate parsing/selection validation (`invalid_candidate_row`)

## Dependency Policy Changes
Startup requirements for Stage 4:
- required Python modules:
  - always required: `trafilatura`
  - conditionally required when `selected_youtube_count > 0`: `faster_whisper`, `ctranslate2`
- required executables for YouTube path when `selected_youtube_count > 0`:
  - `yt-dlp` (must resolve to executable path)
  - `ffmpeg` (must resolve to executable path)

Version contract for YouTube path (`selected_youtube_count > 0`):
- Python package versions for this path are pinned by `requirements.txt`; preflight must resolve expected pinned versions for:
  - `faster-whisper`
  - `ctranslate2`
- pin parsing and enforcement rules (normative):
  - expected versions must come from exact `==` pins in `requirements.txt`
  - `requirements.txt` source is repository-root `requirements.txt` used by this project
  - package-name matching for pin lookup is case-insensitive and treats `-` and `_` as equivalent
  - if either required package pin is missing, not exact-`==`, or unparseable, run is fatal (exit `2`)
- preflight must resolve installed runtime versions for:
  - `faster_whisper`
  - `ctranslate2`
- if resolved runtime version differs from expected pinned version for either package, run is fatal (exit `2`)
- executable versions must be resolved during preflight for:
  - `yt-dlp` via `yt-dlp --version`
  - `ffmpeg` via `ffmpeg -version` (first line)
- if any required version cannot be resolved/determined, run is fatal (exit `2`)
- resolved versions and Python pin-match status must be logged once per run at INFO level

If required dependency checks fail, run is fatal (exit `2`).

YouTube preflight timing:
- selection phase executes first as defined by selection-balance spec
- when `selected_youtube_count > 0`, perform YouTube dependency preflight, CUDA policy checks, and one-time model initialization before first selected item is processed

`youtube-transcript-api` is no longer a required Stage 4 dependency in `asr_only` mode.

## Report Contract Changes (`stage_4_report_*.json`)
Keep all existing required fields from Stage 4 and selection-balance refactor.

Add required fields:
- `youtube_asr_attempted` (int)
- `youtube_asr_succeeded` (int)
- `youtube_asr_model` (string)
- `youtube_asr_device_effective` (string)
- `youtube_asr_compute_type` (string)
- `youtube_preflight_executed` (bool)
- `full_success_required` (bool)

Effective-value semantics:
- `youtube_asr_device_effective` is the actual device used by WhisperModel for this run after CUDA fallback policy
- `youtube_asr_compute_type` is the actual compute type used by WhisperModel for this run after CUDA fallback policy
- `youtube_preflight_executed` is `true` iff Stage 4 begins YouTube preflight/model-init sequence for this run
- when `selected_youtube_count == 0`:
  - `youtube_preflight_executed = false`
  - emit configured ASR values for `youtube_asr_model`, `youtube_asr_device_effective`, and `youtube_asr_compute_type` without running YouTube preflight
- when `selected_youtube_count > 0`:
  - `youtube_preflight_executed = true` before dependency/CUDA/model-init preflight starts
  - if fatal occurs during preflight, keep `youtube_preflight_executed = true` in fatal report

Additional invariants:
- `youtube_asr_succeeded <= youtube_asr_attempted`
- `fail_breakdown.youtube_text_too_short <= youtube_asr_succeeded`
- `youtube_preflight_executed == (selected_youtube_count > 0)`
- in `asr_only` mode:
  - `youtube_transcripts_attempted == 0`
  - `youtube_transcripts_succeeded == 0`
  - `asr_fallbacks_used == 0`
  - `fail_breakdown.youtube_transcript_unavailable == 0`
  - `fail_breakdown.youtube_transcript_failed == 0`
  - `fail_breakdown.transcript_cap_reached == 0`
  - `fail_breakdown.asr_cap_reached == 0`
- if `full_success_required == true` and `run_status == "completed"`:
  - `failed_count == 0`
  - `selected_invalid_count == 0`

Fatal report behavior remains aligned with Stage 4 baseline:
- if fatal report write succeeds, emit all required fields with valid defaults
- fatal defaulting rules for new ASR fields:
  - always include `youtube_asr_attempted`, `youtube_asr_succeeded`, `youtube_asr_model`, `youtube_asr_device_effective`, `youtube_asr_compute_type`, `youtube_preflight_executed`, and `full_success_required`
  - if pipeline config was loaded:
    - `youtube_asr_model` = configured model
    - `youtube_asr_device_effective` = configured device unless CUDA fallback was resolved for this run
    - `youtube_asr_compute_type` = configured compute type unless CUDA fallback forced `int8`
    - `youtube_preflight_executed = (selected_youtube_count > 0)`
    - `full_success_required` = configured `require_full_success`
  - if pipeline config was not available (pre-config fatal):
    - `youtube_asr_model = ""`
    - `youtube_asr_device_effective = ""`
    - `youtube_asr_compute_type = ""`
    - `youtube_preflight_executed = false`
    - `full_success_required = false`
  - when run counters are unavailable due to early fatal:
    - `youtube_asr_attempted = 0`
    - `youtube_asr_succeeded = 0`

## CLI Contract
No new CLI flags required in this refactor.

Existing flags remain:
- `--max-items`
- `--max-transcripts` (accepted for backward compatibility; ignored in `asr_only` mode)
- `--max-asr` (accepted for backward compatibility; ignored in `asr_only` mode)

Normative compatibility behavior:
- `--max-transcripts` and `--max-asr` must still be validated as non-boolean integers `>= 0`
- values are still emitted in legacy report fields (`max_transcripts`, `max_asr`) for compatibility
- in `asr_only`, these values do not affect control flow
- log warning when transcript/asr cap overrides are provided while `mode=asr_only`

## Determinism and Performance Requirements
- selection ordering and selection-phase behavior are unchanged from selection-balance spec, except `require_full_success=true` excludes `invalid_pool` rows from final selected sequence
- ASR transcript text assembly must preserve model segment order
- yt-dlp retry/backoff schedule must be deterministic for identical config and failure sequence
- model object reuse is required for run-time performance
- temp audio artifacts must be cleaned even on exceptions

## Test Plan (Required)
Extend `tests/test_stage_4_enrich.py` with at least:

1. ASR-only YouTube success path
- no transcript API calls
- `youtube_asr_attempted` and `youtube_asr_succeeded` increment
- enrichment method is `asr_faster_whisper`
- ASR evidence snippets follow `[0:240]`, `[240:480]`, `[480:720]` window contract

2. Audio-only yt-dlp invocation
- assert command includes audio-only extraction flags (`-f bestaudio`, `-x`, `--audio-format`, `--ffmpeg-location`)
- assert timeout and retry/backoff behavior follows `audio.download_timeout_s`, `audio.download_retries`, `audio.retry_backoff_s`

3. CUDA model config pass-through
- assert WhisperModel receives configured `model/device/compute_type`
- assert transcribe receives configured decode params
- assert WhisperModel is initialized once per run and reused across multiple YouTube items

4. Startup dependency validation
- when `selected_youtube_count > 0`, missing `faster_whisper`, `yt-dlp`, or `ffmpeg` causes fatal exit `2`
- when `selected_youtube_count > 0`, unresolvable versions for `faster_whisper`, `ctranslate2`, `yt-dlp`, or `ffmpeg` cause fatal exit `2`
- when `selected_youtube_count > 0`, mismatch between runtime version and `requirements.txt` exact-`==` pin for `faster-whisper` or `ctranslate2` causes fatal exit `2`
- when `selected_youtube_count == 0`, missing YouTube-specific dependencies does not fail run startup

5. CUDA availability behavior
- `device=cuda` + unavailable + no fallback => fatal exit `2`
- `device=cuda` + unavailable + fallback allowed => cpu downgrade and run continues

6. Full-success required behavior
- first selected-item failure triggers fatal termination
- completed run with this flag has `failed_count == 0`
- when this flag is true, selected invalid rows are excluded (`selected_invalid_count == 0`)

7. Legacy transcript counters compatibility
- assert transcript counters, transcript fail keys, and `asr_fallbacks_used` are present and zero in `asr_only` mode

8. YouTube threshold edges
- ASR text length `0` maps to `youtube_asr_failed`
- 799/800 char behavior for ASR text threshold (`youtube_text_too_short` vs success)

9. Report invariants
- include new ASR-first invariants and full-success invariants

10. Legacy cap override compatibility
- `--max-transcripts` and `--max-asr` accepted and reported, but do not alter ASR-only behavior
- warning is logged when these overrides are provided in `asr_only`

11. No-network unit tests
- all YouTube/ASR and yt-dlp interactions mocked; no live internet

12. `require_full_success=false` compatibility
- per-item failures do not force fatal termination
- run can still complete with `exit 0` and `failed_count > 0` under continue-on-item-failure behavior when no run-level fatal occurs

13. Uncaught YouTube exception mapping in `asr_only`
- inject uncaught exception in YouTube branch after candidate validation
- assert mapping to `youtube_asr_failed` (not `youtube_transcript_failed`)

14. Full-success fatal payload detail contract
- trigger first selected-item failure with `require_full_success=true`
- assert fatal report payload includes:
  - `run_status = "fatal"`
  - `fatal_error` contains both `reason=<fail_reason>` and `item_id=<item_id>`

15. Zero-YouTube preflight/report behavior
- build a run where `selected_youtube_count == 0`
- assert YouTube dependency preflight and Whisper model initialization are not executed
- assert `youtube_preflight_executed == false`
- assert report emits configured ASR values for:
  - `youtube_asr_model`
  - `youtube_asr_device_effective`
  - `youtube_asr_compute_type`

16. Duration guard behavior
- metadata probe duration above `asr.max_audio_seconds` maps to `youtube_asr_failed` without ASR invocation
- missing/invalid duration in metadata probe maps to `youtube_asr_failed`
- each metadata-probe failure still increments `youtube_asr_attempted` exactly once for that item

17. Preflight executed flag behavior
- with `selected_youtube_count > 0`, assert `youtube_preflight_executed == true` on both completed and preflight-fatal runs

## Implementation File Targets
- `app/enrich/youtube.py`
- `app/enrich/runner.py`
- `app/enrich/models.py` (report/counter model updates if needed)
- `app/enrich/cli.py`
- `config/pipeline.yaml`
- `requirements.txt`
- `tests/test_stage_4_enrich.py`
- `docs/spec_stage_04_enrich.md`
- `docs/config_schemas.md`
- `docs/decisions.md`

## Acceptance Criteria
- Stage 4 YouTube enrichment runs with ASR-only flow and does not depend on transcript API.
- CUDA-accelerated faster-whisper is used by default with `distil-large-v3` + `float16`.
- YouTube media acquisition is audio-only via yt-dlp + ffmpeg extraction path.
- yt-dlp acquisition uses configured timeout/retry policy and enforces metadata duration guard via `asr.max_audio_seconds`.
- Completed runs under default config satisfy full-success requirement (`failed_count == 0`).
- `require_full_success` controls only per-item terminal behavior and does not alter run-level fatal checks.
- Stage 4 report includes new ASR-first fields and preserves backward-compatible transcript/cap fields and `asr_fallbacks_used` as zero in `asr_only` mode.
- Stage 4 report includes `youtube_preflight_executed` and uses configured-value fallback semantics when YouTube preflight is not executed.
- Tests cover ASR flow, dependency checks, CUDA policy, strict success behavior, and report invariants.

---

## Files Changed (Spec Authoring)
- `docs/spec_stage_04_youtube_asr_primary_refactor.md`

## Commands to Run (Spec Authoring)
- Not run (spec-only authoring task)

## Produced Artifacts
- None (no runtime execution in spec-only task)
