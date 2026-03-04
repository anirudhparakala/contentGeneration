# SPEC - STAGE 4: Enrichment (Newsletter text + YouTube transcript) - Manual Batch

## Depends on
- Stage 0
- Stage 3 (SQLite table `candidates` exists; it may be empty)

## Objective
Enrich candidate items with substantive text:
- newsletters: fetch article page and extract main text
- youtube: fetch transcript; fallback to local ASR for a capped number

Persist enriched results to SQLite and emit JSONL for this run.

## In Scope
- Read defaults from `config/pipeline.yaml`:
  - `paths.sqlite_db`
  - `paths.outputs_dir`
  - `http.*`
  - `caps.max_transcripts_per_run`
  - `caps.max_asr_fallbacks_per_run`
- Read candidates directly from SQLite `candidates` table (do not join `items`).
- Enrich per `source_type`:
  - newsletter: HTTP GET article URL + trafilatura extraction
  - youtube: transcript retrieval via youtube-transcript-api; ASR fallback when transcript is unavailable
- Persist successful enrichments to SQLite `enriched_items` table (idempotent).
- Emit:
  - JSONL of enriched items (successes inserted this run only)
  - Stage 4 report JSON

## Out of Scope
- LLM extraction/scoring/generation
- Embeddings/vector DB
- Slack/email delivery
- Google Sheets writes

## Repo Layout (Must Follow)
Implement code under:
- `app/enrich/models.py`
- `app/enrich/fetch.py` (timeouts, retries, size cap; Stage 4 HTTP policy defined below)
- `app/enrich/newsletter.py` (page fetch + trafilatura extraction)
- `app/enrich/youtube.py` (video_id parsing + transcript fetch + ASR fallback)
- `app/enrich/state.py` (SQLite helpers for enriched_items table)
- `app/enrich/runner.py`
- `app/enrich/cli.py`
- `app/enrich/__init__.py`

## CLI Contract
Primary command:
- `python -m app.enrich.cli --pipeline config/pipeline.yaml`

Optional overrides:
- `--db <path>` overrides `paths.sqlite_db`
- `--out <path>` overrides default output path
- `--report <path>` overrides default report path
- `--max-items <int>` caps total candidates selected this run (default `50`)
- `--max-transcripts <int>` overrides `caps.max_transcripts_per_run`
- `--max-asr <int>` overrides `caps.max_asr_fallbacks_per_run`
- `--log-level <LEVEL>` default `INFO`

Override validation:
- `--max-items`, `--max-transcripts`, `--max-asr` must be non-boolean integers `>= 0`; otherwise fatal error (exit code `2`).

Defaults:
- DB: `paths.sqlite_db`
- out: `{paths.outputs_dir}/enriched_items_<YYYY-MM-DD>.jsonl` if `--out` omitted
- report: `{paths.outputs_dir}/stage_4_report_<YYYY-MM-DD>.json` if `--report` omitted
- max-items: `50`
- max-transcripts: `caps.max_transcripts_per_run`
- max-asr: `caps.max_asr_fallbacks_per_run`

Pipeline config validation (normative):
- Fatal error (exit code `2`) if any required config key is missing or has invalid type/value.
- Required mappings: `paths`, `caps`, `http`, `http.retries`.
- `paths.sqlite_db`, `paths.outputs_dir`, `http.user_agent` must be non-empty strings after `strip()`.
- `http.connect_timeout_s`, `http.read_timeout_s`, `http.max_response_mb`, `http.retries.max_attempts` must be non-boolean integers `>= 1`.
- `caps.max_transcripts_per_run`, `caps.max_asr_fallbacks_per_run` must be non-boolean integers `>= 0`.

Date basis:
- Bind one `run_date_utc` at run start from `started_at` (UTC).
- Default output/report filenames must use `run_date_utc` for the entire run.

Behavior:
- Overwrite output JSONL each run.
- Always create/truncate the output JSONL once `output_path` is resolved and before item processing begins; when fatal termination occurs before `output_path` is resolved, output file creation is skipped.
- Create parent directories for `output_path` and `report_path` when missing (`mkdir -p` semantics).
- Select and process candidates in deterministic order (defined below).
- Continue on per-item errors; do not fail the entire run for item-level failures.
- Fatal runtime I/O policy (normative):
  - Any SQLite operation failure that is not an expected `INSERT OR IGNORE` conflict is a fatal run error (exit code `2`).
  - Any failure to write/truncate output JSONL or write report JSON is a fatal run error (exit code `2`).
  - On fatal run error, stop processing immediately.
  - Fatal SQLite/artifact I/O errors are not per-item failures and must never be remapped into `fail_breakdown` reasons.
- Fatal artifact policy (normative):
  - Output JSONL is best-effort and may be partially written with rows emitted before a fatal run error.
  - No rollback/truncation of already-written JSONL rows is required after a fatal run error occurs.
  - Stage 4 must attempt to write report JSON even on fatal run errors once `report_path` is resolved.
  - Pre-config fatal clarification:
    - If fatal occurs before `report_path` is resolved (for example pipeline read/parse failure with no `--report` override), report write is skipped and run exits `2`.
    - If `--report` is provided, that value defines `report_path` even when pipeline load fails; Stage 4 must attempt fatal report write to that path.
  - If fatal report write succeeds, report fields `run_status` and `fatal_error` must indicate fatal termination (defined in report section).
  - If fatal report write fails, run exits `2` and report JSON may be absent.
- Exit code:
  - `0` = completed (even if some items failed)
  - `2` = fatal error (cannot read pipeline, invalid overrides, cannot open DB, cannot write outputs)

## Input Contract (SQLite `candidates` table)
Stage 4 reads from `candidates` table created in Stage 3.

Columns used:
- `item_id` (TEXT PK)
- `source_type` (TEXT)
- `url` (TEXT)
- `title` (TEXT)
- `published_at` (TEXT)
- `relevance_score` (INTEGER)

Trust boundary (normative):
- Stage 4 relies on Stage 3 for schema and field formatting.
- Stage 4 must still reject malformed candidate rows at runtime.
- A candidate row is malformed if any of `item_id`, `source_type`, `url`, `title`, `published_at` is missing, non-string, or empty after `strip()`.
- `relevance_score` is required for deterministic ordering and must be a non-boolean integer `>= 0`; otherwise fail `invalid_candidate_row`.
- Canonicalization rule: for `item_id`, `source_type`, `url`, `title`, and `published_at`, use trimmed values (`value.strip()`) for all downstream logic and for emitted/persisted values.
- After `strip().lower()`, `source_type` must be exactly `newsletter` or `youtube`; otherwise fail `invalid_candidate_row`.
- Use canonical `source_type = source_type.strip().lower()` for branch selection and emitted/persisted values.
- `published_at` must match UTC second-precision ISO8601 `YYYY-MM-DDTHH:MM:SSZ`; otherwise fail `invalid_candidate_row`.
- Malformed candidate rows fail with reason `invalid_candidate_row`.

Stage dependency validation (normative):
- On run start, Stage 4 must verify that table `candidates` exists and can be queried with required columns:
  - `item_id`, `source_type`, `url`, `title`, `published_at`, `relevance_score`
- If the table is missing, unreadable, or required columns are missing, fail the run with exit code `2`.

## Deterministic Selection Query
Create `enriched_items` first (if needed), then select unprocessed candidates:

```sql
SELECT
  c.item_id,
  c.source_type,
  c.url,
  c.title,
  c.published_at,
  c.relevance_score
FROM candidates c
LEFT JOIN enriched_items e ON e.item_id = c.item_id
WHERE e.item_id IS NULL
ORDER BY c.relevance_score DESC, c.published_at DESC, c.item_id ASC
LIMIT :max_items;
```

Definitions:
- `candidates_available_total`: count of rows matching `WHERE e.item_id IS NULL` before `LIMIT`.
- `selected_rows_total`: count of rows returned by the selection query after `LIMIT` (implementation-local; useful for logs/debugging).
- `items_selected`: count of selected rows that reached a terminal per-item outcome (`success` or exactly one mapped failure reason) before run end.
- On completed runs, `items_selected == selected_rows_total`.
- On fatal runs, `items_selected` may be lower than `selected_rows_total` because processing stops immediately.
- If `max_items == 0`, both `selected_rows_total` and `items_selected` must be `0` and no rows are processed.

## Text Normalization (Normative)
Define `normalize_text(s)`:
- input must be `str`; non-string inputs must be handled before calling this function by source-specific failure mapping
- replace all whitespace runs (`\s+`) with a single space
- apply `.strip()`

All minimum-length checks and persisted `enriched_text` use normalized text only.

## EnrichedItem Contract (JSONL + DB)
Each emitted JSONL line is one successfully inserted enriched item.

Required fields:
- `item_id` (string)
- `source_type` (`newsletter` | `youtube`)
- `url` (string)
- `title` (string)
- `published_at` (ISO8601 UTC Z)
- `enriched_text` (string, normalized)
- `evidence_snippets` (list[object])
  - each object: `{ "text": str, "meta": { "type": "article"|"transcript", "offset": int|null, "timestamp": str|null } }`
- `enrichment_method` (string)
  - newsletter: `trafilatura`
  - youtube: `yt_transcript_api` | `asr_faster_whisper`
- `enriched_at` (ISO8601 UTC Z)

Success criteria:
- newsletters: `len(enriched_text) >= 500`
- youtube: `len(enriched_text) >= 800`

Output emission rule (normative):
- Emit JSONL only for rows inserted into `enriched_items` this run.
- If DB insert is ignored due to existing `item_id`, do not emit that row.

## Newsletter Enrichment
For `source_type == "newsletter"`:
1) Fetch article HTML from `url` using the Stage 4 HTTP policy in this spec.
2) Extract main text with trafilatura.
   - Deterministic extraction options (normative):
     - call `trafilatura.extract` with:
       - `output_format="txt"`
       - `include_comments=False`
       - `include_tables=False`
       - `favor_precision=True`
       - `favor_recall=False`
       - `no_fallback=False`
   - Pass fetched article payload directly to extractor (no pre-cleaning transforms).
3) Normalize extracted text via `normalize_text`.
4) Pass if normalized length is `>= 500`.
5) Build `evidence_snippets` from normalized text:
   - `snippet_1`: `enriched_text[0:240]` if non-empty, `offset=0`
   - `snippet_2`: `enriched_text[240:480]` if non-empty, `offset=240`
   - `meta.type = "article"`, `meta.timestamp = null`

Notes:
- Candidate `content_text` is ignored for newsletter enrichment.
- Stage 4 must always fetch and extract from page URL for newsletters.

Failure reasons:
- `newsletter_fetch_failed`
- `newsletter_extract_failed`
- `newsletter_text_too_short`

Failure mapping (normative):
- `newsletter_fetch_failed`:
  - HTTP/network failure after retries, non-retryable HTTP 4xx, or response size-cap violation during fetch.
- `newsletter_extract_failed`:
  - trafilatura invocation raises an exception, or extraction result is `None`.
  - extraction result is non-string.
- `newsletter_text_too_short`:
  - extraction returned a string value, but `len(normalize_text(extracted)) < 500` (including empty/whitespace-only after normalization).

## YouTube Enrichment
For `source_type == "youtube"`:
- Candidate `content_text` from Stage 3 MUST NOT be used for transcript/ASR enrichment, thresholds, or evidence snippets.

### Video ID parsing
Parse `video_id` from URL using this order:
Pre-processing and parse rules (normative):
- Use `url = url.strip()` from validated candidate row input.
- Parse with `urllib.parse.urlparse`.
- Use `parsed.hostname` (lowercased) for host matching and ignore port.
- Scheme must be `http` or `https` and host must be present; otherwise fail `youtube_video_id_parse_failed`.
- Host matching is case-insensitive after lowercasing.
- If multiple query values exist for key `v`, use the first non-empty value in parsed order.
- For `/shorts/<id>`, `/embed/<id>`, and `youtu.be/<id>`, use only the first non-empty path segment after the prefix and ignore trailing segments.

1) host in `youtube.com|www.youtube.com|m.youtube.com` and URL has at least one non-empty query param `v` -> candidate id is first non-empty `v` in parsed order
2) host in `youtube.com|www.youtube.com|m.youtube.com` and path matches `/shorts/<id>` -> candidate id is `<id>`
3) host is `youtu.be` and path `/<id>` -> candidate id is `<id>`
4) host in `youtube.com|www.youtube.com|m.youtube.com` and path matches `/embed/<id>` -> candidate id is `<id>`
5) candidate id is valid only if it matches `^[A-Za-z0-9_-]{11}$`; otherwise fail `youtube_video_id_parse_failed`
6) otherwise fail with `youtube_video_id_parse_failed`

### Transcript + ASR flow
Transcript language policy (normative):
- Preferred transcript languages, in order: `["en", "en-US", "en-GB"]`.
- Transcript fetch logic must be deterministic:
  1) attempt manually-created transcript using preferred languages in order,
  2) if not available, attempt auto-generated transcript using the same preferred-language order.
- Do not use language auto-detection, locale-dependent defaults, or random ordering.
- Counter unit for cap accounting is item-level:
  - `youtube_transcripts_attempted` increments exactly once per selected YouTube item when transcript adapter lookup is invoked.
  - Manual-first then auto-generated fallback may perform multiple internal provider requests, but counts as one attempted item.

Transcript provider contract (normative):
- Transcript fetch must be normalized to exactly one status: `success` | `unavailable` | `failed`.
- `success`: transcript payload exists and normalized joined text is non-empty.
- `unavailable`: transcript not provided by source for this video/language, video unavailable/disabled, or payload normalizes to empty text.
- `failed`: transport/parsing/unexpected provider error.
- Transcript text assembly is normative:
  - Preserve provider segment order.
  - Collect only segment `text` values that are strings and non-empty after `strip()`.
  - Join collected segment texts with a single space (`" "`) before `normalize_text`.
  - If payload exists but collected text list is empty, classify as `unavailable`.
  - If payload shape cannot be iterated/extracted safely (for example missing expected segment structure), classify as `failed`.

Transcript status mapping rule (normative):
- Build status by adapter logic; do not branch directly on raw provider exceptions in runner code.
- Map to `unavailable` when either:
  - provider returns no matching transcript for the language policy, or
  - provider indicates transcript-disabled/video-unavailable conditions, or
  - transcript payload exists but normalized joined text is empty.
- Map to `failed` when either:
  - network/transport/request/rate-limit/provider parsing errors occur, or
  - any exception does not match the explicit `unavailable` conditions above.
- Exception handling rule:
  - classify using provider exception type (or class name) only; do not parse human-readable exception messages.
- youtube-transcript-api exception class-name mapping (normative, when available):
  - map to `unavailable` for class names:
    - `NoTranscriptFound`
    - `NoTranscriptAvailable`
    - `TranscriptsDisabled`
    - `VideoUnavailable`
    - `InvalidVideoId`
  - map all other provider exceptions to `failed`.
  - if an exception class name is unknown in a future library version, treat it as `failed`.

1) Before transcript API call:
   - if `youtube_stop_due_to_transcript_cap` is true, fail `transcript_cap_reached`
   - else if `youtube_transcripts_attempted >= max_transcripts`, set `youtube_stop_due_to_transcript_cap=true`, fail `transcript_cap_reached`
2) Attempt transcript via youtube-transcript-api.
   - increment `youtube_transcripts_attempted` exactly once for the selected YouTube item when transcript adapter lookup starts
3) If transcript status is `success`:
   - normalize text via `normalize_text`
   - set `enrichment_method = "yt_transcript_api"`
   - increment `youtube_transcripts_succeeded` by 1 (retrieval success), even if the item later fails `youtube_text_too_short`
4) If transcript status is `unavailable`:
   - if `max_asr == 0`, fail `youtube_transcript_unavailable`
   - else if `youtube_stop_due_to_asr_cap` is true, fail `asr_cap_reached`
   - else if `asr_fallbacks_used >= max_asr`, set `youtube_stop_due_to_asr_cap=true`, fail `asr_cap_reached`
   - else run ASR fallback (below), increment `asr_fallbacks_used` when ASR is invoked
   - on ASR success: normalize ASR text via `normalize_text` and set `enrichment_method = "asr_faster_whisper"`
5) If transcript status is `failed`, fail `youtube_transcript_failed` (no ASR fallback).
6) If normalized text length `< 800`, fail `youtube_text_too_short`.

ASR fallback requirements:
- ASR method is `faster-whisper`.
- Local toolchain must be available to obtain audio from YouTube URL:
  - `yt-dlp` executable on PATH
  - `ffmpeg` executable on PATH
- ASR decoding parameters are fixed for deterministic behavior:
  - model: `"small"`
  - language: `"en"`
  - beam_size: `5`
  - temperature: `0.0`
  - condition_on_previous_text: `False`
  - vad_filter: `True`
- ASR failure conditions (all map to `youtube_asr_failed`):
  - prerequisites missing
  - audio download/extraction fails
  - transcription execution fails
  - ASR returns empty/whitespace-only text

ASR counter rule (normative):
- Increment `asr_fallbacks_used` exactly once per item, immediately before launching the ASR transcription step.
- If prerequisites are missing before ASR starts, fail with `youtube_asr_failed` and do not increment `asr_fallbacks_used`.

YouTube stop behavior after cap reached:
- If transcript cap is reached once, all subsequent selected YouTube rows fail with `transcript_cap_reached`.
- If ASR cap is reached once, all subsequent selected YouTube rows fail with `asr_cap_reached`.
- Newsletter rows continue to be processed.

Dependency availability policy (normative):
- Required Python dependencies for Stage 4 startup:
  - `trafilatura`
  - `youtube-transcript-api`
- If either required dependency cannot be imported, fail the run with exit code `2`.
- ASR dependencies are checked only when ASR fallback is about to run:
  - Python package: `faster-whisper`
  - executables on PATH: `yt-dlp`, `ffmpeg`
- Missing ASR dependencies must map to per-item failure reason `youtube_asr_failed` and must not be treated as a fatal run error.

Cap edge-case clarification (normative):
- If `max_transcripts == 0`, Stage 4 must not call transcript API for any item.
- In that case, every selected YouTube row fails with `transcript_cap_reached`, and ASR is not attempted even when `max_asr > 0`.

YouTube evidence snippets:
- For transcript API success:
  - derive snippets from the same filtered, ordered segment list used by transcript text assembly (`text` is string and non-empty after `strip()`)
  - use first 3 transcript segments from that list whose `text` value remains non-empty after `normalize_text`
  - snippet text is `normalize_text(segment_text)` truncated to max 240 chars
  - `meta.type = "transcript"`
  - `meta.timestamp` uses segment `start` value formatted with `"{float(start):.3f}"` when conversion to float succeeds, else `null`
  - `meta.offset = null`
- For ASR success:
  - deterministic windows from normalized text:
    - `[0:240]`, `[240:480]`, `[480:720]` (include only non-empty)
  - `meta.type = "transcript"`, `meta.timestamp = null`, `meta.offset` equals window start

Failure reasons:
- `invalid_candidate_row`
- `youtube_video_id_parse_failed`
- `youtube_transcript_unavailable`
- `youtube_transcript_failed`
- `youtube_asr_failed`
- `youtube_text_too_short`
- `transcript_cap_reached`
- `asr_cap_reached`

Failure reason mapping note:
- `youtube_transcript_unavailable` is used only when transcript status is `unavailable` and ASR fallback is disabled (`max_asr == 0`).

Unexpected per-item exception fallback (normative):
- Runner must guard each selected item with per-item exception handling and continue processing subsequent items.
- Scope limitation: this fallback applies only to candidate validation/parsing and source enrichment logic; SQLite errors and artifact write errors are excluded and remain fatal per runtime I/O policy.
- Any uncaught per-item exception must be mapped to exactly one existing failure reason (do not add ad-hoc keys):
  - candidate row validation/parsing path -> `invalid_candidate_row`
  - newsletter branch (after candidate validation) -> `newsletter_extract_failed`
  - youtube branch (after candidate validation) -> `youtube_transcript_failed`
- Uncaught per-item exceptions must not terminate the run.

## HTTP Settings (Newsletter Fetch)
Use `http.*` from `pipeline.yaml`:
- `user_agent`
- `connect_timeout_s`, `read_timeout_s`
- `max_response_mb`
- `retries.max_attempts`

Retry classes must match Stage 1:
- max bytes cap is `http.max_response_mb * 1024 * 1024`
- retry on network errors (`requests.ConnectionError`, `requests.Timeout`), HTTP 429, HTTP 5xx, and status codes `>= 600`
- do not retry on other 4xx
- enforce response-size cap with Stage 4 dual-check policy:
  - if parseable `Content-Length` is present and exceeds cap, fail before body read
  - always enforce streaming byte cap during body read, regardless of whether `Content-Length` is present

## SQLite Persistence (`enriched_items` table)
Create table if not exists:
- `item_id` TEXT PRIMARY KEY
- `source_type` TEXT NOT NULL
- `url` TEXT NOT NULL
- `title` TEXT NOT NULL
- `published_at` TEXT NOT NULL
- `enriched_text` TEXT NOT NULL
- `evidence_snippets` TEXT NOT NULL (JSON string)
- `enrichment_method` TEXT NOT NULL
- `enriched_at` TEXT NOT NULL
- `inserted_at` TEXT NOT NULL

Insert rules:
- Use `INSERT OR IGNORE`.
- If insert succeeds:
  - count as `inserted_db += 1`
  - emit row to output JSONL
- If insert ignored:
  - count as `skipped_already_enriched += 1`
  - do not emit row
- `skipped_already_enriched` semantics:
  - counts only insert-ignore conflicts for items selected this run.
  - in normal single-process sequential reruns, expected value is `0` because selection excludes rows already present in `enriched_items`.

Timestamp binding rule:
- For each successful item, compute one UTC timestamp string at second precision (`YYYY-MM-DDTHH:MM:SSZ`).
- Use that same value for JSONL `enriched_at`, DB `enriched_at`, and DB `inserted_at`.

Do not insert failed enrichments.

## Logging
- INFO: run start/end, totals, cap usage
- WARNING: aggregated failure breakdown
- ERROR: per-item failure with short exception summary

## Stage 4 Report (JSON)
Write report to default `{outputs_dir}/stage_4_report_<YYYY-MM-DD>.json` unless overridden.

Required fields:
- `run_id` (UUID4 string)
- `run_status` (`completed` | `fatal`)
- `fatal_error` (string|null)
- `started_at` (UTC second-precision ISO8601 Z)
- `finished_at` (UTC second-precision ISO8601 Z)
- `db_path`
- `output_path`
- `report_path`
- `candidates_available_total`
- `items_selected`
- `success_count`
- `failed_count`
- `inserted_db`
- `skipped_already_enriched`
- `youtube_transcripts_attempted`
- `youtube_transcripts_succeeded`
- `asr_fallbacks_used`
- `max_items`
- `max_transcripts`
- `max_asr`
- `fail_breakdown` map with keys:
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
- Normative: all listed `fail_breakdown` keys MUST be present on every run with non-boolean integer values `>= 0` (use zero when absent).

Run status semantics (normative):
- completed run (`exit code 0`): `run_status = "completed"` and `fatal_error = null`
- fatal run (`exit code 2`) with report write success: `run_status = "fatal"` and `fatal_error` is a non-empty string summary
- for fatal reports, all required numeric counters and `fail_breakdown` keys must still be present with non-boolean integer values `>= 0`; use `0` when a value is unavailable due to early fatal termination
- explicit pre-config fatal defaulting: when pipeline defaults are unavailable, `max_items`, `max_transcripts`, and `max_asr` MUST still be present; use validated CLI override values when provided, otherwise `0`
- if fatal occurs before non-report paths are known, `db_path` and `output_path` may be emitted as empty strings in the fatal report
- counter invariants below MUST hold for both `completed` and `fatal` reports.

Counter definitions:
- `items_selected`: selected rows that reached a terminal per-item outcome before run end (one `success` or one mapped failure reason per item).
- `success_count`: items that passed enrichment logic and completed DB insert attempt with non-fatal outcome (`inserted` or `insert ignored`).
- `failed_count`: items that failed enrichment logic (exactly one failure reason per failed item).
- `youtube_transcripts_succeeded`: YouTube items whose transcript adapter status is `success` (retrieval success before the `< 800` length gate).

Counter invariants (normative):
- `items_selected = success_count + failed_count`
- `success_count = inserted_db + skipped_already_enriched`
- `failed_count = sum(fail_breakdown.values())`
- `items_selected <= max_items`
- `items_selected <= candidates_available_total`
- `youtube_transcripts_succeeded <= youtube_transcripts_attempted`
- `youtube_transcripts_attempted <= max_transcripts`
- `asr_fallbacks_used <= max_asr`

## Outputs
- JSONL: `data/outputs/enriched_items_<YYYY-MM-DD>.jsonl` (inserted successes only)
- JSON: `data/outputs/stage_4_report_<YYYY-MM-DD>.json`
- SQLite: `enriched_items` table created/updated in configured DB

## Tests (pytest) - Minimum
Add `tests/test_stage_4_enrich.py` with temp SQLite DB and local fixtures/mocks:
1) Newsletter extraction with local HTML fixture (no network)
- extraction succeeds and normalized text threshold is enforced

2) YouTube transcript success path with mocked transcript API (no network)
- transcript counters and enrichment method are correct

3) Video ID parsing coverage
- `watch?v=...`
- `/shorts/...`
- `youtu.be/...`
- `/embed/...`
- invalid URL -> `youtube_video_id_parse_failed`

4) Idempotency and emit-on-insert
- pre-seed `enriched_items`, rerun, ensure pre-seeded rows are not selected, `skipped_already_enriched == 0` (sequential run), and JSONL excludes pre-seeded items

5) Cap behavior
- transcript cap reached causes remaining YouTube rows to fail with `transcript_cap_reached`
- ASR cap reached causes remaining YouTube rows to fail with `asr_cap_reached`
- newsletter rows still process

6) Report invariants
- assert all counter invariants in report payload

7) Threshold edge cases
- newsletter at 499/500 chars
- youtube at 799/800 chars

8) ASR fallback failure path
- missing prerequisite or ASR exception maps to `youtube_asr_failed`

9) Invalid candidate row handling
- unsupported `source_type` maps to `invalid_candidate_row`
- non-ISO `published_at` maps to `invalid_candidate_row`
- non-integer/boolean/negative `relevance_score` maps to `invalid_candidate_row`

10) Transcript mapping + language policy
- verify transcript fetch tries languages in order `["en", "en-US", "en-GB"]` with manual-first then auto-generated fallback
- verify `NoTranscriptFound`/`NoTranscriptAvailable`/`TranscriptsDisabled` map to `unavailable`
- verify non-mapped provider exception maps to `failed`

11) `max_transcripts == 0` edge case
- all selected YouTube rows fail with `transcript_cap_reached`
- transcript API is never called
- ASR is not attempted even if `max_asr > 0`

12) Pipeline config validation
- invalid/missing `caps.max_transcripts_per_run`, `caps.max_asr_fallbacks_per_run`, or `http.*` values exit with code 2

13) Transcript payload text assembly
- mixed transcript segments (empty/non-string/string) produce deterministic joined text from valid string segments only
- payload with no usable segment text maps to `unavailable`

14) YouTube URL parsing edge cases
- uppercase host is accepted after lowercasing
- multiple `v` params use first non-empty value
- extra trailing path segments after `/shorts/<id>` and `/embed/<id>` are ignored for id extraction

15) ASR deterministic parameters
- ASR invocation uses fixed decoding parameters from spec (model/language/beam/temperature/vad/condition flags)

16) Fatal artifact/report behavior
- simulate a fatal output/report I/O condition and verify exit code `2`
- verify JSONL may be partial on fatal termination (no rollback requirement)
- when report write succeeds on fatal path, verify `run_status = "fatal"` and non-empty `fatal_error`

17) Pre-config fatal report behavior
- invalid/unreadable pipeline with no `--report` override exits `2` and may produce no report file
- invalid/unreadable pipeline with `--report <path>` must attempt fatal report write at that path
- in that fatal report, required numeric counters and `fail_breakdown` keys are present (use `0` when unavailable), and `db_path`/`output_path` may be empty strings

18) Fatal mid-run counter semantics
- when a fatal error occurs after selection begins (for example SQLite write failure), `items_selected` counts only terminal per-item outcomes reached before the fatal stop
- report still satisfies all counter invariants

Tests must not use network calls.

## Files Changed (Expected)
- `app/enrich/*` (new)
- `tests/test_stage_4_enrich.py` (new)

## Commands to Run (Expected)
- `python -m app.enrich.cli --pipeline config/pipeline.yaml`
- `pytest -q tests/test_stage_4_enrich.py`

## Produced Artifacts
- `data/outputs/enriched_items_<YYYY-MM-DD>.jsonl`
- `data/outputs/stage_4_report_<YYYY-MM-DD>.json`
- `{paths.sqlite_db}` with table `enriched_items` created/updated
