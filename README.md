# AI Content Research & Generation Pipeline

Manual, deterministic batch pipeline that discovers content, enriches it, extracts intelligence, generates scripts, and delivers outputs for review and distribution.

Primary run command:

```bash
python -m app.main --run daily
```

## What This System Does

- Ingests newsletter and YouTube sources from curated config.
- Normalizes and stores canonical records in SQLite.
- Filters to high-signal candidates.
- Enriches candidates with full text/transcript evidence.
- Uses LLM calls for intelligence extraction/scoring and script generation.
- Persists review-ready rows to Google Sheets.
- Sends top scripts to Slack.
- Produces stage reports and one final run report.

## Pipeline Stages

| Stage | Name | Purpose |
|---|---|---|
| 0 | Bootstrap | Load/validate pipeline and source config |
| 1 | Ingest | Discover new RSS/YouTube items and deduplicate |
| 2 | Normalize | Canonicalize item schema and persist to `items` |
| 3 | Filter | Apply cheap quality/relevance gates, persist to `candidates` |
| 4 | Enrich | Fetch newsletter text and YouTube transcript/ASR into `enriched_items` |
| 5 | Intelligence | LLM extract + score + hooks into `ideas` |
| 6 | Generate | LLM script generation into `scripts` |
| 7 | Persist Sheet | Upsert rows into Google Sheets |
| 8 | Deliver | Post selected scripts to Slack and record deliveries |
| 9 | Ops | Orchestrate stages 1-8 and write final consolidated report |

Flowchart version is available in [ARCHITECTURE.md](ARCHITECTURE.md).

## Tech Stack

- Python + SQLite
- YAML config (`config/pipeline.yaml`, `config/sources.yaml`)
- HTTP/parsing: `requests`, `feedparser`, `trafilatura`, `youtube-transcript-api`
- LLM: `openai`
- ASR fallback: `faster-whisper`
- Integrations: Google Sheets (`gspread`), Slack webhook

## Project Layout

```text
app/
  ingest/ normalize/ filter/ enrich/ intelligence/ generate/ sheets/ deliver/
  main.py
config/
  pipeline.yaml
  sources.yaml
  prompts/
docs/
tests/
data/
  outputs/
```

## Prerequisites

- Python 3.10+
- Pip
- Network access for external APIs/sources
- Optional for Stage 7: Google service account credentials
- Optional for Stage 5/6: OpenAI API key
- Optional for Stage 8: valid Slack webhook

## Setup

### 1) Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Configure inputs and runtime

- Edit `config/sources.yaml` with your newsletter + YouTube source lists.
- Review `config/pipeline.yaml` caps, paths, and stage settings.
- For local secrets, prefer a local non-committed pipeline copy (for example `config/pipeline.local.yaml`).

Optional environment variables:

```powershell
$env:OPENAI_API_KEY="your_key_here"
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service_account.json"
```

## Run the Full Pipeline

Default:

```bash
python -m app.main --run daily
```

Explicit config/report paths:

```bash
python -m app.main --run daily --pipeline config/pipeline.yaml --sources config/sources.yaml --report data/outputs/final_report_YYYY-MM-DD.json
```

Stop after a stage:

```bash
python -m app.main --run daily --stop-after 4
```

Optional markdown summary:

```bash
python -m app.main --run daily --report-md data/outputs/final_report_YYYY-MM-DD.md
```

## Run Individual Stages

```bash
python -m app.ingest.cli --pipeline config/pipeline.yaml --config config/sources.yaml
python -m app.normalize.cli --pipeline config/pipeline.yaml
python -m app.filter.cli --pipeline config/pipeline.yaml
python -m app.enrich.cli --pipeline config/pipeline.yaml
python -m app.intelligence.cli --pipeline config/pipeline.yaml
python -m app.generate.cli --pipeline config/pipeline.yaml
python -m app.sheets.cli --pipeline config/pipeline.yaml
python -m app.deliver.cli --pipeline config/pipeline.yaml
```

## Outputs

Main outputs are written under `data/outputs/`:

- `raw_items_<YYYY-MM-DD>.jsonl`
- `canonical_items_<YYYY-MM-DD>.jsonl`
- `candidate_items_<YYYY-MM-DD>.jsonl`
- `enriched_items_<YYYY-MM-DD>.jsonl`
- `ideas_<YYYY-MM-DD>.jsonl`
- `scripts_<YYYY-MM-DD>.jsonl`
- `stage_<N>_report_<YYYY-MM-DD>.json`
- `final_report_<YYYY-MM-DD>.json`

State is stored in SQLite at path `paths.sqlite_db` in `config/pipeline.yaml`.

## Exit Codes (Stage 9)

- `0`: completed or intentionally stopped (`--stop-after`)
- `2`: fatal failure

## Testing

Run the full test suite:

```bash
pytest -q
```

Run a specific test module:

```bash
pytest -q tests/test_stage_7_persist_sheet.py
```

## Troubleshooting

- `ModuleNotFoundError: feedparser` or similar: install dependencies with `pip install -r requirements.txt`.
- LLM failures in Stage 5/6: verify `OPENAI_API_KEY` or `llm.api_key` in your pipeline file.
- Sheets failures in Stage 7: verify `GOOGLE_APPLICATION_CREDENTIALS` and sheet sharing permissions.
- Slack delivery failures in Stage 8: verify `deliver.slack_webhook_url` is valid and reachable.

## Reference Docs

- Decision baseline: `docs/decisions.md`
- Config contract: `docs/config_schemas.md`
- Stage specs: `docs/spec_stage_*.md` and `docs/spec-stage_09_ops.md`
