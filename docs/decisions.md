# Decisions (Locked) — AI Content Research & Idea Engine

## Goal
Build a manual, batch-first content intelligence engine for the niche:
"AI + automations to make money".

The system ingests content from curated sources, enriches it, extracts insights, scores quality/virality, generates short-form script drafts, and logs results for review.

## Execution Mode
- Manual batch only (no scheduler initially).
- Primary command (happy path):
  - `python -m app.main --run daily`

## Config Structure (3 locations)
1) `config/sources.yaml`
- Curated sources list (newsletters RSS, YouTube channel IDs)
- Optional tags per source

2) `config/pipeline.yaml`
- Run knobs: caps, timeouts, retry policy, paths, recency window
- Later extended with scoring thresholds and model settings

3) `config/prompts/`
- Prompt templates split by task (extract, score, generate)
- One file per task to avoid mega prompts and reduce token usage

## Storage (Local Only)
System-of-record:
- SQLite: `data/state.db`
- Planned tables:
  - `seen_items` (dedup for ingestion)
  - `items` (canonical + enriched content, later)
  - `ideas` (extracted/scored/generated outputs, later)
  - `runs` (run metadata, later)

Debug/demo artifacts:
- Folder: `data/outputs/`
- Run-stamped files:
  - `raw_items_<YYYY-MM-DD>.jsonl`
  - `enriched_items_<YYYY-MM-DD>.jsonl` (later)
  - `run_report_<YYYY-MM-DD>.json`

SQLite is the truth. JSONL is for inspection and Loom demos.

## Scale Caps (Predictable Manual Runs)
Target daily manual run limits:
- Newsletter sources: 10–20
- YouTube channels: 10–20
- Discovery cap per source per run: 50 entries
- Transcript enrich cap per run: 10 videos
- Local ASR fallback cap per run: 3 videos

## Library Choices (Locked)
Ingestion + plumbing:
- feedparser
- requests
- tenacity
- python-dateutil
- PyYAML
- stdlib: sqlite3, logging, hashlib, json, uuid, pathlib

## Newsletter page text extraction:
- trafilatura

## YouTube transcript:
- youtube-transcript-api

## ASR fallback:
- faster-whisper

## LLM calls:
- openai SDK (model configurable in config later)

## Explicitly out of scope for base build:
- Zapier/n8n/Make.com
- scraping restricted platforms as a dependency