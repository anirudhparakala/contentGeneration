# Config Schemas

## 1) `config/sources.yaml`

### Purpose
Defines the curated source list for ingestion. Sources are not discovered automatically.

### Schema
```yaml
newsletters:
  - id: "str_unique"
    name: "Human readable name"
    feed_url: "https://..."
    tags: ["optional", "labels"]

youtube:
  - id: "str_unique"
    name: "Channel name"
    channel_id: "UC...."
    tags: ["optional", "labels"]
```

YouTube feed URLs are derived from:
`https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}`

## 2) `config/pipeline.yaml`

### Required Keys Used by Current Stages
```yaml
run_mode:
  manual: true
  recency_days: 30

caps:
  max_entries_per_source: 50
  max_transcripts_per_run: 10
  max_asr_fallbacks_per_run: 3

http:
  user_agent: "ai-content-engine/0.1 (+contact: local)"
  connect_timeout_s: 10
  read_timeout_s: 20
  max_response_mb: 5
  retries:
    max_attempts: 3

paths:
  sqlite_db: "data/state_stage1_refresh.db"
  outputs_dir: "data/outputs"

stage_3_filter:
  min_content_chars: 120
  min_relevance_score: 3
  max_candidates_default: 100
  keyword_groups:
    ai_automation:
      weight: 1
      terms: ["ai", "agent", "make.com", "openai"]
    monetization:
      weight: 2
      terms: ["revenue", "side hustle", "cold email", "freelanc"]
```

### `stage_3_filter` Validation Rules
- `min_content_chars`: integer >= 0
- `min_relevance_score`: integer >= 0
- `max_candidates_default`: integer >= 0
- `keyword_groups`: non-empty mapping
- each group:
  - `weight`: integer >= 1
  - `terms`: non-empty list of strings, with at least one non-empty string after `strip()`
  - normalization order for terms: `strip()` -> lowercase -> collapse internal whitespace runs to single spaces
  - wildcard grammar:
    - `*` is allowed only as a single trailing character
    - wildcard terms cannot contain spaces
    - base token before `*` must match `[a-z0-9]+(?:\.[a-z0-9]+)*`
  - non-wildcard grammar:
    - token term: no spaces and matches `[a-z0-9]+(?:\.[a-z0-9]+)*`
    - phrase term: has spaces, and each token matches `[a-z0-9]+(?:\.[a-z0-9]+)*`
  - invalid examples: leading/middle `*`, repeated `*`, or tokens like `ai-tools`
  - backward compatibility canonicalization: normalized `freelanc` is interpreted as `freelanc*`
