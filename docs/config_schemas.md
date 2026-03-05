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

llm:
  provider: "openai"
  model: "gpt-4o-mini"
  temperature: 0.2
  max_output_tokens: 900
  requests_per_minute_soft: 30
  request_timeout_s: 60
  retry_max_attempts: 3
  retry_backoff_initial_s: 1.0
  retry_backoff_multiplier: 2.0
  retry_backoff_max_s: 8.0
  api_key_env_var: "OPENAI_API_KEY"
  api_key: ""

stage_5_intelligence:
  max_items_default: 25
  input_max_chars: 12000

stage_6_generate:
  max_items_default: 25

sheets:
  enabled: true
  spreadsheet_id: "<google_sheet_id>"
  worksheet_name: "Ideas"
  key_column: "item_id"
  header_row: 1

stage_7_persist:
  max_rows_default: 200

deliver:
  enabled: true
  channel: "slack"
  slack_webhook_url: "https://hooks.slack.com/services/T00000000/B00000000/REPLACE_ME"
  max_items_per_run: 10
  max_script_chars: 1200
  min_viral_rating: 5
  include_only_status: []
  dry_run: false
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

### `llm` Validation Rules
- `provider`: non-empty string; currently only `openai` is allowed
- `model`: non-empty string
- `temperature`: non-boolean number between `0` and `2` inclusive
- `max_output_tokens`: non-boolean integer >= `1`
- `requests_per_minute_soft`: non-boolean integer >= `1`
- `request_timeout_s`: non-boolean integer >= `1`
- `retry_max_attempts`: non-boolean integer >= `1`
- `retry_backoff_initial_s`: non-boolean number > `0`
- `retry_backoff_multiplier`: non-boolean number >= `1`
- `retry_backoff_max_s`: non-boolean number >= `retry_backoff_initial_s`
- `api_key_env_var`: non-empty string
- `api_key`: string (may be empty)

### `stage_5_intelligence` Validation Rules
- `max_items_default`: non-boolean integer >= `0`
- `input_max_chars`: non-boolean integer >= `1`

### `stage_6_generate` Validation Rules
- `max_items_default`: non-boolean integer >= `0`

### `sheets` Validation Rules
- `enabled`: boolean
- if `enabled` is `true`:
  - `spreadsheet_id`: non-empty string after `strip()`
  - `worksheet_name`: non-empty string after `strip()`
  - `key_column`: non-empty string after `strip()`
  - `header_row`: non-boolean integer >= `1`
- if `enabled` is `false`:
  - `spreadsheet_id`, `worksheet_name`, `key_column`, and `header_row` are optional

### `stage_7_persist` Validation Rules
- `max_rows_default`: non-boolean integer >= `0`

### `deliver` Validation Rules
- `enabled`: boolean
- `channel`: non-empty string after `strip()`, must be `slack`
- if `enabled` is `true`:
  - `slack_webhook_url`: non-empty string after `strip()`, must start with:
    - `https://hooks.slack.com/services/`
    - `https://hooks.slack-gov.com/services/`
- `max_items_per_run`: non-boolean integer >= `0`
- `max_script_chars`: non-boolean integer >= `80`
- `min_viral_rating`: null or non-boolean integer in `[1, 10]`
- `include_only_status`: list of strings and must be empty (`[]`) for Stage 8
- `dry_run`: boolean
