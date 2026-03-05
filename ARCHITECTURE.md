# Pipeline Flowchart (ASCII)

Run command:
`python -m app.main --run daily`

## 1) End-to-End Program Flow

```text
+------------------------------+
| Manual trigger              |
| python -m app.main --run... |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 0: Load config         |
| - config/pipeline.yaml       |
| - config/sources.yaml        |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 9: Orchestrator        |
| Runs Stage 1 -> Stage 8      |
| in strict sequence           |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 1: Ingest              |
| Discover new RSS/YT entries  |
| Dedup by seen_items          |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 2: Normalize           |
| Canonical schema + validate  |
| Persist -> items             |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 3: Filter              |
| Cheap quality/relevance gate |
| Persist pass -> candidates   |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 4: Enrich              |
| Newsletter text + YT tx/ASR  |
| Persist -> enriched_items    |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 5: Intelligence        |
| LLM extract + score + hooks  |
| Persist -> ideas             |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 6: Generate            |
| LLM script drafting          |
| Persist -> scripts           |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 7: Persist Sheet       |
| Upsert ideas/scripts to      |
| Google Sheets                |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 8: Deliver             |
| Send Slack digest            |
| Persist -> deliveries        |
+--------------+---------------+
               |
               v
+------------------------------+
| Stage 9: Finalize            |
| Aggregate stage reports      |
| Write final_report_YYYY...   |
+--------------+---------------+
               |
               v
          +---------+
          |  Done   |
          +---------+
```

## 2) Data/System Interaction Flow

```text
    [External Feeds] ----------> [Stage 1]
    [Newsletter Pages] ---------> [Stage 4]
    [YouTube Transcript/ASR] ---> [Stage 4]
    [LLM Provider] -------------> [Stage 5]
    [LLM Provider] -------------> [Stage 6]
    [Google Sheets] <----------- [Stage 7]
    [Slack Webhook] <----------- [Stage 8]

    [Stage 1] ----\
    [Stage 2] -----\
    [Stage 3] ------\
    [Stage 4] -------+-----> [SQLite: data/state_stage1_refresh.db]
    [Stage 5] ------/
    [Stage 6] -----/
    [Stage 8] ----/

    [Stage 1] ----\
    [Stage 2] -----\
    [Stage 3] ------\
    [Stage 4] -------\
    [Stage 5] --------+-----> [data/outputs/*.jsonl, stage_*_report_*.json]
    [Stage 6] -------/
    [Stage 7] ------/
    [Stage 8] -----/
    [Stage 9] ----/          + [data/outputs/final_report_YYYY-MM-DD.json]
```

## 3) Control Flow (Stop-After and Fatal)

```text
        +---------------------------+
        | Start stage loop (1..8)  |
        +------------+--------------+
                     |
                     v
        +---------------------------+
        | stop-after reached?       |
        +--------+------------------+
                 |Yes
                 v
        +---------------------------+
        | Mark remaining as skipped |
        | pipeline_status=stopped   |
        | exit code 0               |
        +---------------------------+
                 ^
                 |
                 |No
                 |
        +--------+------------------+
        | Run current stage         |
        +--------+------------------+
                 |
                 v
        +---------------------------+
        | Stage fatal?              |
        | exception or run_status   |
        +--------+------------------+
                 |Yes
                 v
        +---------------------------+
        | Mark current failed       |
        | Mark downstream skipped   |
        | pipeline_status=failed    |
        | write final report        |
        | exit code 2               |
        +---------------------------+
                 ^
                 |
                 |No
                 |
        +--------+------------------+
        | Last stage completed?     |
        +--------+------------------+
                 |No --> back to loop
                 |
                 |Yes
                 v
        +---------------------------+
        | pipeline_status=completed |
        | write final report        |
        | exit code 0               |
        +---------------------------+
```
