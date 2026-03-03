# Config Schemas

## 1) config/sources.yaml

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

### YouTube feed URL is constructed from channel_id:
https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}