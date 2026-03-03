from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    source_type: str
    feed_url: str
    tags: tuple[str, ...]


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int


@dataclass(frozen=True)
class HttpConfig:
    user_agent: str
    connect_timeout_s: int
    read_timeout_s: int
    max_response_mb: int
    retries: RetryConfig

    @property
    def max_response_bytes(self) -> int:
        return int(self.max_response_mb * 1024 * 1024)


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class PipelineConfig:
    recency_days: int | None
    max_entries_per_source: int
    http: HttpConfig
    paths: PathsConfig


def load_sources_config(path: str | Path) -> list[Source]:
    data = _load_yaml(path)
    newsletters = data.get("newsletters", [])
    youtube = data.get("youtube", [])
    if not isinstance(newsletters, list) or not isinstance(youtube, list):
        raise ConfigError("newsletters and youtube must be lists")

    source_ids: set[str] = set()
    sources: list[Source] = []

    for row in newsletters:
        source_id = _require_str(row, "id")
        if source_id in source_ids:
            raise ConfigError(f"duplicate source id: {source_id}")
        source_ids.add(source_id)
        feed_url = _require_str(row, "feed_url")
        if not (feed_url.startswith("http://") or feed_url.startswith("https://")):
            raise ConfigError(f"invalid newsletter feed_url for {source_id}: {feed_url}")
        sources.append(
            Source(
                id=source_id,
                name=_require_str(row, "name"),
                source_type="newsletter",
                feed_url=feed_url,
                tags=tuple(_require_tags(row.get("tags"))),
            )
        )

    for row in youtube:
        source_id = _require_str(row, "id")
        if source_id in source_ids:
            raise ConfigError(f"duplicate source id: {source_id}")
        source_ids.add(source_id)
        channel_id = _require_str(row, "channel_id")
        if not channel_id.strip():
            raise ConfigError(f"empty youtube channel_id for {source_id}")
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        sources.append(
            Source(
                id=source_id,
                name=_require_str(row, "name"),
                source_type="youtube",
                feed_url=feed_url,
                tags=tuple(_require_tags(row.get("tags"))),
            )
        )

    return sources


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    data = _load_yaml(path)

    run_mode = data.get("run_mode", {})
    caps = data.get("caps", {})
    http = data.get("http", {})
    retries = http.get("retries", {})
    paths = data.get("paths", {})

    recency_days = run_mode.get("recency_days")
    if recency_days is not None:
        recency_days = int(recency_days)
        if recency_days < 0:
            raise ConfigError("run_mode.recency_days must be >= 0")

    max_entries_per_source = int(caps.get("max_entries_per_source"))
    if max_entries_per_source <= 0:
        raise ConfigError("caps.max_entries_per_source must be > 0")

    return PipelineConfig(
        recency_days=recency_days,
        max_entries_per_source=max_entries_per_source,
        http=HttpConfig(
            user_agent=str(http.get("user_agent")),
            connect_timeout_s=int(http.get("connect_timeout_s")),
            read_timeout_s=int(http.get("read_timeout_s")),
            max_response_mb=int(http.get("max_response_mb")),
            retries=RetryConfig(max_attempts=int(retries.get("max_attempts"))),
        ),
        paths=PathsConfig(
            sqlite_db=str(paths.get("sqlite_db")),
            outputs_dir=str(paths.get("outputs_dir")),
        ),
    )


def _load_yaml(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"missing config file: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"top-level config must be a mapping: {path}")
    return raw


def _require_str(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    return value.strip()


def _require_tags(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError("tags must be a list of strings")
    tags: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            raise ConfigError("tags must be a list of strings")
        tags.append(entry)
    return tags
