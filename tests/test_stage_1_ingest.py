import json
from pathlib import Path

from app.ingest.fetch import FetchError, FetchResponse
from app.ingest.runner import run_ingestion


RSS_FEED = b"""<?xml version='1.0' encoding='UTF-8'?>
<rss version='2.0'>
  <channel>
    <title>Sample Feed</title>
    <item>
      <guid>item-1</guid>
      <title>First Item</title>
      <link>https://example.com/item-1</link>
      <pubDate>Tue, 03 Mar 2026 12:00:00 GMT</pubDate>
      <description>Summary one</description>
      <author>Author One</author>
    </item>
    <item>
      <guid>item-2</guid>
      <title>Second Item</title>
      <link>https://example.com/item-2</link>
      <pubDate>Tue, 03 Mar 2026 13:00:00 GMT</pubDate>
      <description>Summary two</description>
      <author>Author Two</author>
    </item>
  </channel>
</rss>
"""


def _write_sources(path: Path) -> None:
    path.write_text(
        """
newsletters:
  - id: "news_ok"
    name: "News Source"
    feed_url: "https://feeds.example.com/news_ok.xml"
youtube:
  - id: "yt_ok"
    name: "YT Source"
    channel_id: "UC123"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_pipeline(path: Path, db_path: Path, outputs_dir: Path) -> None:
    path.write_text(
        f"""
run_mode:
  manual: true
  recency_days: 365

caps:
  max_entries_per_source: 50

http:
  user_agent: "test-agent"
  connect_timeout_s: 1
  read_timeout_s: 1
  max_response_mb: 2
  retries:
    max_attempts: 1

paths:
  sqlite_db: "{db_path.as_posix()}"
  outputs_dir: "{outputs_dir.as_posix()}"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def test_stage_1_idempotency_second_run_emits_zero(monkeypatch, tmp_path: Path) -> None:
    sources = tmp_path / "sources.yaml"
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "raw_items.jsonl"
    report_path = tmp_path / "run_report.json"

    _write_sources(sources)
    _write_pipeline(pipeline, db_path, tmp_path)

    monkeypatch.setattr(
        "app.ingest.runner.fetch_feed",
        lambda url, http_config: FetchResponse(body=RSS_FEED),
    )

    first = run_ingestion(
        sources_path=str(sources),
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    assert first.total_new_items_emitted == 4
    assert len(_read_jsonl(out_path)) == 4

    second = run_ingestion(
        sources_path=str(sources),
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    assert second.total_new_items_emitted == 0
    assert _read_jsonl(out_path) == []


def test_stage_1_failure_isolation_continues_other_sources(monkeypatch, tmp_path: Path) -> None:
    sources = tmp_path / "sources.yaml"
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "raw_items.jsonl"
    report_path = tmp_path / "run_report.json"

    _write_sources(sources)
    _write_pipeline(pipeline, db_path, tmp_path)

    def _fake_fetch(url: str, http_config):
        if "news_ok" in url:
            raise FetchError("boom")
        return FetchResponse(body=RSS_FEED)

    monkeypatch.setattr("app.ingest.runner.fetch_feed", _fake_fetch)

    result = run_ingestion(
        sources_path=str(sources),
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.sources_failed == 1
    assert result.total_new_items_emitted == 2

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["sources_failed"] == 1
    assert report["sources_succeeded"] == 1
    assert len(report["per_source"]) == 2
    assert any(row["error"] for row in report["per_source"])
    assert any((row["error"] is None and row["new_items_emitted"] == 2) for row in report["per_source"])

    emitted = _read_jsonl(out_path)
    assert len(emitted) == 2


def test_stage_1_raw_item_schema_has_required_fields_and_z_timestamps(monkeypatch, tmp_path: Path) -> None:
    sources = tmp_path / "sources.yaml"
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "raw_items.jsonl"
    report_path = tmp_path / "run_report.json"

    _write_sources(sources)
    _write_pipeline(pipeline, db_path, tmp_path)

    monkeypatch.setattr(
        "app.ingest.runner.fetch_feed",
        lambda url, http_config: FetchResponse(body=RSS_FEED),
    )

    run_ingestion(
        sources_path=str(sources),
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    required = {
        "source_type",
        "source_id",
        "source_name",
        "creator",
        "title",
        "url",
        "published_at",
        "external_id",
        "summary",
        "fetched_at",
    }

    emitted = _read_jsonl(out_path)
    assert emitted
    for item in emitted:
        assert required.issubset(item.keys())
        assert item["published_at"].endswith("Z")
        assert item["fetched_at"].endswith("Z")
