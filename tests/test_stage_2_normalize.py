import json
import sqlite3
from pathlib import Path

from app.normalize.runner import run_normalize


def _write_pipeline(path: Path, db_path: Path, outputs_dir: Path) -> None:
    path.write_text(
        f"""
paths:
  sqlite_db: \"{db_path.as_posix()}\"
  outputs_dir: \"{outputs_dir.as_posix()}\"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    text = "\n".join(json.dumps(row, ensure_ascii=True) for row in rows) + "\n"
    path.write_text(text, encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _db_item_count(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT COUNT(*) FROM items").fetchone()
        return int(row[0])
    finally:
        conn.close()


def _valid_raw_item(*, external_id: str) -> dict:
    return {
        "source_type": "youtube",
        "source_id": "channel-a",
        "source_name": "Channel A",
        "creator": "Host A",
        "title": f"Title {external_id}",
        "url": f"https://example.com/{external_id}",
        "published_at": "2026-03-03T10:15:30Z",
        "external_id": external_id,
        "summary": "short summary",
        "fetched_at": "2026-03-03T12:00:00Z",
    }


def test_stage_2_inserts_new_items_and_emits_canonical_jsonl(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    in_path = tmp_path / "raw_items.jsonl"
    out_path = tmp_path / "canonical_items.jsonl"
    report_path = tmp_path / "stage_2_report.json"

    _write_pipeline(pipeline, db_path, tmp_path)

    raw_1 = {
        "source_type": " YouTube ",
        "source_id": " channel-1 ",
        "source_name": " Creator Hub ",
        "creator": "   ",
        "title": "  A useful video  ",
        "url": " https://example.com/video-1 ",
        "published_at": "2026-03-03T10:15:30+05:30",
        "external_id": " ext-1 ",
        "summary": "  quick summary  ",
        "fetched_at": "2026-03-03T10:15:30",
    }
    raw_2 = {
        "title": "Untyped source item",
        "url": "https://example.com/news-1",
        "published_at": "2026-03-03",
        "external_id": "news-1",
        "fetched_at": "2026-03-03T11:00:00Z",
    }
    _write_jsonl(in_path, [raw_1, raw_2])

    result = run_normalize(
        pipeline_path=str(pipeline),
        in_path=str(in_path),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.canonical_items_emitted == 2
    assert result.items_inserted_db == 2
    assert result.items_skipped_already_present == 0
    assert result.items_skipped_invalid == 0

    emitted = _read_jsonl(out_path)
    assert len(emitted) == 2

    first = emitted[0]
    assert first["source_type"] == "youtube"
    assert first["source_id"] == "channel-1"
    assert first["creator"] == "Creator Hub"
    assert first["title"] == "A useful video"
    assert first["url"] == "https://example.com/video-1"
    assert first["published_at"] == "2026-03-03T04:45:30Z"
    assert first["fetched_at"] == "2026-03-03T10:15:30Z"
    assert first["external_id"] == "ext-1"
    assert first["summary"] == "quick summary"
    assert first["content_text"] == "quick summary"
    assert first["raw_item_json"] == raw_1

    second = emitted[1]
    assert second["source_type"] == "newsletter"
    assert second["source_id"] == "unknown"
    assert second["source_name"] == "unknown"
    assert second["creator"] == "unknown"

    assert _db_item_count(db_path) == 2


def test_stage_2_idempotency_second_run_skips_existing_but_emits_again(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    in_path = tmp_path / "raw_items.jsonl"
    out_path = tmp_path / "canonical_items.jsonl"
    report_path = tmp_path / "stage_2_report.json"

    _write_pipeline(pipeline, db_path, tmp_path)
    _write_jsonl(
        in_path,
        [
            _valid_raw_item(external_id="item-1"),
            _valid_raw_item(external_id="item-2"),
        ],
    )

    first = run_normalize(
        pipeline_path=str(pipeline),
        in_path=str(in_path),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    second = run_normalize(
        pipeline_path=str(pipeline),
        in_path=str(in_path),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert first.canonical_items_emitted == 2
    assert first.items_inserted_db == 2
    assert first.items_skipped_already_present == 0

    assert second.canonical_items_emitted == 2
    assert second.items_inserted_db == 0
    assert second.items_skipped_already_present == 2

    assert len(_read_jsonl(out_path)) == 2
    assert _db_item_count(db_path) == 2


def test_stage_2_invalid_input_is_skipped_and_counted_by_reason(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    in_path = tmp_path / "raw_items.jsonl"
    out_path = tmp_path / "canonical_items.jsonl"
    report_path = tmp_path / "stage_2_report.json"

    _write_pipeline(pipeline, db_path, tmp_path)

    valid = _valid_raw_item(external_id="ok-1")
    invalid_field_type = {
        **_valid_raw_item(external_id="bad-type"),
        "title": 42,
    }
    missing_required = {
        **_valid_raw_item(external_id="missing-url"),
        "url": "   ",
    }
    invalid_timestamp = {
        **_valid_raw_item(external_id="bad-time"),
        "published_at": "not-a-date",
    }

    lines = [
        "{bad json",
        "[]",
        json.dumps(invalid_field_type, ensure_ascii=True),
        json.dumps(missing_required, ensure_ascii=True),
        json.dumps(invalid_timestamp, ensure_ascii=True),
        json.dumps(valid, ensure_ascii=True),
    ]
    in_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = run_normalize(
        pipeline_path=str(pipeline),
        in_path=str(in_path),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.total_lines_read == 6
    assert result.total_raw_items_parsed == 4
    assert result.canonical_items_emitted == 1
    assert result.items_inserted_db == 1

    assert result.invalid_json_lines == 1
    assert result.invalid_json_objects == 1
    assert result.invalid_field_types == 1
    assert result.missing_required_fields == 1
    assert result.invalid_timestamps == 1
    assert result.items_skipped_invalid == 5

    emitted = _read_jsonl(out_path)
    assert len(emitted) == 1
    assert emitted[0]["external_id"] == "ok-1"
    assert _db_item_count(db_path) == 1
