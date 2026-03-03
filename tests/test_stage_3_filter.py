import json
import sqlite3
from pathlib import Path

from app.filter.cli import main as filter_cli_main
from app.filter.models import compile_keyword_groups, score_relevance
from app.filter.runner import run_filter


def _write_pipeline(
    path: Path,
    *,
    db_path: Path,
    outputs_dir: Path,
    min_content_chars: int = 20,
    min_relevance_score: int = 2,
    max_candidates_default: int = 100,
) -> None:
    path.write_text(
        f"""
paths:
  sqlite_db: "{db_path.as_posix()}"
  outputs_dir: "{outputs_dir.as_posix()}"
stage_3_filter:
  min_content_chars: {min_content_chars}
  min_relevance_score: {min_relevance_score}
  max_candidates_default: {max_candidates_default}
  keyword_groups:
    automation:
      weight: 1
      terms: ["agent", "freelanc"]
    monetization:
      weight: 2
      terms: ["ai", "side hustle", "cold email"]
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _seed_items(db_path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                item_id TEXT PRIMARY KEY,
                source_type TEXT,
                source_id TEXT,
                source_name TEXT,
                creator TEXT,
                title TEXT,
                url TEXT,
                published_at TEXT,
                fetched_at TEXT,
                summary TEXT,
                content_text TEXT
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO items (
                    item_id,
                    source_type,
                    source_id,
                    source_name,
                    creator,
                    title,
                    url,
                    published_at,
                    fetched_at,
                    summary,
                    content_text
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["item_id"],
                    row["source_type"],
                    row["source_id"],
                    row["source_name"],
                    row["creator"],
                    row["title"],
                    row["url"],
                    row["published_at"],
                    row["fetched_at"],
                    row.get("summary"),
                    row.get("content_text"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _item(
    *,
    item_id: str,
    published_at: str,
    title: str = "AI workflow for creators",
    summary: str | None = "This side hustle content is long enough for filtering.",
    content_text: str | None = "This AI side hustle content is detailed and long enough to pass.",
) -> dict:
    return {
        "item_id": item_id,
        "source_type": "youtube",
        "source_id": "channel-a",
        "source_name": "Channel A",
        "creator": "Host A",
        "title": title,
        "url": f"https://example.com/{item_id}",
        "published_at": published_at,
        "fetched_at": "2026-03-03T13:00:00Z",
        "summary": summary,
        "content_text": content_text,
    }


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _candidate_count(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()
        return int(row[0])
    finally:
        conn.close()


def _candidate_rows(db_path: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            "SELECT item_id, content_text, created_at FROM candidates ORDER BY item_id ASC"
        )
        return cursor.fetchall()
    finally:
        conn.close()


def _assert_counter_invariants(report: dict) -> None:
    fail_total = sum(int(v) for v in report["fail_breakdown"].values())
    assert report["items_considered"] == report["passed_count"] + report["failed_count"]
    assert report["inserted_db"] == report["candidate_items_emitted"]
    assert report["items_considered"] <= report["items_available_total"]
    assert report["candidate_items_emitted"] <= report["max_candidates"]
    assert report["failed_count"] == fail_total
    assert report["passed_count"] == (
        report["inserted_db"] + report["candidates_skipped_already_present"]
    )


def test_stage_3_pass_fail_reason_precedence_and_report_invariants(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "candidate_items.jsonl"
    report_path = tmp_path / "stage_3_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, min_content_chars=20)

    rows = [
        _item(
            item_id="missing-required",
            published_at="2026-03-03T12:00:00Z",
            title="   ",
            content_text="This is long enough and contains ai side hustle terms for score.",
        ),
        _item(
            item_id="too-short",
            published_at="2026-03-03T11:00:00Z",
            title="Valid title",
            content_text="ai",
        ),
        _item(
            item_id="low-score",
            published_at="2026-03-03T10:00:00Z",
            title="Long content but irrelevant",
            content_text="This body is long enough but has no configured matching keywords inside.",
        ),
        _item(
            item_id="pass-item",
            published_at="2026-03-03T09:00:00Z",
            title="AI ideas for creators",
            content_text="This side hustle plan uses AI tools and has enough detail to pass.",
        ),
    ]
    _seed_items(db_path, rows)

    result = run_filter(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.items_available_total == 4
    assert result.items_considered == 4
    assert result.passed_count == 1
    assert result.failed_count == 3
    assert result.inserted_db == 1
    assert result.candidate_items_emitted == 1
    assert result.fail_breakdown == {
        "missing_required_fields": 1,
        "content_too_short": 1,
        "low_relevance_score": 1,
    }

    emitted = _read_jsonl(out_path)
    assert [row["item_id"] for row in emitted] == ["pass-item"]

    report = _read_json(report_path)
    _assert_counter_invariants(report)
    assert report["fail_breakdown"] == result.fail_breakdown


def test_stage_3_scoring_unique_phrase_prefix_and_highest_weight_once() -> None:
    groups = compile_keyword_groups(
        {
            "low_weight": {"weight": 1, "terms": ["ai", "side hustle", "freelanc", "ai"]},
            "high_weight": {"weight": 5, "terms": ["ai", "cold email"]},
        }
    )

    score, matched = score_relevance(
        title="AI AI Side Hustle ideas",
        body_text="Freelancing strategy with cold email and ai. cold email appears again.",
        keyword_groups=groups,
    )

    assert matched == ["ai", "cold email", "freelanc*", "side hustle"]
    assert score == 12


def test_stage_3_deterministic_ordering_with_cap(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "candidate_items.jsonl"
    report_path = tmp_path / "stage_3_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)

    rows = [
        _item(item_id="item-b", published_at="2026-03-03T12:00:00Z"),
        _item(item_id="item-a", published_at="2026-03-03T12:00:00Z"),
        _item(item_id="item-c", published_at="2026-03-03T11:00:00Z"),
        _item(item_id="item-d", published_at="2026-03-03T10:00:00Z"),
    ]
    _seed_items(db_path, rows)

    result = run_filter(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
        max_candidates_override=2,
    )

    emitted = _read_jsonl(out_path)
    assert [row["item_id"] for row in emitted] == ["item-a", "item-b"]
    assert result.max_candidates == 2
    assert result.reached_max_candidates is True
    assert result.passed_count == 2
    assert result.items_considered == 2


def test_stage_3_idempotency_second_run_emits_zero(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "candidate_items.jsonl"
    report_1 = tmp_path / "stage_3_report_first.json"
    report_2 = tmp_path / "stage_3_report_second.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_items(
        db_path,
        [
            _item(item_id="pass-1", published_at="2026-03-03T12:00:00Z"),
            _item(item_id="pass-2", published_at="2026-03-03T11:00:00Z"),
        ],
    )

    first = run_filter(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_1),
    )
    second = run_filter(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_2),
    )

    assert first.inserted_db == 2
    assert first.candidate_items_emitted == 2
    assert second.items_available_total == 0
    assert second.items_considered == 0
    assert second.inserted_db == 0
    assert second.candidate_items_emitted == 0
    assert _read_jsonl(out_path) == []
    assert _candidate_count(db_path) == 2


def test_stage_3_max_candidates_zero_short_circuits_with_reached_true(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "candidate_items.jsonl"
    report_path = tmp_path / "stage_3_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_items(
        db_path,
        [
            _item(item_id="item-1", published_at="2026-03-03T12:00:00Z"),
            _item(item_id="item-2", published_at="2026-03-03T11:00:00Z"),
        ],
    )

    result = run_filter(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
        max_candidates_override=0,
    )

    assert result.max_candidates == 0
    assert result.items_available_total == 2
    assert result.items_considered == 0
    assert result.passed_count == 0
    assert result.failed_count == 0
    assert result.inserted_db == 0
    assert result.candidate_items_emitted == 0
    assert result.reached_max_candidates is True
    assert result.fail_breakdown == {
        "missing_required_fields": 0,
        "content_too_short": 0,
        "low_relevance_score": 0,
    }
    assert _candidate_count(db_path) == 0
    assert _read_jsonl(out_path) == []


def test_stage_3_invalid_config_via_cli_returns_exit_code_2(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline_invalid.yaml"
    pipeline.write_text(
        f"""
paths:
  sqlite_db: "{(tmp_path / "state.db").as_posix()}"
  outputs_dir: "{tmp_path.as_posix()}"
stage_3_filter:
  min_content_chars: -1
  min_relevance_score: 1
  max_candidates_default: 10
  keyword_groups: {{}}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    exit_code = filter_cli_main(["--pipeline", str(pipeline)])
    assert exit_code == 2


def test_stage_3_body_robustness_and_emitted_db_content_consistency(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "candidate_items.jsonl"
    report_path = tmp_path / "stage_3_report.json"
    _write_pipeline(
        pipeline,
        db_path=db_path,
        outputs_dir=tmp_path,
        min_content_chars=5,
    )
    _seed_items(
        db_path,
        [
            _item(
                item_id="null-body",
                published_at="2026-03-03T12:00:00Z",
                summary=None,
                content_text=None,
            ),
            _item(
                item_id="summary-fallback",
                published_at="2026-03-03T11:00:00Z",
                summary="  This AI side hustle summary is used as body text.  ",
                content_text=None,
            ),
            _item(
                item_id="content-preferred",
                published_at="2026-03-03T10:00:00Z",
                summary="This summary should be ignored.",
                content_text="  This cold email AI content should be selected instead.  ",
            ),
        ],
    )

    result = run_filter(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.fail_breakdown["content_too_short"] == 1
    emitted_rows = _read_jsonl(out_path)
    emitted_by_id = {row["item_id"]: row for row in emitted_rows}

    assert emitted_by_id["summary-fallback"]["content_text"] == (
        "This AI side hustle summary is used as body text."
    )
    assert emitted_by_id["content-preferred"]["content_text"] == (
        "This cold email AI content should be selected instead."
    )

    db_rows = {row["item_id"]: row for row in _candidate_rows(db_path)}
    assert db_rows["summary-fallback"]["content_text"] == emitted_by_id["summary-fallback"]["content_text"]
    assert db_rows["content-preferred"]["content_text"] == emitted_by_id["content-preferred"]["content_text"]
    assert db_rows["summary-fallback"]["created_at"] == emitted_by_id["summary-fallback"]["scored_at"]
    assert db_rows["content-preferred"]["created_at"] == emitted_by_id["content-preferred"]["scored_at"]
