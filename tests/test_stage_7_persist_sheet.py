import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.sheets.runner import FatalPersistError, run_persist


class FakeWorksheet:
    def __init__(
        self,
        values: list[list[str]],
        *,
        fail_update: bool = False,
        fail_append: bool = False,
    ) -> None:
        self.values = [list(row) for row in values]
        self.fail_update = fail_update
        self.fail_append = fail_append
        self.update_calls: list[dict] = []
        self.append_calls: list[list[str]] = []
        self.write_order: list[str] = []

    def fetch_all_values(self) -> list[list[str]]:
        return [list(row) for row in self.values]

    def update_row(self, *, row_number: int, values: list[str]) -> None:
        if self.fail_update:
            raise RuntimeError("forced update failure")
        while len(self.values) < row_number:
            self.values.append([])
        self.values[row_number - 1] = list(values)
        self.update_calls.append({"row_number": row_number, "values": list(values)})
        self.write_order.append(_extract_item_id(self.values[0], values))

    def append_row(self, *, values: list[str]) -> None:
        if self.fail_append:
            raise RuntimeError("forced append failure")
        self.values.append(list(values))
        self.append_calls.append(list(values))
        header = self.values[0] if self.values else []
        self.write_order.append(_extract_item_id(header, values))


class FakeSheetsClient:
    def __init__(self, worksheet: FakeWorksheet) -> None:
        self._worksheet = worksheet
        self.open_calls: list[dict[str, str]] = []

    def open_worksheet(self, *, spreadsheet_id: str, worksheet_name: str) -> FakeWorksheet:
        self.open_calls.append(
            {"spreadsheet_id": spreadsheet_id, "worksheet_name": worksheet_name}
        )
        return self._worksheet


def _extract_item_id(header: list[str], values: list[str]) -> str:
    normalized_header = [str(cell).strip() for cell in header]
    try:
        index = normalized_header.index("item_id")
    except ValueError:
        return ""
    if index >= len(values):
        return ""
    return str(values[index]).strip()


def _write_pipeline(
    path: Path,
    *,
    db_path: Path,
    outputs_dir: Path,
    sheets_enabled: bool = True,
    spreadsheet_id: str = "sheet-123",
    worksheet_name: str = "Ideas",
    key_column: str = "item_id",
    header_row: int = 1,
    max_rows_default: int = 200,
) -> None:
    path.write_text(
        f"""
paths:
  sqlite_db: "{db_path.as_posix()}"
  outputs_dir: "{outputs_dir.as_posix()}"
sheets:
  enabled: {"true" if sheets_enabled else "false"}
  spreadsheet_id: "{spreadsheet_id}"
  worksheet_name: "{worksheet_name}"
  key_column: "{key_column}"
  header_row: {header_row}
stage_7_persist:
  max_rows_default: {max_rows_default}
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _seed_stage7_db(
    db_path: Path,
    *,
    ideas: list[dict],
    scripts: list[dict],
    items: list[dict],
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ideas (
                item_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                topic TEXT NOT NULL,
                viral_rating INTEGER NOT NULL,
                hooks TEXT NOT NULL,
                platform TEXT NOT NULL,
                monetization_angle TEXT NOT NULL,
                tools_mentioned TEXT NOT NULL,
                published_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scripts (
                item_id TEXT PRIMARY KEY,
                primary_hook TEXT NOT NULL,
                script_sections TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                item_id TEXT PRIMARY KEY,
                creator TEXT NOT NULL,
                source_name TEXT NOT NULL
            )
            """
        )

        for row in ideas:
            conn.execute(
                """
                INSERT INTO ideas (
                    item_id,
                    url,
                    title,
                    topic,
                    viral_rating,
                    hooks,
                    platform,
                    monetization_angle,
                    tools_mentioned,
                    published_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["item_id"],
                    row["url"],
                    row["title"],
                    row["topic"],
                    row["viral_rating"],
                    row["hooks"],
                    row["platform"],
                    row["monetization_angle"],
                    row["tools_mentioned"],
                    row["published_at"],
                ),
            )

        for row in scripts:
            conn.execute(
                """
                INSERT INTO scripts (
                    item_id,
                    primary_hook,
                    script_sections
                )
                VALUES (?, ?, ?)
                """,
                (
                    row["item_id"],
                    row["primary_hook"],
                    row["script_sections"],
                ),
            )

        for row in items:
            conn.execute(
                """
                INSERT INTO items (
                    item_id,
                    creator,
                    source_name
                )
                VALUES (?, ?, ?)
                """,
                (
                    row["item_id"],
                    row["creator"],
                    row["source_name"],
                ),
            )

        conn.commit()
    finally:
        conn.close()


def _idea_row(
    *,
    item_id: str,
    viral_rating: int = 7,
    published_at: str = "2026-03-03T12:00:00Z",
    hooks: str | None = None,
    tools_mentioned: str | None = None,
) -> dict:
    return {
        "item_id": item_id,
        "url": f"https://example.com/{item_id}",
        "title": f"Title {item_id}",
        "topic": "AI automations",
        "viral_rating": viral_rating,
        "hooks": hooks
        if hooks is not None
        else json.dumps(["Fallback hook", "Backup hook"], ensure_ascii=True),
        "platform": "youtube",
        "monetization_angle": "Sell automation services.",
        "tools_mentioned": tools_mentioned
        if tools_mentioned is not None
        else json.dumps(["OpenAI", "Zapier"], ensure_ascii=True),
        "published_at": published_at,
    }


def _script_sections_json(*, hook_text: str = "Primary hook from script") -> str:
    return json.dumps(
        [
            {"label": "hook", "text": hook_text},
            {"label": "setup", "text": "Setup context"},
            {"label": "steps", "text": "Step one\nStep two"},
            {"label": "cta", "text": "Follow for more"},
        ],
        ensure_ascii=True,
    )


def _script_row(
    *,
    item_id: str,
    primary_hook: str = "Primary hook from script",
    script_sections: str | None = None,
) -> dict:
    return {
        "item_id": item_id,
        "primary_hook": primary_hook,
        "script_sections": script_sections if script_sections is not None else _script_sections_json(),
    }


def _item_row(*, item_id: str, creator: str = "Creator One", source_name: str = "Source One") -> dict:
    return {
        "item_id": item_id,
        "creator": creator,
        "source_name": source_name,
    }


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_report_invariants(payload: dict) -> None:
    assert payload["run_status"] in {"completed", "fatal"}
    if payload["run_status"] == "completed":
        assert payload["fatal_error"] is None
    if payload["run_status"] == "fatal":
        assert isinstance(payload["fatal_error"], str)
        assert payload["fatal_error"].strip()

    assert payload["rows_considered"] <= payload["max_rows"]
    assert (
        payload["rows_inserted"]
        + payload["rows_updated"]
        + payload["rows_skipped_missing_script"]
        + payload["rows_skipped_invalid_payload"]
        == payload["rows_considered"]
    )
    assert payload["errors_count"] == payload["rows_skipped_invalid_payload"]
    if payload["run_status"] == "completed" and payload["sheets_enabled"] is False:
        for field in (
            "rows_available_total",
            "rows_considered",
            "rows_inserted",
            "rows_updated",
            "rows_skipped_missing_script",
            "rows_skipped_invalid_payload",
            "errors_count",
        ):
            assert payload[field] == 0
        assert payload["spreadsheet_id"] == ""
        assert payload["worksheet_name"] == ""


def test_sheets_disabled_skips_db_and_api_and_writes_zero_report(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "missing.db"
    report_path = tmp_path / "stage_7_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, sheets_enabled=False)

    worksheet = FakeWorksheet([["item_id", "status"]])
    fake_client = FakeSheetsClient(worksheet)

    result = run_persist(
        pipeline_path=str(pipeline),
        report_path=str(report_path),
        sheet_id_override="override-sheet-id",
        worksheet_override="override-worksheet",
        sheets_client=fake_client,
    )

    assert result.run_status == "completed"
    assert result.sheets_enabled is False
    assert result.rows_available_total == 0
    assert result.rows_considered == 0
    assert result.rows_inserted == 0
    assert result.rows_updated == 0
    assert result.rows_skipped_missing_script == 0
    assert result.rows_skipped_invalid_payload == 0
    assert result.errors_count == 0
    assert result.spreadsheet_id == ""
    assert result.worksheet_name == ""
    assert fake_client.open_calls == []

    report = _read_json(report_path)
    _assert_report_invariants(report)


@pytest.mark.parametrize(
    "header,key_column",
    [
        (
            [
                "item_id",
                "creator",
                "post_link",
                "topic",
                "viral_rating",
                "hook",
                "platform",
                "status",
            ],
            "item_id",
        ),
        (
            [
                "item_id",
                "item_id",
                "creator",
                "post_link",
                "topic",
                "viral_rating",
                "hook",
                "platform",
                "draft_script",
                "status",
            ],
            "item_id",
        ),
        (
            [
                "item_id",
                "creator",
                "post_link",
                "topic",
                "viral_rating",
                "hook",
                "platform",
                "draft_script",
                "status",
            ],
            "post_link",
        ),
    ],
)
def test_header_contract_failures_are_fatal(
    tmp_path: Path, header: list[str], key_column: str
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "fatal_report.json"
    _write_pipeline(
        pipeline,
        db_path=db_path,
        outputs_dir=tmp_path,
        sheets_enabled=True,
        key_column=key_column,
    )
    _seed_stage7_db(
        db_path,
        ideas=[_idea_row(item_id="row-1")],
        scripts=[_script_row(item_id="row-1")],
        items=[_item_row(item_id="row-1")],
    )

    worksheet = FakeWorksheet([header])
    fake_client = FakeSheetsClient(worksheet)

    with pytest.raises(FatalPersistError):
        run_persist(
            pipeline_path=str(pipeline),
            report_path=str(report_path),
            sheets_client=fake_client,
        )

    report = _read_json(report_path)
    assert report["run_status"] == "fatal"
    assert report["fatal_error"]


def test_insert_sets_status_new_notes_empty_and_updated_at(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_stage7_db(
        db_path,
        ideas=[_idea_row(item_id="row-1")],
        scripts=[_script_row(item_id="row-1")],
        items=[_item_row(item_id="row-1", creator="Analyst Creator", source_name="Source X")],
    )

    header = [
        "item_id",
        "creator",
        "post_link",
        "topic",
        "viral_rating",
        "hook",
        "platform",
        "draft_script",
        "status",
        "monetization_angle",
        "tools_mentioned",
        "published_at",
        "updated_at",
        "notes",
    ]
    worksheet = FakeWorksheet([header])
    fake_client = FakeSheetsClient(worksheet)

    result = run_persist(
        pipeline_path=str(pipeline),
        report_path=str(report_path),
        sheets_client=fake_client,
    )

    assert result.run_status == "completed"
    assert result.rows_inserted == 1
    assert result.rows_updated == 0
    assert len(worksheet.append_calls) == 1

    inserted = worksheet.append_calls[0]
    index = {name: idx for idx, name in enumerate(header)}
    assert inserted[index["item_id"]] == "row-1"
    assert inserted[index["creator"]] == "Analyst Creator"
    assert inserted[index["status"]] == "New"
    assert inserted[index["notes"]] == ""
    assert inserted[index["updated_at"]]
    assert inserted[index["draft_script"]] == (
        "Hook: Primary hook from script\n\n"
        "Setup: Setup context\n\n"
        "Steps:\n"
        "Step one\nStep two\n\n"
        "CTA: Follow for more"
    )

    report = _read_json(report_path)
    _assert_report_invariants(report)


def test_existing_row_update_preserves_status_and_notes(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_stage7_db(
        db_path,
        ideas=[_idea_row(item_id="row-1")],
        scripts=[_script_row(item_id="row-1", primary_hook="Updated hook")],
        items=[_item_row(item_id="row-1", creator="Updated Creator")],
    )

    header = [
        "item_id",
        "creator",
        "post_link",
        "topic",
        "viral_rating",
        "hook",
        "platform",
        "draft_script",
        "status",
        "notes",
    ]
    existing_row = [
        "row-1",
        "Old Creator",
        "https://old.example.com",
        "Old topic",
        "5",
        "Old hook",
        "youtube",
        "Old script",
        "In Review",
        "Keep this note",
    ]
    worksheet = FakeWorksheet([header, existing_row])
    fake_client = FakeSheetsClient(worksheet)

    result = run_persist(pipeline_path=str(pipeline), sheets_client=fake_client)

    assert result.rows_inserted == 0
    assert result.rows_updated == 1
    assert len(worksheet.update_calls) == 1
    assert len(worksheet.append_calls) == 0

    updated_row = worksheet.update_calls[0]["values"]
    index = {name: idx for idx, name in enumerate(header)}
    assert updated_row[index["creator"]] == "Updated Creator"
    assert updated_row[index["hook"]] == "Updated hook"
    assert updated_row[index["status"]] == "In Review"
    assert updated_row[index["notes"]] == "Keep this note"


def test_missing_script_row_increments_counter(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_stage7_db(
        db_path,
        ideas=[_idea_row(item_id="row-1")],
        scripts=[],
        items=[_item_row(item_id="row-1")],
    )

    header = [
        "item_id",
        "creator",
        "post_link",
        "topic",
        "viral_rating",
        "hook",
        "platform",
        "draft_script",
        "status",
    ]
    worksheet = FakeWorksheet([header])
    fake_client = FakeSheetsClient(worksheet)

    result = run_persist(pipeline_path=str(pipeline), sheets_client=fake_client)

    assert result.rows_considered == 1
    assert result.rows_skipped_missing_script == 1
    assert result.rows_inserted == 0
    assert result.rows_updated == 0
    assert len(worksheet.append_calls) == 0
    assert len(worksheet.update_calls) == 0


def test_invalid_payload_increments_rows_skipped_invalid_payload(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_stage7_db(
        db_path,
        ideas=[
            _idea_row(item_id="bad-script", viral_rating=10),
            _idea_row(item_id="bad-tools", viral_rating=9, tools_mentioned="{bad-json"),
        ],
        scripts=[
            _script_row(item_id="bad-script", script_sections='{"not":"array"}'),
            _script_row(item_id="bad-tools"),
        ],
        items=[
            _item_row(item_id="bad-script"),
            _item_row(item_id="bad-tools"),
        ],
    )

    header = [
        "item_id",
        "creator",
        "post_link",
        "topic",
        "viral_rating",
        "hook",
        "platform",
        "draft_script",
        "status",
    ]
    worksheet = FakeWorksheet([header])
    fake_client = FakeSheetsClient(worksheet)

    result = run_persist(pipeline_path=str(pipeline), sheets_client=fake_client)

    assert result.rows_considered == 2
    assert result.rows_skipped_invalid_payload == 2
    assert result.errors_count == 2
    assert isinstance(result.first_error, str)
    assert result.first_error
    assert result.rows_inserted == 0
    assert result.rows_updated == 0


def test_deterministic_ordering_uses_viral_rating_then_published_then_item_id(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, max_rows_default=3)
    _seed_stage7_db(
        db_path,
        ideas=[
            _idea_row(item_id="b-item", viral_rating=9, published_at="2026-03-03T12:00:00Z"),
            _idea_row(item_id="a-item", viral_rating=9, published_at="2026-03-03T12:00:00Z"),
            _idea_row(item_id="c-item", viral_rating=8, published_at="2026-03-03T13:00:00Z"),
        ],
        scripts=[
            _script_row(item_id="a-item"),
            _script_row(item_id="b-item"),
            _script_row(item_id="c-item"),
        ],
        items=[
            _item_row(item_id="a-item"),
            _item_row(item_id="b-item"),
            _item_row(item_id="c-item"),
        ],
    )

    header = [
        "item_id",
        "creator",
        "post_link",
        "topic",
        "viral_rating",
        "hook",
        "platform",
        "draft_script",
        "status",
    ]
    existing_b = [
        "b-item",
        "Creator",
        "https://example.com/b-item",
        "Topic",
        "8",
        "Old",
        "youtube",
        "Old draft",
        "Review",
    ]
    worksheet = FakeWorksheet([header, existing_b])
    fake_client = FakeSheetsClient(worksheet)

    result = run_persist(pipeline_path=str(pipeline), sheets_client=fake_client)

    assert result.rows_considered == 3
    assert result.rows_inserted == 2
    assert result.rows_updated == 1
    assert worksheet.write_order == ["a-item", "b-item", "c-item"]


def test_fatal_before_pipeline_resolution_writes_report_when_report_override_is_valid(
    tmp_path: Path,
) -> None:
    missing_pipeline = tmp_path / "missing_pipeline.yaml"
    report_path = tmp_path / "fatal_stage7_report.json"

    with pytest.raises(FatalPersistError):
        run_persist(
            pipeline_path=str(missing_pipeline),
            report_path=str(report_path),
        )

    payload = _read_json(report_path)
    assert payload["run_status"] == "fatal"
    assert payload["max_rows"] == 0
    assert payload["rows_available_total"] == 0
    assert payload["rows_considered"] == 0
    assert payload["rows_inserted"] == 0
    assert payload["rows_updated"] == 0
    assert payload["rows_skipped_missing_script"] == 0
    assert payload["rows_skipped_invalid_payload"] == 0
    assert payload["errors_count"] == 0
    assert payload["db_path"] == ""
    assert payload["sheets_enabled"] is False
    assert payload["spreadsheet_id"] == ""
    assert payload["worksheet_name"] == ""
    assert payload["first_error"] == payload["fatal_error"]


def test_invalid_override_and_config_errors_are_fatal(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "fatal_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, sheets_enabled=False)

    with pytest.raises(FatalPersistError):
        run_persist(
            pipeline_path=str(pipeline),
            report_path=str(report_path),
            max_rows_override="abc",
        )
    assert _read_json(report_path)["run_status"] == "fatal"

    bad_pipeline = tmp_path / "bad_pipeline.yaml"
    bad_pipeline.write_text(
        """
paths:
  sqlite_db: "data/state.db"
  outputs_dir: "data/outputs"
sheets:
  enabled: "true"
stage_7_persist:
  max_rows_default: 1
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(FatalPersistError):
        run_persist(
            pipeline_path=str(bad_pipeline),
            report_path=str(tmp_path / "fatal_bad_pipeline_report.json"),
        )

