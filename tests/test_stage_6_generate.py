import json
import sqlite3
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.generate import llm as llm_module
from app.generate.llm import (
    FatalLLMError,
    LLMRuntimeConfig,
    NonRetryableLLMError,
    RetryableLLMError,
)
from app.generate.models import FAIL_BREAKDOWN_KEYS
from app.generate.prompts import PromptLoadError
from app.generate.runner import FatalGenerateError, run_generate


class FakeLLMClient:
    def __init__(
        self,
        *,
        responses: list[str] | None = None,
        errors: list[Exception | None] | None = None,
    ) -> None:
        self.responses = responses or []
        self.errors = errors or []
        self.calls: list[dict[str, str]] = []

    def call_json(
        self,
        *,
        prompt: str,
        schema_name: str,
        schema: dict,
        call_label: str,
    ) -> str:
        index = len(self.calls)
        self.calls.append(
            {
                "prompt": prompt,
                "schema_name": schema_name,
                "call_label": call_label,
            }
        )
        if index < len(self.errors):
            err = self.errors[index]
            if err is not None:
                raise err
        if not self.responses:
            raise AssertionError("no fake response configured")
        if index < len(self.responses):
            return self.responses[index]
        return self.responses[-1]


def _write_pipeline(
    path: Path,
    *,
    db_path: Path,
    outputs_dir: Path,
    api_key: str = "test-api-key",
    api_key_env_var: str = "OPENAI_API_KEY",
    max_items_default: int = 25,
) -> None:
    path.write_text(
        f"""
paths:
  sqlite_db: "{db_path.as_posix()}"
  outputs_dir: "{outputs_dir.as_posix()}"
llm:
  provider: "openai"
  model: "gpt-4o-mini"
  temperature: 0.2
  max_output_tokens: 900
  requests_per_minute_soft: 120
  request_timeout_s: 60
  retry_max_attempts: 3
  retry_backoff_initial_s: 1.0
  retry_backoff_multiplier: 2.0
  retry_backoff_max_s: 8.0
  api_key_env_var: "{api_key_env_var}"
  api_key: "{api_key}"
stage_6_generate:
  max_items_default: {max_items_default}
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _seed_ideas(db_path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ideas (
                item_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                recommended_format TEXT NOT NULL,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                published_at TEXT NOT NULL,
                topic TEXT NOT NULL,
                core_claim TEXT NOT NULL,
                workflow_steps TEXT NOT NULL,
                tools_mentioned TEXT NOT NULL,
                monetization_angle TEXT NOT NULL,
                metrics_claims TEXT NOT NULL,
                assumptions TEXT NOT NULL,
                hooks TEXT NOT NULL,
                viral_rating INTEGER NOT NULL
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO ideas (
                    item_id,
                    platform,
                    recommended_format,
                    url,
                    title,
                    published_at,
                    topic,
                    core_claim,
                    workflow_steps,
                    tools_mentioned,
                    monetization_angle,
                    metrics_claims,
                    assumptions,
                    hooks,
                    viral_rating
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["item_id"],
                    row["platform"],
                    row["recommended_format"],
                    row["url"],
                    row["title"],
                    row["published_at"],
                    row["topic"],
                    row["core_claim"],
                    row["workflow_steps"],
                    row["tools_mentioned"],
                    row["monetization_angle"],
                    row["metrics_claims"],
                    row["assumptions"],
                    row["hooks"],
                    row["viral_rating"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_scripts(db_path: Path, item_ids: list[str]) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scripts (
                item_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                recommended_format TEXT NOT NULL,
                primary_hook TEXT NOT NULL,
                alt_hooks TEXT NOT NULL,
                script_sections TEXT NOT NULL,
                word_count INTEGER NOT NULL,
                estimated_seconds INTEGER NOT NULL,
                cta TEXT NOT NULL,
                disclaimer TEXT NOT NULL,
                llm_provider TEXT NOT NULL,
                llm_model TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        for item_id in item_ids:
            payload = _valid_script_payload_dict(recommended_format="shorts")
            conn.execute(
                """
                INSERT INTO scripts (
                    item_id,
                    platform,
                    recommended_format,
                    primary_hook,
                    alt_hooks,
                    script_sections,
                    word_count,
                    estimated_seconds,
                    cta,
                    disclaimer,
                    llm_provider,
                    llm_model,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    "youtube",
                    "shorts",
                    payload["primary_hook"],
                    json.dumps(payload["alt_hooks"], ensure_ascii=True),
                    json.dumps(payload["script"]["sections"], ensure_ascii=True),
                    120,
                    60,
                    payload["cta"],
                    payload["disclaimer"],
                    "openai",
                    "gpt-4o-mini",
                    "2026-03-03T12:00:00Z",
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _idea_row(
    *,
    item_id: str,
    platform: str = "youtube",
    recommended_format: str = "shorts",
    published_at: str = "2026-03-03T12:00:00Z",
    viral_rating: int = 7,
    workflow_steps: str | None = None,
    tools_mentioned: str | None = None,
    metrics_claims: str | None = None,
    assumptions: str | None = None,
    hooks: str | None = None,
) -> dict:
    return {
        "item_id": item_id,
        "platform": platform,
        "recommended_format": recommended_format,
        "url": f"https://example.com/{item_id}",
        "title": f"Title {item_id}",
        "published_at": published_at,
        "topic": "Automation offer",
        "core_claim": "Do this workflow to sell faster.",
        "workflow_steps": workflow_steps
        if workflow_steps is not None
        else json.dumps(["Find a niche", "Build a demo", "Pitch leads"], ensure_ascii=True),
        "tools_mentioned": tools_mentioned
        if tools_mentioned is not None
        else json.dumps(["OpenAI", "Zapier"], ensure_ascii=True),
        "monetization_angle": "Sell implementation retainers.",
        "metrics_claims": metrics_claims
        if metrics_claims is not None
        else json.dumps(["Made $1k in week one"], ensure_ascii=True),
        "assumptions": assumptions
        if assumptions is not None
        else json.dumps(["Has outbound list"], ensure_ascii=True),
        "hooks": hooks
        if hooks is not None
        else json.dumps(["Hook A", "Hook B", "Hook C"], ensure_ascii=True),
        "viral_rating": viral_rating,
    }


def _word_series(prefix: str, count: int) -> str:
    return " ".join(f"{prefix}{idx}" for idx in range(1, count + 1))


def _valid_script_payload_dict(
    *,
    recommended_format: str = "shorts",
    reported_word_count: int | None = None,
    estimated_seconds: int | None = None,
    mismatch_cta: bool = False,
) -> dict:
    if recommended_format in {"shorts", "reel"}:
        hook = _word_series("hook", 30)
        setup = _word_series("setup", 30)
        steps = "\n".join(
            [
                "- " + _word_series("stepa", 10),
                "- " + _word_series("stepb", 10),
                "- " + _word_series("stepc", 10),
            ]
        )
        cta = _word_series("cta", 30)
        computed_words = 120
        est = 60 if estimated_seconds is None else estimated_seconds
    else:
        hook = _word_series("hook", 40)
        setup = _word_series("setup", 40)
        steps = "\n".join(
            [
                "- " + _word_series("stepa", 10),
                "- " + _word_series("stepb", 10),
                "- " + _word_series("stepc", 10),
                "- " + _word_series("stepd", 10),
            ]
        )
        cta = _word_series("cta", 60)
        computed_words = 180
        est = 80 if estimated_seconds is None else estimated_seconds

    top_level_cta = cta if not mismatch_cta else cta + " mismatch"
    return {
        "primary_hook": "Build this client workflow in one hour.",
        "alt_hooks": [
            "A practical AI service offer you can sell this week.",
            "Turn this automation stack into recurring client revenue.",
        ],
        "script": {
            "sections": [
                {"label": "hook", "text": hook},
                {"label": "setup", "text": setup},
                {"label": "steps", "text": steps},
                {"label": "cta", "text": cta},
            ],
            "word_count": computed_words if reported_word_count is None else reported_word_count,
            "estimated_seconds": est,
        },
        "cta": top_level_cta,
        "disclaimer": "Metric claims are from the source and may not generalize.",
    }


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _scripts_rows(db_path: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute("SELECT * FROM scripts ORDER BY item_id ASC").fetchall()
    finally:
        conn.close()


def _assert_report_invariants(report: dict) -> None:
    assert set(report["fail_breakdown"].keys()) == set(FAIL_BREAKDOWN_KEYS)
    assert report["items_selected"] == report["success_count"] + report["failed_count"]
    assert report["success_count"] == report["inserted_db"] + report["skipped_already_present"]
    assert report["failed_count"] == sum(int(v) for v in report["fail_breakdown"].values())
    assert report["items_selected"] <= report["selected_rows_total"]
    assert report["selected_rows_total"] <= report["max_items"]
    assert report["items_selected"] <= report["items_available_total"]
    if report["run_status"] == "completed":
        assert report["fatal_error"] is None
        assert report["items_selected"] == report["selected_rows_total"]
    if report["run_status"] == "fatal":
        assert isinstance(report["fatal_error"], str)
        assert report["fatal_error"].strip()


def test_success_path_inserts_and_emits_jsonl_arrays(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "scripts.jsonl"
    report_path = tmp_path / "stage_6_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(db_path, [_idea_row(item_id="idea-1")])

    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )
    result = run_generate(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
        llm_client=llm_client,
    )

    assert result.run_status == "completed"
    assert result.items_available_total == 1
    assert result.selected_rows_total == 1
    assert result.success_count == 1
    assert result.failed_count == 0
    assert result.inserted_db == 1
    assert result.skipped_already_present == 0
    assert len(llm_client.calls) == 1

    rows = _read_jsonl(out_path)
    assert len(rows) == 1
    assert isinstance(rows[0]["alt_hooks"], list)
    assert isinstance(rows[0]["script_sections"], list)
    assert rows[0]["word_count"] == 120

    scripts_rows = _scripts_rows(db_path)
    assert len(scripts_rows) == 1
    assert isinstance(scripts_rows[0]["alt_hooks"], str)
    assert isinstance(scripts_rows[0]["script_sections"], str)

    report = _read_json(report_path)
    _assert_report_invariants(report)


def test_pre_llm_validation_failure_maps_and_skips_provider(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="bad-platform", platform="podcast", viral_rating=10),
            _idea_row(item_id="good-row", viral_rating=9),
        ],
    )
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )

    result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client)

    assert result.failed_count == 1
    assert result.success_count == 1
    assert result.fail_breakdown["script_validation_failed"] == 1
    assert result.fail_breakdown["script_llm_failed"] == 0
    assert len(llm_client.calls) == 1


def test_malformed_json_list_field_maps_validation_and_skips_provider(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="bad-json", workflow_steps="{bad-json"),
        ],
    )
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )

    result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client)
    assert result.failed_count == 1
    assert result.fail_breakdown["script_validation_failed"] == 1
    assert len(llm_client.calls) == 0


def test_json_and_schema_failure_mapping(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="invalid-json", viral_rating=10),
            _idea_row(item_id="invalid-schema", viral_rating=9),
        ],
    )
    bad_schema_payload = _valid_script_payload_dict(recommended_format="shorts")
    del bad_schema_payload["alt_hooks"]
    llm_client = FakeLLMClient(
        responses=["not-json", json.dumps(bad_schema_payload, ensure_ascii=True)]
    )

    result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client)
    assert result.failed_count == 2
    assert result.fail_breakdown["script_invalid_json"] == 1
    assert result.fail_breakdown["script_validation_failed"] == 1
    assert len(llm_client.calls) == 2


def test_llm_error_mapping_and_fatal_status(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "fatal_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="retryable", viral_rating=10),
            _idea_row(item_id="nonretryable", viral_rating=9),
        ],
    )
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)],
        errors=[RetryableLLMError("transient"), NonRetryableLLMError("bad request")],
    )
    result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client)
    assert result.failed_count == 2
    assert result.fail_breakdown["script_llm_failed"] == 2

    db_path_fatal = tmp_path / "fatal_state.db"
    _write_pipeline(pipeline, db_path=db_path_fatal, outputs_dir=tmp_path)
    _seed_ideas(
        db_path_fatal,
        [
            _idea_row(item_id="fatal-1", viral_rating=10),
            _idea_row(item_id="fatal-2", viral_rating=9),
        ],
    )
    fatal_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)],
        errors=[FatalLLMError("provider fatal status: 401")],
    )
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            report_path=str(report_path),
            llm_client=fatal_client,
        )
    report = _read_json(report_path)
    assert report["run_status"] == "fatal"
    assert report["items_selected"] == 0
    assert report["selected_rows_total"] == 2


def test_cta_mismatch_maps_validation_failed(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(db_path, [_idea_row(item_id="cta-mismatch")])
    llm_client = FakeLLMClient(
        responses=[
            json.dumps(
                _valid_script_payload_dict(recommended_format="shorts", mismatch_cta=True),
                ensure_ascii=True,
            )
        ]
    )

    result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client)
    assert result.failed_count == 1
    assert result.fail_breakdown["script_validation_failed"] == 1


def test_format_policy_enforced_and_authoritative_word_count(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "scripts.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="shorts-valid", recommended_format="shorts", viral_rating=10),
            _idea_row(item_id="tweet-too-short", recommended_format="tweet", viral_rating=9),
        ],
    )
    shorts_payload = _valid_script_payload_dict(recommended_format="shorts", reported_word_count=999)
    too_short_payload = _valid_script_payload_dict(recommended_format="shorts")
    llm_client = FakeLLMClient(
        responses=[
            json.dumps(shorts_payload, ensure_ascii=True),
            json.dumps(too_short_payload, ensure_ascii=True),
        ]
    )

    result = run_generate(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        llm_client=llm_client,
    )
    assert result.success_count == 1
    assert result.failed_count == 1
    assert result.fail_breakdown["script_validation_failed"] == 1

    emitted = _read_jsonl(out_path)
    assert emitted[0]["item_id"] == "shorts-valid"
    assert emitted[0]["word_count"] == 120

    db_rows = _scripts_rows(db_path)
    assert any(row["item_id"] == "shorts-valid" and int(row["word_count"]) == 120 for row in db_rows)


def test_idempotency_preseeded_scripts_not_selected_or_emitted(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "scripts.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="already"),
            _idea_row(item_id="new"),
        ],
    )
    _seed_scripts(db_path, ["already"])
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )

    result = run_generate(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        llm_client=llm_client,
    )
    assert result.items_available_total == 1
    assert result.selected_rows_total == 1
    assert len(llm_client.calls) == 1
    emitted = _read_jsonl(out_path)
    assert [row["item_id"] for row in emitted] == ["new"]


def test_missing_and_incompatible_ideas_table_are_fatal(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    missing_db = tmp_path / "missing_ideas.db"
    report_1 = tmp_path / "report_missing.json"
    _write_pipeline(pipeline, db_path=missing_db, outputs_dir=tmp_path)
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            report_path=str(report_1),
            llm_client=FakeLLMClient(
                responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
            ),
        )
    assert _read_json(report_1)["run_status"] == "fatal"

    bad_db = tmp_path / "bad_ideas.db"
    conn = sqlite3.connect(str(bad_db))
    try:
        conn.execute(
            """
            CREATE TABLE ideas (
                item_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                recommended_format TEXT NOT NULL,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                published_at TEXT NOT NULL,
                topic TEXT NOT NULL,
                core_claim TEXT NOT NULL,
                workflow_steps TEXT NOT NULL,
                tools_mentioned TEXT NOT NULL,
                monetization_angle TEXT NOT NULL,
                metrics_claims TEXT NOT NULL,
                assumptions TEXT NOT NULL,
                hooks TEXT NOT NULL,
                viral_rating TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    _write_pipeline(pipeline, db_path=bad_db, outputs_dir=tmp_path)
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            llm_client=FakeLLMClient(
                responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
            ),
        )


def test_scripts_table_creation_and_incompatible_schema_fatal(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(db_path, [_idea_row(item_id="idea-1")])
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )
    run_generate(pipeline_path=str(pipeline), llm_client=llm_client)
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scripts' LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        conn.close()

    bad_db = tmp_path / "bad_scripts.db"
    _write_pipeline(pipeline, db_path=bad_db, outputs_dir=tmp_path)
    _seed_ideas(bad_db, [_idea_row(item_id="idea-2")])
    conn = sqlite3.connect(str(bad_db))
    try:
        conn.execute(
            """
            CREATE TABLE scripts (
                item_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                recommended_format TEXT NOT NULL,
                primary_hook TEXT NOT NULL,
                alt_hooks TEXT NOT NULL,
                script_sections TEXT NOT NULL,
                word_count TEXT NOT NULL,
                estimated_seconds INTEGER NOT NULL,
                cta TEXT NOT NULL,
                disclaimer TEXT NOT NULL,
                llm_provider TEXT NOT NULL,
                llm_model TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            llm_client=FakeLLMClient(
                responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
            ),
        )


def test_report_invariants_and_fatal_report_on_override_error(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "scripts.jsonl"
    report_path = tmp_path / "stage_6_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(db_path, [_idea_row(item_id="idea-1")])
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )
    run_generate(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
        llm_client=llm_client,
    )
    report = _read_json(report_path)
    _assert_report_invariants(report)

    fatal_report = tmp_path / "fatal_override_report.json"
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            report_path=str(fatal_report),
            max_items_override="abc",
            llm_client=llm_client,
        )
    fatal_payload = _read_json(fatal_report)
    assert fatal_payload["run_status"] == "fatal"
    assert fatal_payload["fatal_error"]
    assert fatal_payload["max_items"] == 0


def test_invalid_path_overrides_and_invalid_report_override_skip_fatal_write(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "fatal_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(db_path, [_idea_row(item_id="idea-1")])
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )

    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            report_path=str(report_path),
            db_path_override="   ",
            llm_client=llm_client,
        )
    assert report_path.exists()

    invalid_report = "   "
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            report_path=invalid_report,
            max_items_override="abc",
            llm_client=llm_client,
        )
    assert not (tmp_path / "stage_6_report_invalid.json").exists()


def test_deterministic_selection_order_and_max_items_zero(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "scripts.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(
                item_id="b-item",
                viral_rating=9,
                published_at="2026-03-03T12:00:00Z",
            ),
            _idea_row(
                item_id="a-item",
                viral_rating=9,
                published_at="2026-03-03T12:00:00Z",
            ),
            _idea_row(
                item_id="c-item",
                viral_rating=8,
                published_at="2026-03-03T13:00:00Z",
            ),
        ],
    )
    llm_client = FakeLLMClient(
        responses=[
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
        ]
    )
    run_generate(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        max_items_override="3",
        llm_client=llm_client,
    )
    emitted = _read_jsonl(out_path)
    assert [row["item_id"] for row in emitted] == ["a-item", "b-item", "c-item"]

    out_zero = tmp_path / "scripts_zero.jsonl"
    zero_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )
    zero_result = run_generate(
        pipeline_path=str(pipeline),
        out_path=str(out_zero),
        max_items_override="0",
        llm_client=zero_client,
    )
    assert zero_result.selected_rows_total == 0
    assert zero_result.items_selected == 0
    assert len(zero_client.calls) == 0
    assert _read_jsonl(out_zero) == []


def test_api_key_resolution_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(
        pipeline,
        db_path=db_path,
        outputs_dir=tmp_path,
        api_key="config-key",
        api_key_env_var="STAGE6_TEST_API_KEY",
    )
    _seed_ideas(db_path, [_idea_row(item_id="idea-1")])

    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )
    monkeypatch.setenv("STAGE6_TEST_API_KEY", "env-key")
    run_generate(pipeline_path=str(pipeline), llm_client=llm_client)

    db_path_2 = tmp_path / "state2.db"
    _write_pipeline(
        pipeline,
        db_path=db_path_2,
        outputs_dir=tmp_path,
        api_key="config-key",
        api_key_env_var="STAGE6_TEST_API_KEY",
    )
    _seed_ideas(db_path_2, [_idea_row(item_id="idea-2")])
    monkeypatch.setenv("STAGE6_TEST_API_KEY", "   ")
    run_generate(pipeline_path=str(pipeline), llm_client=llm_client)

    db_path_3 = tmp_path / "state3.db"
    _write_pipeline(
        pipeline,
        db_path=db_path_3,
        outputs_dir=tmp_path,
        api_key="",
        api_key_env_var="STAGE6_TEST_API_KEY",
    )
    _seed_ideas(db_path_3, [_idea_row(item_id="idea-3")])
    monkeypatch.delenv("STAGE6_TEST_API_KEY", raising=False)
    with pytest.raises(FatalGenerateError):
        run_generate(pipeline_path=str(pipeline), llm_client=llm_client)


def test_prompt_load_and_placeholder_failures_are_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "fatal_prompt_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(db_path, [_idea_row(item_id="idea-1")])
    llm_client = FakeLLMClient(
        responses=[json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True)]
    )

    monkeypatch.setattr(
        "app.generate.runner.load_prompt_template",
        lambda: (_ for _ in ()).throw(PromptLoadError("missing prompt")),
    )
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            report_path=str(report_path),
            llm_client=llm_client,
        )
    assert _read_json(report_path)["run_status"] == "fatal"

    bad_prompt = "{{PLATFORM}} {{RECOMMENDED_FORMAT}} {{TITLE}} {{URL}} {{TOPIC}} {{CORE_CLAIM}} {{WORKFLOW_STEPS}} {{TOOLS_MENTIONED}} {{MONETIZATION_ANGLE}} {{METRICS_CLAIMS}} {{ASSUMPTIONS}} {{PRIOR_HOOKS}} {{EXTRA}}"
    with pytest.raises(FatalGenerateError):
        run_generate(
            pipeline_path=str(pipeline),
            llm_client=llm_client,
            prompt_template=bad_prompt,
        )


def test_unexpected_per_item_exception_fallback_before_and_after_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_ideas(
        db_path,
        [
            _idea_row(item_id="row-1", viral_rating=10),
            _idea_row(item_id="row-2", viral_rating=9),
        ],
    )
    llm_client = FakeLLMClient(
        responses=[
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
        ]
    )

    import app.generate.runner as runner_module

    original_parse = runner_module.parse_selected_row
    parse_calls = {"count": 0}

    def _patched_parse(raw):
        parse_calls["count"] += 1
        if parse_calls["count"] == 1:
            raise RuntimeError("unexpected pre-provider")
        return original_parse(raw)

    monkeypatch.setattr("app.generate.runner.parse_selected_row", _patched_parse)
    pre_result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client)
    assert pre_result.fail_breakdown["script_validation_failed"] == 1
    assert pre_result.success_count == 1

    db_path_2 = tmp_path / "state2.db"
    _write_pipeline(pipeline, db_path=db_path_2, outputs_dir=tmp_path)
    _seed_ideas(
        db_path_2,
        [
            _idea_row(item_id="row-a", viral_rating=10),
            _idea_row(item_id="row-b", viral_rating=9),
        ],
    )
    llm_client_2 = FakeLLMClient(
        responses=[
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
            json.dumps(_valid_script_payload_dict(recommended_format="shorts"), ensure_ascii=True),
        ]
    )
    original_validate = runner_module.validate_script_payload
    validate_calls = {"count": 0}

    def _patched_validate(raw, *, recommended_format):
        validate_calls["count"] += 1
        if validate_calls["count"] == 1:
            raise RuntimeError("unexpected post-provider")
        return original_validate(raw, recommended_format=recommended_format)

    monkeypatch.setattr("app.generate.runner.validate_script_payload", _patched_validate)
    post_result = run_generate(pipeline_path=str(pipeline), llm_client=llm_client_2)
    assert post_result.fail_breakdown["script_llm_failed"] == 1
    assert post_result.success_count == 1


def test_llm_retry_wait_respects_rpm_and_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeHttpClient:
        def __init__(self, timeout: int) -> None:
            self.timeout = timeout

    class FakeOpenAI:
        def __init__(self, api_key: str, timeout: int, http_client: object) -> None:
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=lambda **kwargs: types.SimpleNamespace(
                        choices=[types.SimpleNamespace(message=types.SimpleNamespace(content='{"ok":1}'))]
                    )
                )
            )

    monkeypatch.setitem(sys.modules, "httpx", types.SimpleNamespace(Client=FakeHttpClient))
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI))

    timestamps = iter([0.0, 0.0, 0.1, 0.1, 1.1])

    def _monotonic() -> float:
        return float(next(timestamps))

    sleep_calls: list[float] = []

    client = llm_module._OpenAIJsonClient(
        config=LLMRuntimeConfig(
            provider="openai",
            model="gpt-4o-mini",
            temperature=0.2,
            max_output_tokens=200,
            requests_per_minute_soft=120,
            request_timeout_s=60,
            retry_max_attempts=2,
            retry_backoff_initial_s=1.0,
            retry_backoff_multiplier=2.0,
            retry_backoff_max_s=8.0,
        ),
        api_key="test-key",
        monotonic_fn=_monotonic,
        sleep_fn=lambda seconds: sleep_calls.append(seconds),
    )

    attempts = {"count": 0}

    def _fake_single_attempt(*, prompt: str, schema_name: str, schema: dict) -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RetryableLLMError("retryable")
        return '{"ok":1}'

    monkeypatch.setattr(client, "_single_attempt", _fake_single_attempt)
    result = client.call_json(
        prompt="prompt",
        schema_name="schema",
        schema={"type": "object"},
        call_label="call",
    )
    assert result == '{"ok":1}'
    assert attempts["count"] == 2
    assert sleep_calls == [1.0]
