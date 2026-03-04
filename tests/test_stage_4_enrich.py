import json
import sqlite3
import subprocess
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.enrich.models import (
    FAIL_BREAKDOWN_KEYS,
    EvidenceMeta,
    EvidenceSnippet,
)
from app.enrich.runner import ConfigError, FatalEnrichError, run_enrich
from app.enrich.youtube import (
    ASRAudioConfig,
    ASRDecodeConfig,
    ASRError,
    ASRRuntime,
    ASRStartupError,
    initialize_asr_runtime,
)


ISO_PUBLISHED = "2026-03-03T12:00:00Z"


class FakeASRRuntime:
    def __init__(
        self,
        *,
        default_text: str = "y" * 900,
        text_by_url: dict[str, str] | None = None,
        error_by_url: dict[str, Exception] | None = None,
        device_effective: str = "cpu",
        compute_type_effective: str = "int8",
    ) -> None:
        self.default_text = default_text
        self.text_by_url = text_by_url or {}
        self.error_by_url = error_by_url or {}
        self.device_effective = device_effective
        self.compute_type_effective = compute_type_effective
        self.calls: list[str] = []

    def transcribe_url(self, url: str) -> str:
        self.calls.append(url)
        if url in self.error_by_url:
            raise self.error_by_url[url]
        return self.text_by_url.get(url, self.default_text)


@pytest.fixture(autouse=True)
def _stub_required_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.enrich.runner._ensure_required_dependencies", lambda: None)


def _yaml_bool(value: bool) -> str:
    return "true" if value else "false"


def _write_pipeline(
    path: Path,
    *,
    db_path: Path,
    outputs_dir: Path,
    require_full_success: bool = True,
    max_items_default: int = 50,
    max_transcripts: int = 10,
    max_asr: int = 3,
    asr_device: str = "cpu",
    asr_compute_type: str = "int8",
    asr_allow_cpu_fallback: bool = False,
    asr_min_chars: int = 800,
) -> None:
    path.write_text(
        f"""
paths:
  sqlite_db: "{db_path.as_posix()}"
  outputs_dir: "{outputs_dir.as_posix()}"
caps:
  max_transcripts_per_run: {max_transcripts}
  max_asr_fallbacks_per_run: {max_asr}
http:
  user_agent: "stage4-test-agent"
  connect_timeout_s: 2
  read_timeout_s: 2
  max_response_mb: 2
  retries:
    max_attempts: 1
stage_4_enrich:
  max_items_default: {max_items_default}
  selection_policy:
    min_newsletters_per_run: 0
    min_youtube_per_run: 0
    max_items_per_source: {max(1, min(10, max_items_default if max_items_default > 0 else 1))}
    source_diversity_first_pass: true
  cooldown_policy:
    enabled: true
    after_consecutive_failures: 1
    skip_for_hours: 24
    reasons:
      - newsletter_fetch_failed
      - newsletter_extract_failed
      - newsletter_text_too_short
      - youtube_video_id_parse_failed
      - youtube_asr_failed
      - youtube_text_too_short
  youtube_enrichment:
    mode: asr_only
    require_full_success: {_yaml_bool(require_full_success)}
    audio:
      format_selector: "bestaudio"
      extract_format: "wav"
      download_timeout_s: 180
      download_retries: 2
      retry_backoff_s: 2.0
    asr:
      model: "distil-large-v3"
      device: "{asr_device}"
      compute_type: "{asr_compute_type}"
      language: "en"
      beam_size: 5
      temperature: 0.0
      condition_on_previous_text: false
      vad_filter: true
      max_audio_seconds: 7200
      min_chars: {asr_min_chars}
      allow_cpu_fallback: {_yaml_bool(asr_allow_cpu_fallback)}
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _seed_candidates(db_path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candidates (
                item_id TEXT PRIMARY KEY,
                source_type TEXT,
                source_id TEXT,
                url TEXT,
                title TEXT,
                published_at TEXT,
                relevance_score INTEGER
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO candidates (
                    item_id,
                    source_type,
                    source_id,
                    url,
                    title,
                    published_at,
                    relevance_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["item_id"],
                    row["source_type"],
                    row["source_id"],
                    row["url"],
                    row["title"],
                    row["published_at"],
                    row["relevance_score"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _candidate(
    *,
    item_id: str,
    source_type: str,
    source_id: str | None,
    url: str,
    relevance_score: int,
    title: str | None = None,
    published_at: str = ISO_PUBLISHED,
) -> dict:
    return {
        "item_id": item_id,
        "source_type": source_type,
        "source_id": source_id if source_id is not None else f"{source_type}_source",
        "url": url,
        "title": title or f"Title {item_id}",
        "published_at": published_at,
        "relevance_score": relevance_score,
    }


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _long_newsletter_result() -> tuple[str, list[EvidenceSnippet]]:
    text = "n" * 520
    snippets = [
        EvidenceSnippet(
            text=text[0:240],
            meta=EvidenceMeta(type="article", offset=0, timestamp=None),
        )
    ]
    return text, snippets


def _assert_report_invariants(report: dict) -> None:
    assert report["items_selected"] == report["success_count"] + report["failed_count"]
    assert report["success_count"] == report["inserted_db"] + report["skipped_already_enriched"]
    assert report["failed_count"] == sum(int(v) for v in report["fail_breakdown"].values())
    assert report["items_selected"] <= report["max_items"]
    assert report["items_selected"] <= report["candidates_available_total"]
    assert report["youtube_asr_succeeded"] <= report["youtube_asr_attempted"]
    assert report["youtube_preflight_executed"] == (report["selected_youtube_count"] > 0)
    assert report["youtube_transcripts_attempted"] == 0
    assert report["youtube_transcripts_succeeded"] == 0
    assert report["asr_fallbacks_used"] == 0
    assert report["fail_breakdown"]["youtube_transcript_unavailable"] == 0
    assert report["fail_breakdown"]["youtube_transcript_failed"] == 0
    assert report["fail_breakdown"]["transcript_cap_reached"] == 0
    assert report["fail_breakdown"]["asr_cap_reached"] == 0
    assert set(report["fail_breakdown"].keys()) == set(FAIL_BREAKDOWN_KEYS)
    if report["full_success_required"]:
        assert report["selected_invalid_count"] == 0
        if report["run_status"] == "completed":
            assert report["failed_count"] == 0


def test_asr_only_youtube_success_updates_counters_method_and_evidence_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "enriched.jsonl"
    report_path = tmp_path / "stage_4_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=True)

    url = "https://www.youtube.com/watch?v=AAAAAAAAAAA"
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-1",
                source_type="youtube",
                source_id="yt_source",
                url=url,
                relevance_score=9,
            )
        ],
    )

    runtime = FakeASRRuntime(default_text="y" * 900)
    monkeypatch.setattr(
        "app.enrich.runner._prepare_youtube_asr_runtime",
        lambda youtube_config: runtime,
    )

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.youtube_asr_attempted == 1
    assert result.youtube_asr_succeeded == 1
    assert result.youtube_transcripts_attempted == 0
    assert result.youtube_transcripts_succeeded == 0
    assert result.asr_fallbacks_used == 0
    assert result.youtube_preflight_executed is True

    rows = _read_jsonl(out_path)
    assert len(rows) == 1
    assert rows[0]["enrichment_method"] == "asr_faster_whisper"
    snippets = rows[0]["evidence_snippets"]
    assert [snippet["meta"]["offset"] for snippet in snippets] == [0, 240, 480]


def test_asr_runtime_invokes_audio_only_ytdlp_with_retry_timeout_and_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[list[str], int]] = []
    sleep_calls: list[float] = []
    download_attempts = {"count": 0}

    class FakeModel:
        def transcribe(self, audio_path: str, **kwargs):
            return ([types.SimpleNamespace(text="one"), types.SimpleNamespace(text="two")], None)

    def _fake_run(command, check, capture_output, text, timeout):
        calls.append((list(command), int(timeout)))
        if "--dump-single-json" in command:
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"duration": 120}\n',
                stderr="",
            )
        if "-x" in command:
            download_attempts["count"] += 1
            if download_attempts["count"] == 1:
                raise subprocess.CalledProcessError(returncode=1, cmd=command)
            output_template = Path(command[command.index("-o") + 1])
            audio_file = Path(str(output_template).replace("%(ext)s", "wav"))
            audio_file.parent.mkdir(parents=True, exist_ok=True)
            audio_file.write_bytes(b"audio")
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("app.enrich.youtube.subprocess.run", _fake_run)
    monkeypatch.setattr("app.enrich.youtube.time.sleep", lambda seconds: sleep_calls.append(seconds))

    runtime = ASRRuntime(
        model=FakeModel(),
        model_name="distil-large-v3",
        device_effective="cpu",
        compute_type_effective="int8",
        ytdlp_bin="yt-dlp",
        ffmpeg_bin="ffmpeg",
        decode=ASRDecodeConfig(
            language="en",
            beam_size=5,
            temperature=0.0,
            condition_on_previous_text=False,
            vad_filter=True,
        ),
        audio=ASRAudioConfig(
            format_selector="bestaudio",
            extract_format="wav",
            download_timeout_s=37,
            download_retries=1,
            retry_backoff_s=2.0,
        ),
        max_audio_seconds=7200,
    )

    normalized = runtime.transcribe_url("https://www.youtube.com/watch?v=AAAAAAAAAAA")
    assert normalized == "one two"
    assert sleep_calls == [2.0]
    assert download_attempts["count"] == 2

    metadata_calls = [command for command, _ in calls if "--dump-single-json" in command]
    download_calls = [command for command, _ in calls if "-x" in command]
    assert metadata_calls
    assert download_calls
    assert all(timeout == 37 for _, timeout in calls)

    first_download = download_calls[0]
    assert "-f" in first_download
    assert first_download[first_download.index("-f") + 1] == "bestaudio"
    assert "-x" in first_download
    assert "--audio-format" in first_download
    assert "--ffmpeg-location" in first_download
    assert "--no-playlist" in first_download


def test_initialize_asr_runtime_cuda_fallback_and_no_fallback_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_calls: list[dict[str, str]] = []

    class FakeWhisperModel:
        def __init__(self, model_name: str, *, device: str, compute_type: str):
            init_calls.append(
                {
                    "model_name": model_name,
                    "device": device,
                    "compute_type": compute_type,
                }
            )

    fake_ct2 = types.SimpleNamespace(get_cuda_device_count=lambda: 0)
    fake_fw = types.SimpleNamespace(WhisperModel=FakeWhisperModel)

    monkeypatch.setitem(sys.modules, "ctranslate2", fake_ct2)
    monkeypatch.setitem(sys.modules, "faster_whisper", fake_fw)

    with pytest.raises(ASRStartupError):
        initialize_asr_runtime(
            model_name="distil-large-v3",
            device="cuda",
            compute_type="float16",
            allow_cpu_fallback=False,
            ytdlp_bin="yt-dlp",
            ffmpeg_bin="ffmpeg",
            decode=ASRDecodeConfig(
                language="en",
                beam_size=5,
                temperature=0.0,
                condition_on_previous_text=False,
                vad_filter=True,
            ),
            audio=ASRAudioConfig(
                format_selector="bestaudio",
                extract_format="wav",
                download_timeout_s=180,
                download_retries=2,
                retry_backoff_s=2.0,
            ),
            max_audio_seconds=7200,
        )

    runtime = initialize_asr_runtime(
        model_name="distil-large-v3",
        device="cuda",
        compute_type="float16",
        allow_cpu_fallback=True,
        ytdlp_bin="yt-dlp",
        ffmpeg_bin="ffmpeg",
        decode=ASRDecodeConfig(
            language="en",
            beam_size=5,
            temperature=0.0,
            condition_on_previous_text=False,
            vad_filter=True,
        ),
        audio=ASRAudioConfig(
            format_selector="bestaudio",
            extract_format="wav",
            download_timeout_s=180,
            download_retries=2,
            retry_backoff_s=2.0,
        ),
        max_audio_seconds=7200,
    )
    assert runtime.device_effective == "cpu"
    assert runtime.compute_type_effective == "int8"
    assert init_calls[-1]["device"] == "cpu"
    assert init_calls[-1]["compute_type"] == "int8"


def test_selected_youtube_runs_preflight_once_and_reuses_runtime_for_multiple_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "enriched.jsonl"
    report_path = tmp_path / "stage_4_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-1",
                source_type="youtube",
                source_id="yt_source_1",
                url="https://youtube.com/watch?v=AAAAAAAAAAA",
                relevance_score=10,
            ),
            _candidate(
                item_id="yt-2",
                source_type="youtube",
                source_id="yt_source_2",
                url="https://youtube.com/watch?v=BBBBBBBBBBB",
                relevance_score=9,
            ),
        ],
    )

    runtime = FakeASRRuntime()
    preflight_calls = {"count": 0}

    def _prepare(youtube_config):
        preflight_calls["count"] += 1
        return runtime

    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", _prepare)

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert preflight_calls["count"] == 1
    assert len(runtime.calls) == 2
    assert result.youtube_asr_attempted == 2
    assert result.youtube_asr_succeeded == 2
    report = _read_json(report_path)
    assert report["youtube_preflight_executed"] is True


def test_selected_youtube_preflight_failure_is_fatal_and_sets_preflight_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-preflight",
                source_type="youtube",
                source_id="yt_source",
                url="https://youtube.com/watch?v=AAAAAAAAAAA",
                relevance_score=7,
            )
        ],
    )

    monkeypatch.setattr(
        "app.enrich.runner._prepare_youtube_asr_runtime",
        lambda youtube_config: (_ for _ in ()).throw(ConfigError("missing required executable: yt-dlp")),
    )

    with pytest.raises(FatalEnrichError):
        run_enrich(
            pipeline_path=str(pipeline),
            out_path=str(out_path),
            report_path=str(report_path),
        )

    report = _read_json(report_path)
    assert report["run_status"] == "fatal"
    assert report["youtube_preflight_executed"] is True


def test_zero_youtube_skips_preflight_and_emits_configured_asr_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(
        pipeline,
        db_path=db_path,
        outputs_dir=tmp_path,
        asr_device="cuda",
        asr_compute_type="float16",
    )
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="news-1",
                source_type="newsletter",
                source_id="news_source",
                url="https://example.com/news",
                relevance_score=6,
            )
        ],
    )
    monkeypatch.setattr(
        "app.enrich.runner._prepare_youtube_asr_runtime",
        lambda youtube_config: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )
    monkeypatch.setattr("app.enrich.runner.enrich_newsletter", lambda url, http_policy: _long_newsletter_result())

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.youtube_preflight_executed is False
    assert result.youtube_asr_model == "distil-large-v3"
    assert result.youtube_asr_device_effective == "cuda"
    assert result.youtube_asr_compute_type == "float16"


def test_require_full_success_true_fails_fast_and_fatal_payload_has_reason_and_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=True)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-bad",
                source_type="youtube",
                source_id="yt_source_1",
                url="https://youtube.com/watch?v=bad",
                relevance_score=10,
            ),
            _candidate(
                item_id="yt-good",
                source_type="youtube",
                source_id="yt_source_2",
                url="https://youtube.com/watch?v=BBBBBBBBBBB",
                relevance_score=9,
            ),
        ],
    )

    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: FakeASRRuntime())

    with pytest.raises(FatalEnrichError):
        run_enrich(
            pipeline_path=str(pipeline),
            out_path=str(out_path),
            report_path=str(report_path),
        )

    report = _read_json(report_path)
    assert report["run_status"] == "fatal"
    assert "reason=youtube_video_id_parse_failed" in report["fatal_error"]
    assert "item_id=yt-bad" in report["fatal_error"]
    assert report["items_selected"] == 1
    assert report["fail_breakdown"]["youtube_video_id_parse_failed"] == 1


def test_require_full_success_true_excludes_invalid_pool_rows_from_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "enriched.jsonl"
    report_path = tmp_path / "stage_4_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=True, max_items_default=2)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="invalid-row",
                source_type="podcast",
                source_id="bad_source",
                url="https://example.com/podcast",
                relevance_score=100,
            ),
            _candidate(
                item_id="news-good",
                source_type="newsletter",
                source_id="news_source",
                url="https://example.com/news",
                relevance_score=90,
            ),
        ],
    )
    monkeypatch.setattr("app.enrich.runner.enrich_newsletter", lambda url, http_policy: _long_newsletter_result())

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    assert result.selected_invalid_count == 0
    assert result.fail_breakdown["invalid_candidate_row"] == 0
    assert [row["item_id"] for row in _read_jsonl(out_path)] == ["news-good"]


def test_require_full_success_false_allows_continue_after_item_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "enriched.jsonl"
    report_path = tmp_path / "stage_4_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-bad",
                source_type="youtube",
                source_id="yt_source_1",
                url="https://youtube.com/watch?v=bad",
                relevance_score=100,
            ),
            _candidate(
                item_id="yt-good",
                source_type="youtube",
                source_id="yt_source_2",
                url="https://youtube.com/watch?v=BBBBBBBBBBB",
                relevance_score=99,
            ),
        ],
    )
    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: FakeASRRuntime())

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    assert result.run_status == "completed"
    assert result.failed_count == 1
    assert result.success_count == 1
    assert result.fail_breakdown["youtube_video_id_parse_failed"] == 1


def test_transcript_legacy_counters_and_fail_keys_remain_zero_in_asr_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-1",
                source_type="youtube",
                source_id="yt_source",
                url="https://youtube.com/watch?v=AAAAAAAAAAA",
                relevance_score=8,
            )
        ],
    )
    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: FakeASRRuntime())

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    report = _read_json(report_path)

    assert result.youtube_transcripts_attempted == 0
    assert result.youtube_transcripts_succeeded == 0
    assert result.asr_fallbacks_used == 0
    assert report["fail_breakdown"]["youtube_transcript_unavailable"] == 0
    assert report["fail_breakdown"]["youtube_transcript_failed"] == 0
    assert report["fail_breakdown"]["transcript_cap_reached"] == 0
    assert report["fail_breakdown"]["asr_cap_reached"] == 0


def test_youtube_threshold_edges_and_empty_text_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "enriched.jsonl"
    report_path = tmp_path / "stage_4_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False, asr_min_chars=800)

    url_empty = "https://youtube.com/watch?v=AAAAAAAAAAA"
    url_799 = "https://youtube.com/watch?v=BBBBBBBBBBB"
    url_800 = "https://youtube.com/watch?v=CCCCCCCCCCC"
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-empty",
                source_type="youtube",
                source_id="yt_source_1",
                url=url_empty,
                relevance_score=100,
            ),
            _candidate(
                item_id="yt-799",
                source_type="youtube",
                source_id="yt_source_2",
                url=url_799,
                relevance_score=90,
            ),
            _candidate(
                item_id="yt-800",
                source_type="youtube",
                source_id="yt_source_3",
                url=url_800,
                relevance_score=80,
            ),
        ],
    )

    runtime = FakeASRRuntime(
        text_by_url={url_799: "x" * 799, url_800: "x" * 800},
        error_by_url={url_empty: ASRError("empty")},
    )
    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: runtime)

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )

    assert result.youtube_asr_attempted == 3
    assert result.youtube_asr_succeeded == 2
    assert result.fail_breakdown["youtube_asr_failed"] == 1
    assert result.fail_breakdown["youtube_text_too_short"] == 1
    assert [row["item_id"] for row in _read_jsonl(out_path)] == ["yt-800"]


def test_uncaught_youtube_exception_maps_to_youtube_asr_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-err",
                source_type="youtube",
                source_id="yt_source",
                url="https://youtube.com/watch?v=AAAAAAAAAAA",
                relevance_score=4,
            )
        ],
    )
    monkeypatch.setattr(
        "app.enrich.runner._prepare_youtube_asr_runtime",
        lambda youtube_config: FakeASRRuntime(
            error_by_url={"https://youtube.com/watch?v=AAAAAAAAAAA": RuntimeError("boom")}
        ),
    )

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    assert result.fail_breakdown["youtube_asr_failed"] == 1
    assert result.fail_breakdown["youtube_transcript_failed"] == 0


def test_cli_cap_overrides_are_accepted_reported_and_warned_in_asr_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-cap",
                source_type="youtube",
                source_id="yt_source",
                url="https://youtube.com/watch?v=AAAAAAAAAAA",
                relevance_score=3,
            )
        ],
    )
    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: FakeASRRuntime())

    caplog.set_level("WARNING")
    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
        max_transcripts_override=77,
        max_asr_override=88,
    )

    assert result.max_transcripts == 77
    assert result.max_asr == 88
    assert result.youtube_asr_attempted == 1
    warning_messages = " ".join(record.getMessage() for record in caplog.records)
    assert "ignoring --max-transcripts override" in warning_messages
    assert "ignoring --max-asr override" in warning_messages


def test_report_invariants_hold_for_mixed_outcomes_in_asr_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    out_path = tmp_path / "enriched.jsonl"
    report_path = tmp_path / "stage_4_report.json"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-bad-parse",
                source_type="youtube",
                source_id="yt_source_1",
                url="https://youtube.com/watch?v=bad",
                relevance_score=100,
            ),
            _candidate(
                item_id="yt-good",
                source_type="youtube",
                source_id="yt_source_2",
                url="https://youtube.com/watch?v=BBBBBBBBBBB",
                relevance_score=90,
            ),
            _candidate(
                item_id="news-good",
                source_type="newsletter",
                source_id="news_source",
                url="https://example.com/news",
                relevance_score=80,
            ),
        ],
    )
    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: FakeASRRuntime())
    monkeypatch.setattr("app.enrich.runner.enrich_newsletter", lambda url, http_policy: _long_newsletter_result())

    run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    report = _read_json(report_path)
    _assert_report_invariants(report)


def test_asr_runtime_duration_guard_blocks_long_or_invalid_probe_without_transcribe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transcribe_calls = {"count": 0}

    class FakeModel:
        def transcribe(self, audio_path: str, **kwargs):
            transcribe_calls["count"] += 1
            return ([types.SimpleNamespace(text="ok")], None)

    def _probe_too_long(command, check, capture_output, text, timeout):
        if "--dump-single-json" in command:
            return types.SimpleNamespace(returncode=0, stdout='{"duration": 9000}\n', stderr="")
        raise AssertionError("download should not execute when duration guard fails")

    runtime = ASRRuntime(
        model=FakeModel(),
        model_name="distil-large-v3",
        device_effective="cpu",
        compute_type_effective="int8",
        ytdlp_bin="yt-dlp",
        ffmpeg_bin="ffmpeg",
        decode=ASRDecodeConfig(
            language="en",
            beam_size=5,
            temperature=0.0,
            condition_on_previous_text=False,
            vad_filter=True,
        ),
        audio=ASRAudioConfig(
            format_selector="bestaudio",
            extract_format="wav",
            download_timeout_s=30,
            download_retries=0,
            retry_backoff_s=1.0,
        ),
        max_audio_seconds=7200,
    )
    monkeypatch.setattr("app.enrich.youtube.subprocess.run", _probe_too_long)
    with pytest.raises(ASRError):
        runtime.transcribe_url("https://youtube.com/watch?v=AAAAAAAAAAA")
    assert transcribe_calls["count"] == 0

    def _probe_invalid_duration(command, check, capture_output, text, timeout):
        return types.SimpleNamespace(returncode=0, stdout='{"duration": "oops"}\n', stderr="")

    monkeypatch.setattr("app.enrich.youtube.subprocess.run", _probe_invalid_duration)
    with pytest.raises(ASRError):
        runtime.transcribe_url("https://youtube.com/watch?v=AAAAAAAAAAA")


def test_runner_attempt_counter_increments_once_on_asr_runtime_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    db_path = tmp_path / "state.db"
    report_path = tmp_path / "stage_4_report.json"
    out_path = tmp_path / "enriched.jsonl"
    _write_pipeline(pipeline, db_path=db_path, outputs_dir=tmp_path, require_full_success=False)
    _seed_candidates(
        db_path,
        [
            _candidate(
                item_id="yt-1",
                source_type="youtube",
                source_id="yt_source_1",
                url="https://youtube.com/watch?v=AAAAAAAAAAA",
                relevance_score=10,
            ),
            _candidate(
                item_id="yt-2",
                source_type="youtube",
                source_id="yt_source_2",
                url="https://youtube.com/watch?v=BBBBBBBBBBB",
                relevance_score=9,
            ),
        ],
    )

    runtime = FakeASRRuntime(
        error_by_url={
            "https://youtube.com/watch?v=AAAAAAAAAAA": ASRError("metadata probe failed"),
            "https://youtube.com/watch?v=BBBBBBBBBBB": ASRError("metadata probe failed"),
        }
    )
    monkeypatch.setattr("app.enrich.runner._prepare_youtube_asr_runtime", lambda youtube_config: runtime)

    result = run_enrich(
        pipeline_path=str(pipeline),
        out_path=str(out_path),
        report_path=str(report_path),
    )
    assert result.youtube_asr_attempted == 2
    assert result.fail_breakdown["youtube_asr_failed"] == 2
