from __future__ import annotations

import importlib.util
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlparse

from .models import EvidenceMeta, EvidenceSnippet, TRANSCRIPT_LANGUAGES, normalize_text


VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
VTT_TIME_RE = re.compile(
    r"^(?:(?P<h>\d{2,}):)?(?P<m>\d{2}):(?P<s>\d{2})(?:\.(?P<ms>\d{3}))?$"
)
YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com"}
UNAVAILABLE_EXCEPTION_CLASS_NAMES = {
    "NoTranscriptFound",
    "NoTranscriptAvailable",
    "TranscriptsDisabled",
    "VideoUnavailable",
    "InvalidVideoId",
    "TooManyRequests",
    "RequestBlocked",
    "IpBlocked",
    "FailedToCreateConsentCookie",
}
LOGGER = logging.getLogger(__name__)


class VideoIdParseError(ValueError):
    pass


class ASRError(RuntimeError):
    pass


class ASRStartupError(RuntimeError):
    pass


@dataclass(frozen=True)
class TranscriptSegment:
    text: str
    start: Any


@dataclass(frozen=True)
class TranscriptLookupResult:
    status: str
    normalized_text: str
    segments: list[TranscriptSegment]


@dataclass(frozen=True)
class ASRDecodeConfig:
    language: str
    beam_size: int
    temperature: float
    condition_on_previous_text: bool
    vad_filter: bool


@dataclass(frozen=True)
class ASRAudioConfig:
    format_selector: str
    extract_format: str
    download_timeout_s: int
    download_retries: int
    retry_backoff_s: float


@dataclass
class ASRRuntime:
    model: Any
    model_name: str
    device_effective: str
    compute_type_effective: str
    ytdlp_bin: str
    ffmpeg_bin: str
    decode: ASRDecodeConfig
    audio: ASRAudioConfig
    max_audio_seconds: int

    def transcribe_url(self, url: str) -> str:
        self._probe_duration(url=url)
        with tempfile.TemporaryDirectory(prefix="stage4_asr_") as tmpdir:
            temp_dir = Path(tmpdir)
            audio_path = self._download_audio(url=url, temp_dir=temp_dir)
            return self._transcribe_audio(audio_path=audio_path)

    def _probe_duration(self, *, url: str) -> None:
        command = [
            self.ytdlp_bin,
            "--quiet",
            "--no-warnings",
            "--dump-single-json",
            "--no-playlist",
            url,
        ]
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=self.audio.download_timeout_s,
            )
        except Exception as exc:
            raise ASRError("yt-dlp metadata probe failed") from exc

        payload = _parse_probe_json_payload(result.stdout)
        duration = payload.get("duration")
        if not _is_non_bool_number(duration) or float(duration) <= 0:
            raise ASRError("yt-dlp metadata duration missing/invalid")
        if float(duration) > float(self.max_audio_seconds):
            raise ASRError("yt-dlp metadata duration exceeds max_audio_seconds")

    def _download_audio(self, *, url: str, temp_dir: Path) -> Path:
        output_template = temp_dir / "audio.%(ext)s"
        total_attempts = 1 + self.audio.download_retries
        for attempt in range(total_attempts):
            _cleanup_temp_audio(temp_dir=temp_dir)
            command = [
                self.ytdlp_bin,
                "--quiet",
                "--no-warnings",
                "--no-playlist",
                "-f",
                self.audio.format_selector,
                "-x",
                "--audio-format",
                self.audio.extract_format,
                "--ffmpeg-location",
                self.ffmpeg_bin,
                "-o",
                str(output_template),
                url,
            ]
            try:
                subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=self.audio.download_timeout_s,
                )
                audio_files = sorted(temp_dir.glob("audio.*"))
                if not audio_files:
                    raise ASRError("yt-dlp produced no audio output")
                return audio_files[0]
            except Exception as exc:
                if attempt >= total_attempts - 1:
                    raise ASRError("yt-dlp audio extraction failed") from exc
                backoff_s = self.audio.retry_backoff_s * (2**attempt)
                time.sleep(backoff_s)
        raise ASRError("yt-dlp audio extraction failed")

    def _transcribe_audio(self, *, audio_path: Path) -> str:
        try:
            segments, _ = self.model.transcribe(
                str(audio_path),
                language=self.decode.language,
                beam_size=self.decode.beam_size,
                temperature=self.decode.temperature,
                condition_on_previous_text=self.decode.condition_on_previous_text,
                vad_filter=self.decode.vad_filter,
            )
        except Exception as exc:
            raise ASRError("ASR transcription failed") from exc

        text_parts: list[str] = []
        for segment in segments:
            segment_text = getattr(segment, "text", None)
            if isinstance(segment_text, str) and segment_text.strip():
                text_parts.append(segment_text)

        normalized = normalize_text(" ".join(text_parts))
        if not normalized:
            raise ASRError("ASR returned empty text")
        return normalized


def parse_video_id(url: str) -> str:
    normalized_url = url.strip()
    parsed = urlparse(normalized_url)

    if parsed.scheme not in {"http", "https"}:
        raise VideoIdParseError("youtube URL scheme must be http/https")
    host = (parsed.hostname or "").lower()
    if not host:
        raise VideoIdParseError("youtube URL host is required")

    candidate_id = ""
    if host in YOUTUBE_HOSTS:
        candidate_id = _first_non_empty_v_param(parsed.query)
    if not candidate_id and host in YOUTUBE_HOSTS:
        candidate_id = _path_id_after_prefix(parsed.path, "shorts")
    if not candidate_id and host == "youtu.be":
        candidate_id = _first_path_segment(parsed.path)
    if not candidate_id and host in YOUTUBE_HOSTS:
        candidate_id = _path_id_after_prefix(parsed.path, "embed")

    if not VIDEO_ID_RE.fullmatch(candidate_id):
        raise VideoIdParseError("invalid youtube video id")
    return candidate_id


def fetch_transcript(video_id: str) -> TranscriptLookupResult:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except Exception as exc:
        raise RuntimeError("youtube-transcript-api import failed") from exc

    cookies, proxy_pool = _resolve_transcript_provider_network_options()
    retry_count, base_backoff_s = _resolve_transcript_retry_policy()
    provider_statuses: list[str] = []
    for proxies in _ordered_proxy_attempts_for_video(video_id=video_id, proxy_pool=proxy_pool):
        for attempt in range(retry_count + 1):
            result = _fetch_transcript_with_provider(
                video_id=video_id,
                cookies=cookies,
                proxies=proxies,
                youtube_transcript_api_cls=YouTubeTranscriptApi,
            )
            if result.status == "success":
                return result
            provider_statuses.append(result.status)

            # Incremental waiting between retries to reduce burst pressure when rate-limited.
            if result.status == "unavailable" and attempt < retry_count:
                time.sleep(base_backoff_s * (2**attempt))
                continue
            break

    fallback = _fetch_transcript_with_ytdlp(video_id)
    if fallback is not None:
        return fallback

    if "unavailable" in provider_statuses:
        return TranscriptLookupResult(status="unavailable", normalized_text="", segments=[])
    return TranscriptLookupResult(status="failed", normalized_text="", segments=[])


def _fetch_transcript_with_provider(
    *,
    video_id: str,
    cookies: str | None,
    proxies: dict[str, str] | None,
    youtube_transcript_api_cls: Any,
) -> TranscriptLookupResult:
    try:
        transcript_list = youtube_transcript_api_cls.list_transcripts(
            video_id,
            proxies=proxies,
            cookies=cookies,
        )
    except Exception as exc:
        if _is_unavailable_exception(exc):
            return TranscriptLookupResult(status="unavailable", normalized_text="", segments=[])
        return TranscriptLookupResult(status="failed", normalized_text="", segments=[])

    transcript_obj: Any | None = None
    manual_outcome = _find_transcript_by_language_order(
        transcript_list=transcript_list,
        languages=TRANSCRIPT_LANGUAGES,
        find_generated=False,
    )
    if manual_outcome["status"] == "failed":
        return TranscriptLookupResult(status="failed", normalized_text="", segments=[])
    transcript_obj = manual_outcome["transcript"]

    if transcript_obj is None:
        generated_outcome = _find_transcript_by_language_order(
            transcript_list=transcript_list,
            languages=TRANSCRIPT_LANGUAGES,
            find_generated=True,
        )
        if generated_outcome["status"] == "failed":
            return TranscriptLookupResult(status="failed", normalized_text="", segments=[])
        transcript_obj = generated_outcome["transcript"]

    if transcript_obj is None:
        return TranscriptLookupResult(status="unavailable", normalized_text="", segments=[])

    try:
        payload = transcript_obj.fetch()
    except Exception as exc:
        if _is_unavailable_exception(exc):
            return TranscriptLookupResult(status="unavailable", normalized_text="", segments=[])
        return TranscriptLookupResult(status="failed", normalized_text="", segments=[])

    status, normalized, segments = _assemble_transcript_payload(payload)
    return TranscriptLookupResult(status=status, normalized_text=normalized, segments=segments)


def build_transcript_evidence(segments: list[TranscriptSegment]) -> list[EvidenceSnippet]:
    snippets: list[EvidenceSnippet] = []
    for segment in segments:
        normalized = normalize_text(segment.text)
        if not normalized:
            continue
        snippets.append(
            EvidenceSnippet(
                text=normalized[:240],
                meta=EvidenceMeta(
                    type="transcript",
                    offset=None,
                    timestamp=_format_timestamp(segment.start),
                ),
            )
        )
        if len(snippets) == 3:
            break
    return snippets


def build_asr_evidence(enriched_text: str) -> list[EvidenceSnippet]:
    snippets: list[EvidenceSnippet] = []
    windows = ((0, 240), (240, 480), (480, 720))
    for start, end in windows:
        chunk = enriched_text[start:end]
        if not chunk:
            continue
        snippets.append(
            EvidenceSnippet(
                text=chunk,
                meta=EvidenceMeta(type="transcript", offset=start, timestamp=None),
            )
        )
    return snippets


def resolve_executable(*, env_name: str, candidates: tuple[str, ...]) -> str | None:
    return _resolve_executable(env_name=env_name, candidates=candidates)


def initialize_asr_runtime(
    *,
    model_name: str,
    device: str,
    compute_type: str,
    allow_cpu_fallback: bool,
    ytdlp_bin: str,
    ffmpeg_bin: str,
    decode: ASRDecodeConfig,
    audio: ASRAudioConfig,
    max_audio_seconds: int,
) -> ASRRuntime:
    try:
        import ctranslate2
    except Exception as exc:
        raise ASRStartupError("ctranslate2 import failed") from exc
    try:
        from faster_whisper import WhisperModel
    except Exception as exc:
        raise ASRStartupError("faster-whisper import failed") from exc

    effective_device = device
    effective_compute_type = compute_type
    if device == "cuda":
        cuda_count = _get_cuda_device_count(ctranslate2)
        if cuda_count < 1:
            if not allow_cpu_fallback:
                raise ASRStartupError("cuda unavailable and cpu fallback disabled")
            effective_device = "cpu"
            effective_compute_type = "int8"
            LOGGER.warning(
                "stage_4_enrich youtube_asr cuda unavailable; falling back to cpu/int8"
            )

    try:
        model = WhisperModel(
            model_name,
            device=effective_device,
            compute_type=effective_compute_type,
        )
    except Exception as exc:
        raise ASRStartupError(
            f"failed to initialize WhisperModel model={model_name} device={effective_device} "
            f"compute_type={effective_compute_type}"
        ) from exc

    return ASRRuntime(
        model=model,
        model_name=model_name,
        device_effective=effective_device,
        compute_type_effective=effective_compute_type,
        ytdlp_bin=ytdlp_bin,
        ffmpeg_bin=ffmpeg_bin,
        decode=decode,
        audio=audio,
        max_audio_seconds=max_audio_seconds,
    )


def check_asr_prerequisites() -> tuple[bool, str | None]:
    missing: list[str] = []
    if importlib.util.find_spec("faster_whisper") is None:
        missing.append("faster-whisper")
    ytdlp_bin = _resolve_executable(
        env_name="YOUTUBE_YTDLP_BIN",
        candidates=("yt-dlp", "yt-dlp.exe"),
    )
    ffmpeg_bin = _resolve_executable(
        env_name="YOUTUBE_FFMPEG_BIN",
        candidates=("ffmpeg", "ffmpeg.exe"),
    )
    if ytdlp_bin is None:
        missing.append("yt-dlp")
    if ffmpeg_bin is None:
        missing.append("ffmpeg")
    if missing:
        return False, ", ".join(missing)
    return True, None


def transcribe_with_asr(url: str) -> str:
    try:
        from faster_whisper import WhisperModel
    except Exception as exc:
        raise ASRError("faster-whisper import failed") from exc

    ytdlp_bin = _resolve_executable(
        env_name="YOUTUBE_YTDLP_BIN",
        candidates=("yt-dlp", "yt-dlp.exe"),
    )
    ffmpeg_bin = _resolve_executable(
        env_name="YOUTUBE_FFMPEG_BIN",
        candidates=("ffmpeg", "ffmpeg.exe"),
    )
    if ytdlp_bin is None:
        raise ASRError("yt-dlp executable not found")
    if ffmpeg_bin is None:
        raise ASRError("ffmpeg executable not found")

    with tempfile.TemporaryDirectory(prefix="stage4_asr_") as tmpdir:
        temp_dir = Path(tmpdir)
        output_template = temp_dir / "audio.%(ext)s"
        command = [
            ytdlp_bin,
            "--quiet",
            "--no-warnings",
            "--no-playlist",
            "--ffmpeg-location",
            ffmpeg_bin,
            "-x",
            "--audio-format",
            "wav",
            "-o",
            str(output_template),
            url,
        ]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except Exception as exc:
            raise ASRError("yt-dlp audio extraction failed") from exc

        audio_files = sorted(temp_dir.glob("audio.*"))
        if not audio_files:
            raise ASRError("yt-dlp produced no audio output")
        audio_path = audio_files[0]

        try:
            model = WhisperModel("small", device="cpu")
            segments, _ = model.transcribe(
                str(audio_path),
                language="en",
                beam_size=5,
                temperature=0.0,
                condition_on_previous_text=False,
                vad_filter=True,
            )
            text_parts: list[str] = []
            for segment in segments:
                segment_text = getattr(segment, "text", None)
                if isinstance(segment_text, str) and segment_text.strip():
                    text_parts.append(segment_text)
        except Exception as exc:
            raise ASRError("ASR transcription failed") from exc

    normalized = normalize_text(" ".join(text_parts))
    if not normalized:
        raise ASRError("ASR returned empty text")
    return normalized


def _assemble_transcript_payload(
    payload: Any,
) -> tuple[str, str, list[TranscriptSegment]]:
    try:
        iterator: Iterable[Any] = iter(payload)
    except TypeError:
        return "failed", "", []

    collected_segments: list[TranscriptSegment] = []
    text_parts: list[str] = []
    for segment in iterator:
        if not isinstance(segment, dict):
            return "failed", "", []
        text_value = segment.get("text")
        if isinstance(text_value, str) and text_value.strip():
            collected_segments.append(TranscriptSegment(text=text_value, start=segment.get("start")))
            text_parts.append(text_value)

    if not text_parts:
        return "unavailable", "", []

    joined = " ".join(text_parts)
    normalized = normalize_text(joined)
    if not normalized:
        return "unavailable", "", []

    return "success", normalized, collected_segments


def _find_transcript_by_language_order(
    *,
    transcript_list: Any,
    languages: tuple[str, ...],
    find_generated: bool,
) -> dict[str, Any]:
    method_name = "find_generated_transcript" if find_generated else "find_manually_created_transcript"
    finder = getattr(transcript_list, method_name, None)
    if finder is None:
        return {"status": "failed", "transcript": None}

    for language in languages:
        try:
            transcript = finder([language])
            return {"status": "success", "transcript": transcript}
        except Exception as exc:
            if _is_unavailable_exception(exc):
                continue
            return {"status": "failed", "transcript": None}
    return {"status": "unavailable", "transcript": None}


def _is_unavailable_exception(exc: Exception) -> bool:
    if exc.__class__.__name__ in UNAVAILABLE_EXCEPTION_CLASS_NAMES:
        return True

    # YouTube sometimes returns non-XML/empty timedtext payloads under throttling,
    # which surface as ElementTree.ParseError during transcript fetch.
    if exc.__class__.__name__ == "ParseError" and exc.__class__.__module__ == "xml.etree.ElementTree":
        return True

    # youtube-transcript-api surfaces throttling and some blocking cases as YouTubeRequestFailed.
    # Treat transient HTTP failures as unavailable so retries/proxy rotation/subtitle fallback can run.
    if exc.__class__.__name__ == "YouTubeRequestFailed":
        status_code = _extract_youtube_request_failed_status_code(exc)
        if status_code in {403, 429}:
            return True
        if isinstance(status_code, int) and status_code >= 500:
            return True
        if status_code is None:
            return True

    return False


def _extract_youtube_request_failed_status_code(exc: Exception) -> int | None:
    http_error = getattr(exc, "video_id", None)
    response = getattr(http_error, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    return None


def _format_timestamp(value: Any) -> str | None:
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return None


def _first_non_empty_v_param(query: str) -> str:
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key == "v":
            candidate = value.strip()
            if candidate:
                return candidate
    return ""


def _path_id_after_prefix(path: str, prefix: str) -> str:
    parts = [part for part in path.split("/") if part]
    if len(parts) < 2:
        return ""
    if parts[0].lower() != prefix:
        return ""
    return parts[1].strip()


def _first_path_segment(path: str) -> str:
    parts = [part for part in path.split("/") if part]
    if not parts:
        return ""
    return parts[0].strip()


def _resolve_transcript_provider_network_options() -> tuple[str | None, list[dict[str, str]]]:
    cookies = _optional_non_empty_env("YOUTUBE_TRANSCRIPT_API_COOKIES")
    proxy_pool_raw = _optional_non_empty_env("YOUTUBE_TRANSCRIPT_API_PROXIES")
    single_proxy = _optional_non_empty_env("YOUTUBE_TRANSCRIPT_API_PROXY")
    proxy_pool: list[str] = []

    if proxy_pool_raw is not None:
        for value in re.split(r"[\s,;]+", proxy_pool_raw):
            normalized = value.strip()
            if normalized:
                proxy_pool.append(normalized)
    elif single_proxy is not None:
        proxy_pool.append(single_proxy)

    deduped_pool: list[str] = []
    seen: set[str] = set()
    for proxy in proxy_pool:
        if proxy in seen:
            continue
        seen.add(proxy)
        deduped_pool.append(proxy)

    proxy_dicts = [{"http": proxy, "https": proxy} for proxy in deduped_pool]
    return cookies, proxy_dicts


def _optional_non_empty_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _resolve_transcript_retry_policy() -> tuple[int, float]:
    retries_raw = _optional_non_empty_env("YOUTUBE_TRANSCRIPT_API_RETRIES")
    base_wait_raw = _optional_non_empty_env("YOUTUBE_TRANSCRIPT_API_RETRY_BASE_WAIT_S")

    retries = 2
    if retries_raw is not None:
        try:
            parsed = int(retries_raw)
            if parsed >= 0:
                retries = parsed
        except ValueError:
            pass

    base_wait_s = 1.0
    if base_wait_raw is not None:
        try:
            parsed = float(base_wait_raw)
            if parsed > 0:
                base_wait_s = parsed
        except ValueError:
            pass

    return retries, base_wait_s


def _ordered_proxy_attempts_for_video(
    *, video_id: str, proxy_pool: list[dict[str, str]]
) -> list[dict[str, str] | None]:
    if not proxy_pool:
        return [None]

    size = len(proxy_pool)
    stable_hash = int(hashlib.sha256(video_id.encode("utf-8")).hexdigest(), 16)
    start = stable_hash % size
    ordered = proxy_pool[start:] + proxy_pool[:start]
    # Always keep a direct attempt at the end in case proxies are down.
    ordered.append(None)
    return ordered


def _fetch_transcript_with_ytdlp(video_id: str) -> TranscriptLookupResult | None:
    ytdlp_bin = _resolve_executable(
        env_name="YOUTUBE_YTDLP_BIN",
        candidates=("yt-dlp", "yt-dlp.exe"),
    )
    if ytdlp_bin is None:
        return None

    url = f"https://www.youtube.com/watch?v={video_id}"
    with tempfile.TemporaryDirectory(prefix="stage4_subs_") as tmpdir:
        output_template = str(Path(tmpdir) / "%(id)s.%(ext)s")
        command = [
            ytdlp_bin,
            "--ignore-config",
            "--ignore-errors",
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            "en,en-US,en-GB,en.*",
            "--sub-format",
            "vtt/best",
            *_build_ytdlp_ffmpeg_args(),
            *_build_ytdlp_auth_args(),
            *_build_ytdlp_backoff_args(),
            "-o",
            output_template,
            url,
        ]
        try:
            subprocess.run(command, check=False, capture_output=True, text=True)
        except Exception:
            return None

        candidates = sorted(Path(tmpdir).glob(f"{video_id}*.vtt"))
        for subtitle_path in candidates:
            try:
                raw_text = subtitle_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                raw_text = subtitle_path.read_text(encoding="utf-8-sig")
            segments = _parse_vtt_segments(raw_text)
            if not segments:
                continue
            normalized = normalize_text(" ".join(segment.text for segment in segments))
            if not normalized:
                continue
            return TranscriptLookupResult(
                status="success",
                normalized_text=normalized,
                segments=segments,
            )
    return None


def _parse_vtt_segments(vtt_text: str) -> list[TranscriptSegment]:
    lines = vtt_text.splitlines()
    segments: list[TranscriptSegment] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx].strip().lstrip("\ufeff")
        if "-->" not in line:
            idx += 1
            continue

        start_raw = line.split("-->", 1)[0].strip()
        start_value = _parse_vtt_timestamp(start_raw)
        idx += 1

        text_lines: list[str] = []
        while idx < len(lines):
            raw = lines[idx].strip()
            if not raw:
                break
            cleaned = _strip_vtt_markup(raw)
            if cleaned:
                text_lines.append(cleaned)
            idx += 1

        cue_text = normalize_text(" ".join(text_lines))
        if cue_text:
            segments.append(TranscriptSegment(text=cue_text, start=start_value))
        idx += 1
    return segments


def _strip_vtt_markup(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw)
    return normalize_text(text)


def _parse_vtt_timestamp(raw: str) -> float | None:
    match = VTT_TIME_RE.match(raw.strip())
    if match is None:
        return None
    hours = int(match.group("h") or 0)
    minutes = int(match.group("m") or 0)
    seconds = int(match.group("s") or 0)
    millis = int(match.group("ms") or 0)
    return float(hours * 3600 + minutes * 60 + seconds + (millis / 1000))


def _build_ytdlp_auth_args() -> list[str]:
    cookies_file = _optional_non_empty_env("YOUTUBE_YTDLP_COOKIES_FILE")
    if cookies_file is not None:
        return ["--cookies", cookies_file]

    cookies_from_browser = _optional_non_empty_env("YOUTUBE_YTDLP_COOKIES_FROM_BROWSER")
    if cookies_from_browser is not None:
        return ["--cookies-from-browser", cookies_from_browser]
    return []


def _build_ytdlp_backoff_args() -> list[str]:
    sleep_interval = _optional_positive_float_env("YOUTUBE_YTDLP_SLEEP_INTERVAL_S")
    max_sleep_interval = _optional_positive_float_env("YOUTUBE_YTDLP_MAX_SLEEP_INTERVAL_S")
    args: list[str] = []
    if sleep_interval is not None:
        args.extend(["--sleep-interval", str(sleep_interval)])
    if max_sleep_interval is not None:
        args.extend(["--max-sleep-interval", str(max_sleep_interval)])
    return args


def _build_ytdlp_ffmpeg_args() -> list[str]:
    ffmpeg_bin = _resolve_executable(
        env_name="YOUTUBE_FFMPEG_BIN",
        candidates=("ffmpeg", "ffmpeg.exe"),
    )
    if ffmpeg_bin is None:
        return []
    return ["--ffmpeg-location", ffmpeg_bin]


def _parse_probe_json_payload(raw_stdout: str) -> dict[str, Any]:
    lines = [line.strip() for line in raw_stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise ASRError("yt-dlp metadata probe returned invalid json")


def _is_non_bool_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    return isinstance(value, (int, float))


def _cleanup_temp_audio(*, temp_dir: Path) -> None:
    for path in temp_dir.glob("audio.*"):
        try:
            path.unlink()
        except OSError:
            continue


def _get_cuda_device_count(ctranslate2_module: Any) -> int:
    getter = getattr(ctranslate2_module, "get_cuda_device_count", None)
    if not callable(getter):
        return 0
    try:
        count = getter()
    except Exception:
        return 0
    if isinstance(count, int) and count > 0:
        return count
    return 0


def _optional_positive_float_env(name: str) -> float | None:
    value = _optional_non_empty_env(name)
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def _resolve_executable(*, env_name: str, candidates: tuple[str, ...]) -> str | None:
    explicit = _optional_non_empty_env(env_name)
    if explicit is not None:
        if Path(explicit).exists():
            return explicit
        resolved_explicit = shutil.which(explicit)
        if resolved_explicit:
            return resolved_explicit

    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved

    # Fallback for virtualenv runs where PATH does not include Scripts/.
    python_scripts_dir = Path(sys.executable).resolve().parent
    for candidate in candidates:
        local_candidate = python_scripts_dir / candidate
        if local_candidate.exists():
            return str(local_candidate)
    return None
