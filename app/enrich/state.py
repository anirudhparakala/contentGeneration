from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import EnrichedItem


REQUIRED_CANDIDATE_COLUMNS = {
    "item_id",
    "source_type",
    "source_id",
    "url",
    "title",
    "published_at",
    "relevance_score",
}


class StateError(RuntimeError):
    pass


@dataclass
class EnrichStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize sqlite db at {self.db_path}") from exc

    def close(self) -> None:
        self._conn.close()

    def ensure_enriched_items_table(self) -> None:
        try:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS enriched_items (
                    item_id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    enriched_text TEXT NOT NULL,
                    evidence_snippets TEXT NOT NULL,
                    enrichment_method TEXT NOT NULL,
                    enriched_at TEXT NOT NULL,
                    inserted_at TEXT NOT NULL
                )
                """
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateError("failed creating enriched_items table") from exc

    def ensure_retry_state_table(self) -> None:
        try:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS enrich_retry_state (
                    item_id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    attempts_total INTEGER NOT NULL,
                    consecutive_failures INTEGER NOT NULL,
                    last_outcome TEXT NOT NULL,
                    last_fail_reason TEXT NULL,
                    last_attempt_at TEXT NOT NULL,
                    next_eligible_at TEXT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateError("failed creating enrich_retry_state table") from exc

    def validate_candidates_table(self) -> None:
        try:
            exists = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='candidates' LIMIT 1"
            ).fetchone()
            if exists is None:
                raise StateError("candidates table is missing")

            rows = self._conn.execute("PRAGMA table_info(candidates)").fetchall()
        except sqlite3.Error as exc:
            raise StateError("failed validating candidates table") from exc

        columns = {str(row["name"]) for row in rows}
        missing = sorted(REQUIRED_CANDIDATE_COLUMNS - columns)
        if missing:
            raise StateError(f"candidates table missing required columns: {', '.join(missing)}")

    def count_unenriched_candidates(self) -> int:
        try:
            row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM candidates c
                LEFT JOIN enriched_items e ON e.item_id = c.item_id
                WHERE e.item_id IS NULL
                """
            ).fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise StateError("failed counting unenriched candidates") from exc

    def select_unenriched_candidates(self, *, max_items: int | None = None) -> list[dict[str, Any]]:
        try:
            query = """
                SELECT
                  c.item_id,
                  c.source_type,
                  c.source_id,
                  c.url,
                  c.title,
                  c.published_at,
                  c.relevance_score
                FROM candidates c
                LEFT JOIN enriched_items e ON e.item_id = c.item_id
                WHERE e.item_id IS NULL
                ORDER BY c.relevance_score DESC, c.published_at DESC, c.item_id ASC
            """
            params: tuple[Any, ...] = ()
            if max_items is not None:
                query += "\nLIMIT ?"
                params = (max_items,)

            rows = self._conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            raise StateError("failed selecting candidates for enrichment") from exc

    def insert_enriched_item(self, item: EnrichedItem, *, inserted_at: str) -> bool:
        try:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO enriched_items (
                    item_id,
                    source_type,
                    url,
                    title,
                    published_at,
                    enriched_text,
                    evidence_snippets,
                    enrichment_method,
                    enriched_at,
                    inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.item_id,
                    item.source_type,
                    item.url,
                    item.title,
                    item.published_at,
                    item.enriched_text,
                    json.dumps([snippet.to_dict() for snippet in item.evidence_snippets], ensure_ascii=True),
                    item.enrichment_method,
                    item.enriched_at,
                    inserted_at,
                ),
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise StateError("failed writing enriched item row") from exc

    def get_retry_state(self, *, item_id: str) -> dict[str, Any] | None:
        try:
            row = self._conn.execute(
                """
                SELECT
                  item_id,
                  source_type,
                  source_id,
                  attempts_total,
                  consecutive_failures,
                  last_outcome,
                  last_fail_reason,
                  last_attempt_at,
                  next_eligible_at,
                  updated_at
                FROM enrich_retry_state
                WHERE item_id = ?
                """,
                (item_id,),
            ).fetchone()
            return dict(row) if row is not None else None
        except sqlite3.Error as exc:
            raise StateError("failed reading enrich_retry_state row") from exc

    def get_retry_states(self, *, item_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not item_ids:
            return {}

        results: dict[str, dict[str, Any]] = {}
        try:
            chunk_size = 900
            for start in range(0, len(item_ids), chunk_size):
                chunk = item_ids[start : start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                query = f"""
                    SELECT
                      item_id,
                      source_type,
                      source_id,
                      attempts_total,
                      consecutive_failures,
                      last_outcome,
                      last_fail_reason,
                      last_attempt_at,
                      next_eligible_at,
                      updated_at
                    FROM enrich_retry_state
                    WHERE item_id IN ({placeholders})
                """
                rows = self._conn.execute(query, tuple(chunk)).fetchall()
                for row in rows:
                    payload = dict(row)
                    results[str(payload["item_id"])] = payload
            return results
        except sqlite3.Error as exc:
            raise StateError("failed reading enrich_retry_state rows") from exc

    def upsert_retry_state(
        self,
        *,
        item_id: str,
        source_type: str,
        source_id: str,
        attempts_total: int,
        consecutive_failures: int,
        last_outcome: str,
        last_fail_reason: str | None,
        last_attempt_at: str,
        next_eligible_at: str | None,
        updated_at: str,
    ) -> None:
        try:
            self._conn.execute(
                """
                INSERT INTO enrich_retry_state (
                    item_id,
                    source_type,
                    source_id,
                    attempts_total,
                    consecutive_failures,
                    last_outcome,
                    last_fail_reason,
                    last_attempt_at,
                    next_eligible_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    source_type = excluded.source_type,
                    source_id = excluded.source_id,
                    attempts_total = excluded.attempts_total,
                    consecutive_failures = excluded.consecutive_failures,
                    last_outcome = excluded.last_outcome,
                    last_fail_reason = excluded.last_fail_reason,
                    last_attempt_at = excluded.last_attempt_at,
                    next_eligible_at = excluded.next_eligible_at,
                    updated_at = excluded.updated_at
                """,
                (
                    item_id,
                    source_type,
                    source_id,
                    attempts_total,
                    consecutive_failures,
                    last_outcome,
                    last_fail_reason,
                    last_attempt_at,
                    next_eligible_at,
                    updated_at,
                ),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateError("failed upserting enrich_retry_state row") from exc
