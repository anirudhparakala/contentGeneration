from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import CandidateItem


class StateError(RuntimeError):
    pass


@dataclass
class CandidatesStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._create_table()
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize sqlite db at {self.db_path}") from exc

    def close(self) -> None:
        self._conn.close()

    def count_unprocessed_items(self) -> int:
        try:
            cursor = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM items i
                LEFT JOIN candidates c ON c.item_id = i.item_id
                WHERE c.item_id IS NULL
                """
            )
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise StateError("failed counting unprocessed items") from exc

    def iter_unprocessed_items(self) -> list[dict[str, Any]]:
        try:
            cursor = self._conn.execute(
                """
                SELECT
                  i.item_id,
                  i.source_type,
                  i.source_id,
                  i.source_name,
                  i.creator,
                  i.title,
                  i.url,
                  i.published_at,
                  i.fetched_at,
                  i.summary,
                  i.content_text
                FROM items i
                LEFT JOIN candidates c ON c.item_id = i.item_id
                WHERE c.item_id IS NULL
                ORDER BY i.published_at DESC, i.item_id ASC
                """
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as exc:
            raise StateError("failed selecting unprocessed items") from exc

    def insert_candidate(self, candidate: CandidateItem, *, created_at: str) -> bool:
        try:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO candidates (
                    item_id,
                    relevance_score,
                    matched_keywords,
                    source_type,
                    source_id,
                    source_name,
                    creator,
                    title,
                    url,
                    published_at,
                    fetched_at,
                    content_text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.item_id,
                    candidate.relevance_score,
                    json.dumps(candidate.matched_keywords, ensure_ascii=True),
                    candidate.source_type,
                    candidate.source_id,
                    candidate.source_name,
                    candidate.creator,
                    candidate.title,
                    candidate.url,
                    candidate.published_at,
                    candidate.fetched_at,
                    candidate.content_text,
                    created_at,
                ),
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise StateError("failed writing candidates row") from exc

    def _create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candidates (
                item_id TEXT PRIMARY KEY,
                relevance_score INTEGER NOT NULL,
                matched_keywords TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                creator TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                content_text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
