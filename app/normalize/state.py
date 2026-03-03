from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .models import CanonicalItem


class StateError(RuntimeError):
    pass


@dataclass
class ItemsStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._create_table()
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize sqlite db at {self.db_path}") from exc

    def close(self) -> None:
        self._conn.close()

    def insert_if_new(self, item: CanonicalItem, *, inserted_at: str) -> bool:
        try:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO items (
                    item_id,
                    external_id,
                    source_type,
                    source_id,
                    source_name,
                    creator,
                    title,
                    url,
                    published_at,
                    fetched_at,
                    summary,
                    content_text,
                    raw_item_json,
                    inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.item_id,
                    item.external_id,
                    item.source_type,
                    item.source_id,
                    item.source_name,
                    item.creator,
                    item.title,
                    item.url,
                    item.published_at,
                    item.fetched_at,
                    item.summary,
                    item.content_text,
                    json.dumps(item.raw_item_json, ensure_ascii=True),
                    inserted_at,
                ),
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise StateError("failed writing items row") from exc

    def _create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                item_id TEXT PRIMARY KEY,
                external_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                creator TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                summary TEXT NOT NULL,
                content_text TEXT NOT NULL,
                raw_item_json TEXT NOT NULL,
                inserted_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
