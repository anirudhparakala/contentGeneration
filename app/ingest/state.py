from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path


class StateError(RuntimeError):
    pass


@dataclass
class SeenItemsStore:
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

    def dedup_key(self, source_id: str, external_id: str) -> str:
        raw = f"{source_id}|{external_id}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def register_if_new(
        self,
        *,
        source_id: str,
        source_type: str,
        external_id: str,
        url: str,
        published_at: str,
        first_seen_at: str,
    ) -> bool:
        dedup_key = self.dedup_key(source_id, external_id)
        try:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO seen_items (
                    dedup_key, external_id, url, source_type, source_id, published_at, first_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (dedup_key, external_id, url, source_type, source_id, published_at, first_seen_at),
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise StateError("failed writing seen_items row") from exc

    def _create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_items (
                dedup_key TEXT PRIMARY KEY,
                external_id TEXT NOT NULL,
                url TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                published_at TEXT NOT NULL,
                first_seen_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
