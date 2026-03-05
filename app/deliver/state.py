from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ColumnContract:
    declared_type: str
    pk_position: int
    not_null: bool


IDEAS_COLUMN_CONTRACT: dict[str, ColumnContract] = {
    "item_id": ColumnContract("TEXT", 1, False),
    "url": ColumnContract("TEXT", 0, True),
    "topic": ColumnContract("TEXT", 0, True),
    "viral_rating": ColumnContract("INTEGER", 0, True),
    "published_at": ColumnContract("TEXT", 0, True),
}

SCRIPTS_COLUMN_CONTRACT: dict[str, ColumnContract] = {
    "item_id": ColumnContract("TEXT", 1, False),
    "primary_hook": ColumnContract("TEXT", 0, True),
    "script_sections": ColumnContract("TEXT", 0, True),
}

ITEMS_COLUMN_CONTRACT: dict[str, ColumnContract] = {
    "item_id": ColumnContract("TEXT", 1, False),
    "creator": ColumnContract("TEXT", 0, True),
    "source_name": ColumnContract("TEXT", 0, True),
}

DELIVERIES_COLUMN_CONTRACT: dict[str, ColumnContract] = {
    "item_id": ColumnContract("TEXT", 1, True),
    "channel": ColumnContract("TEXT", 2, True),
    "webhook_hash": ColumnContract("TEXT", 3, True),
    "sent_at": ColumnContract("TEXT", 0, True),
}


class StateError(RuntimeError):
    pass


@dataclass
class DeliverStore:
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

    def validate_dependencies(self) -> None:
        self._validate_table_compatibility(
            table_name="ideas",
            required_columns=IDEAS_COLUMN_CONTRACT,
        )
        self._validate_table_compatibility(
            table_name="scripts",
            required_columns=SCRIPTS_COLUMN_CONTRACT,
        )
        self._validate_table_compatibility(
            table_name="items",
            required_columns=ITEMS_COLUMN_CONTRACT,
        )

    def ensure_deliveries_table(self) -> None:
        try:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    item_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    webhook_hash TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    PRIMARY KEY (item_id, channel, webhook_hash)
                )
                """
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateError("failed creating deliveries table") from exc

    def validate_deliveries_compatibility(self) -> None:
        self._validate_table_compatibility(
            table_name="deliveries",
            required_columns=DELIVERIES_COLUMN_CONTRACT,
        )

    def count_items_available_total(self, *, min_viral_rating: int | None) -> int:
        try:
            row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM ideas
                WHERE (:min_viral_rating IS NULL OR viral_rating >= :min_viral_rating)
                """,
                {"min_viral_rating": min_viral_rating},
            ).fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise StateError("failed counting stage_8 available rows") from exc

    def select_candidates(self, *, min_viral_rating: int | None) -> list[dict[str, Any]]:
        try:
            rows = self._conn.execute(
                """
                SELECT
                  i.item_id,
                  i.url,
                  i.topic,
                  i.viral_rating,
                  i.published_at,
                  s.item_id AS script_item_id,
                  s.primary_hook,
                  s.script_sections,
                  it.creator,
                  it.source_name
                FROM ideas i
                LEFT JOIN scripts s ON s.item_id = i.item_id
                LEFT JOIN items it ON it.item_id = i.item_id
                WHERE (:min_viral_rating IS NULL OR i.viral_rating >= :min_viral_rating)
                ORDER BY i.viral_rating DESC, i.published_at DESC, i.item_id ASC
                """,
                {"min_viral_rating": min_viral_rating},
            ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            raise StateError("failed selecting stage_8 candidate rows") from exc

    def was_already_sent(self, *, item_id: str, channel: str, webhook_hash: str) -> bool:
        try:
            row = self._conn.execute(
                """
                SELECT 1
                FROM deliveries
                WHERE item_id = ?
                  AND channel = ?
                  AND webhook_hash = ?
                LIMIT 1
                """,
                (item_id, channel, webhook_hash),
            ).fetchone()
            return row is not None
        except sqlite3.Error as exc:
            raise StateError("failed checking delivery idempotency state") from exc

    def insert_delivery(
        self,
        *,
        item_id: str,
        channel: str,
        webhook_hash: str,
        sent_at: str,
    ) -> None:
        try:
            self._conn.execute(
                """
                INSERT INTO deliveries (
                    item_id,
                    channel,
                    webhook_hash,
                    sent_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (item_id, channel, webhook_hash, sent_at),
            )
            self._conn.commit()
        except sqlite3.IntegrityError as exc:
            raise StateError(
                f"unexpected duplicate delivery row for item_id={item_id}, channel={channel}"
            ) from exc
        except sqlite3.Error as exc:
            raise StateError("failed writing delivery row") from exc

    def _validate_table_compatibility(
        self,
        *,
        table_name: str,
        required_columns: dict[str, ColumnContract],
    ) -> None:
        try:
            exists = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table_name,),
            ).fetchone()
            if exists is None:
                raise StateError(f"{table_name} table is missing")
            rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except sqlite3.Error as exc:
            raise StateError(f"failed validating {table_name} table compatibility") from exc

        if not rows:
            raise StateError(f"{table_name} table compatibility check failed")

        by_name = {str(row["name"]): row for row in rows}
        for column_name, contract in required_columns.items():
            row = by_name.get(column_name)
            if row is None:
                raise StateError(f"{table_name} table missing required column: {column_name}")

            declared_type = str(row["type"]).upper()
            if declared_type != contract.declared_type:
                raise StateError(
                    f"{table_name} table incompatible type for {column_name}: "
                    f"expected {contract.declared_type}, got {declared_type}"
                )

            pk_position = int(row["pk"])
            if pk_position != contract.pk_position:
                raise StateError(
                    f"{table_name} table incompatible PK for {column_name}: "
                    f"expected {contract.pk_position}, got {pk_position}"
                )

            not_null = int(row["notnull"]) == 1
            if not_null != contract.not_null:
                expected = "NOT NULL" if contract.not_null else "nullable"
                actual = "NOT NULL" if not_null else "nullable"
                raise StateError(
                    f"{table_name} table incompatible nullability for {column_name}: "
                    f"expected {expected}, got {actual}"
                )

