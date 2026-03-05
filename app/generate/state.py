from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import ScriptRecord


REQUIRED_IDEAS_COLUMNS: dict[str, str] = {
    "item_id": "TEXT",
    "platform": "TEXT",
    "recommended_format": "TEXT",
    "url": "TEXT",
    "title": "TEXT",
    "published_at": "TEXT",
    "topic": "TEXT",
    "core_claim": "TEXT",
    "workflow_steps": "TEXT",
    "tools_mentioned": "TEXT",
    "monetization_angle": "TEXT",
    "metrics_claims": "TEXT",
    "assumptions": "TEXT",
    "hooks": "TEXT",
    "viral_rating": "INTEGER",
}

REQUIRED_SCRIPTS_COLUMNS: dict[str, str] = {
    "item_id": "TEXT",
    "platform": "TEXT",
    "recommended_format": "TEXT",
    "primary_hook": "TEXT",
    "alt_hooks": "TEXT",
    "script_sections": "TEXT",
    "word_count": "INTEGER",
    "estimated_seconds": "INTEGER",
    "cta": "TEXT",
    "disclaimer": "TEXT",
    "llm_provider": "TEXT",
    "llm_model": "TEXT",
    "created_at": "TEXT",
}


class StateError(RuntimeError):
    pass


@dataclass
class GenerateStore:
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

    def validate_ideas_table(self) -> None:
        self._validate_table_compatibility(
            table_name="ideas",
            required_columns=REQUIRED_IDEAS_COLUMNS,
            validate_exists=True,
        )

    def ensure_scripts_table(self) -> None:
        try:
            self._conn.execute(
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
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateError("failed creating scripts table") from exc

    def validate_scripts_compatibility(self) -> None:
        self._validate_table_compatibility(
            table_name="scripts",
            required_columns=REQUIRED_SCRIPTS_COLUMNS,
            validate_exists=True,
        )

    def count_items_available_total(self) -> int:
        try:
            row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM ideas i
                LEFT JOIN scripts s ON s.item_id = i.item_id
                WHERE s.item_id IS NULL
                """
            ).fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise StateError("failed counting stage_6 available rows") from exc

    def select_rows(self, *, max_items: int) -> list[dict[str, Any]]:
        try:
            rows = self._conn.execute(
                """
                SELECT
                  i.item_id,
                  i.platform,
                  i.recommended_format,
                  i.url,
                  i.title,
                  i.published_at,
                  i.topic,
                  i.core_claim,
                  i.workflow_steps,
                  i.tools_mentioned,
                  i.monetization_angle,
                  i.metrics_claims,
                  i.assumptions,
                  i.hooks,
                  i.viral_rating
                FROM ideas i
                LEFT JOIN scripts s ON s.item_id = i.item_id
                WHERE s.item_id IS NULL
                ORDER BY i.viral_rating DESC, i.published_at DESC, i.item_id ASC
                LIMIT ?
                """,
                (max_items,),
            ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            raise StateError("failed selecting stage_6 rows") from exc

    def insert_script(self, script: ScriptRecord) -> bool:
        try:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO scripts (
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
                    script.item_id,
                    script.platform,
                    script.recommended_format,
                    script.primary_hook,
                    json.dumps(script.alt_hooks, ensure_ascii=True),
                    json.dumps(script.script_sections, ensure_ascii=True),
                    script.word_count,
                    script.estimated_seconds,
                    script.cta,
                    script.disclaimer,
                    script.llm_provider,
                    script.llm_model,
                    script.created_at,
                ),
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise StateError("failed writing stage_6 script row") from exc

    def _validate_table_compatibility(
        self,
        *,
        table_name: str,
        required_columns: dict[str, str],
        validate_exists: bool,
    ) -> None:
        try:
            if validate_exists:
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
        for name, expected_type in required_columns.items():
            row = by_name.get(name)
            if row is None:
                raise StateError(f"{table_name} table missing required column: {name}")
            declared_type = str(row["type"]).upper()
            if declared_type != expected_type:
                raise StateError(
                    f"{table_name} table incompatible type for {name}: expected {expected_type}, got {declared_type}"
                )

            is_pk = int(row["pk"]) == 1
            is_not_null = int(row["notnull"]) == 1
            if name == "item_id":
                if not is_pk:
                    raise StateError(f"{table_name}.item_id must be primary key")
            else:
                if not is_not_null:
                    raise StateError(f"{table_name}.{name} must be NOT NULL")

