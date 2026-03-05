from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import IdeaRecord


REQUIRED_ENRICHED_COLUMNS: frozenset[str] = frozenset(
    {
        "item_id",
        "source_type",
        "url",
        "title",
        "published_at",
        "enriched_text",
        "enrichment_method",
        "evidence_snippets",
    }
)

REQUIRED_IDEAS_COLUMNS: dict[str, str] = {
    "item_id": "TEXT",
    "source_type": "TEXT",
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
    "content_type": "TEXT",
    "viral_rating": "INTEGER",
    "rating_rationale": "TEXT",
    "hooks": "TEXT",
    "platform": "TEXT",
    "recommended_format": "TEXT",
    "llm_provider": "TEXT",
    "llm_model": "TEXT",
    "created_at": "TEXT",
}


class StateError(RuntimeError):
    pass


@dataclass
class IntelligenceStore:
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

    def validate_enriched_items_table(self) -> None:
        try:
            exists = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='enriched_items' LIMIT 1"
            ).fetchone()
            if exists is None:
                raise StateError("enriched_items table is missing")
            rows = self._conn.execute("PRAGMA table_info(enriched_items)").fetchall()
        except sqlite3.Error as exc:
            raise StateError("failed validating enriched_items table") from exc

        columns = {str(row["name"]) for row in rows}
        missing = sorted(REQUIRED_ENRICHED_COLUMNS - columns)
        if missing:
            raise StateError(f"enriched_items missing required columns: {', '.join(missing)}")

    def ensure_ideas_table(self) -> None:
        try:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ideas (
                    item_id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
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
                    content_type TEXT NOT NULL,
                    viral_rating INTEGER NOT NULL,
                    rating_rationale TEXT NOT NULL,
                    hooks TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    recommended_format TEXT NOT NULL,
                    llm_provider TEXT NOT NULL,
                    llm_model TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateError("failed creating ideas table") from exc

    def validate_ideas_compatibility(self) -> None:
        try:
            rows = self._conn.execute("PRAGMA table_info(ideas)").fetchall()
        except sqlite3.Error as exc:
            raise StateError("failed validating ideas table compatibility") from exc

        if not rows:
            raise StateError("ideas table compatibility check failed")

        by_name = {str(row["name"]): row for row in rows}
        for name, expected_type in REQUIRED_IDEAS_COLUMNS.items():
            row = by_name.get(name)
            if row is None:
                raise StateError(f"ideas table missing required column: {name}")
            declared_type = str(row["type"]).upper()
            if declared_type != expected_type:
                raise StateError(
                    f"ideas table incompatible type for {name}: expected {expected_type}, got {declared_type}"
                )

            is_pk = int(row["pk"]) == 1
            is_not_null = int(row["notnull"]) == 1
            if name == "item_id":
                if not is_pk:
                    raise StateError("ideas.item_id must be primary key")
            else:
                if not is_not_null:
                    raise StateError(f"ideas.{name} must be NOT NULL")

    def count_items_available_total(self) -> int:
        try:
            row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM enriched_items e
                LEFT JOIN ideas i ON i.item_id = e.item_id
                WHERE i.item_id IS NULL
                """
            ).fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise StateError("failed counting stage_5 available rows") from exc

    def select_rows(self, *, max_items: int) -> list[dict[str, Any]]:
        try:
            rows = self._conn.execute(
                """
                SELECT
                  e.item_id,
                  e.source_type,
                  e.url,
                  e.title,
                  e.published_at,
                  e.enriched_text,
                  e.enrichment_method,
                  e.evidence_snippets
                FROM enriched_items e
                LEFT JOIN ideas i ON i.item_id = e.item_id
                WHERE i.item_id IS NULL
                ORDER BY e.published_at DESC, e.item_id ASC
                LIMIT ?
                """,
                (max_items,),
            ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            raise StateError("failed selecting stage_5 rows") from exc

    def insert_idea(self, idea: IdeaRecord) -> bool:
        try:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO ideas (
                    item_id,
                    source_type,
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
                    content_type,
                    viral_rating,
                    rating_rationale,
                    hooks,
                    platform,
                    recommended_format,
                    llm_provider,
                    llm_model,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    idea.item_id,
                    idea.source_type,
                    idea.url,
                    idea.title,
                    idea.published_at,
                    idea.topic,
                    idea.core_claim,
                    json.dumps(idea.workflow_steps, ensure_ascii=True),
                    json.dumps(idea.tools_mentioned, ensure_ascii=True),
                    idea.monetization_angle,
                    json.dumps(idea.metrics_claims, ensure_ascii=True),
                    json.dumps(idea.assumptions, ensure_ascii=True),
                    idea.content_type,
                    idea.viral_rating,
                    idea.rating_rationale,
                    json.dumps(idea.hooks, ensure_ascii=True),
                    idea.platform,
                    idea.recommended_format,
                    idea.llm_provider,
                    idea.llm_model,
                    idea.created_at,
                ),
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise StateError("failed writing stage_5 idea row") from exc

