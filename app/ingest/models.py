from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class RawItem:
    source_type: str
    source_id: str
    source_name: str
    creator: str
    title: str
    url: str
    published_at: str
    external_id: str
    summary: str
    fetched_at: str

    def to_dict(self) -> dict:
        return asdict(self)
