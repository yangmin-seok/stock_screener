from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DartClient:
    api_key: str
