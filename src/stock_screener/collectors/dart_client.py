from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


DART_SINGLE_COMPANY_ALL_ACCOUNTS_ENDPOINT = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"


@dataclass(frozen=True)
class DartClient:
    api_key: str
    endpoint: str | None = DART_SINGLE_COMPANY_ALL_ACCOUNTS_ENDPOINT
    timeout_s: float = 10.0
    default_params: dict[str, str] = field(default_factory=dict)

    def fetch_financials(self, dt: date) -> list[dict[str, Any]]:
        """Fetch raw DART financial payload for an anchor date.

        This client is intentionally lightweight and returns an empty payload
        when the endpoint response does not include a list payload.
        """

        params = {
            "crtfc_key": self.api_key,
            "base_date": dt.strftime("%Y%m%d"),
            **self.default_params,
        }
        query = urlencode(params)
        if not self.endpoint:
            return []
        with urlopen(f"{self.endpoint}?{query}", timeout=self.timeout_s) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))

        if isinstance(payload, dict):
            rows = payload.get("list")
            if isinstance(rows, list):
                return rows
        if isinstance(payload, list):
            return payload
        return []
