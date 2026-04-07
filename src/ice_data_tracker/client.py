from __future__ import annotations

import logging
from typing import Any

import requests

from .config import HISTORICAL_SPAN, REQUEST_TIMEOUT, USER_AGENT, Instrument


class IceClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def fetch_contract_metadata(self, instrument: Instrument) -> list[dict[str, Any]]:
        logging.info("Fetching contract metadata for %s", instrument.name)
        payload = self._get_json(instrument.metadata_url)
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected metadata payload for {instrument.slug}: {type(payload)!r}")
        return payload

    def fetch_historical(self, market_id: int) -> dict[str, Any]:
        url = "https://www.ice.com/marketdata/api/productguide/charting/data/historical"
        payload = self._get_json(url, params={"marketId": market_id, "historicalSpan": HISTORICAL_SPAN})
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected historical payload for market {market_id}: {type(payload)!r}")
        return payload
