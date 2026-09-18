"""HTTP client for the ICE market data API.

www.ice.com sits behind Cloudflare. A plain ``requests`` call is rejected with
HTTP 403 and a "Attention Required! | Cloudflare" HTML body, because Cloudflare
scores the *connection*, not just the User-Agent header:

  * the TLS ClientHello fingerprint (JA3/JA4) of Python's ssl module does not
    look like any real browser;
  * the HTTP/2 SETTINGS frame and header ordering differ from a browser's;
  * no Cloudflare clearance cookies are present, because the API endpoint is
    normally called by JavaScript running on a page the visitor already loaded.

This client addresses all three:

  1. ``curl_cffi`` replaces ``requests`` and impersonates a real Chrome TLS and
     HTTP/2 fingerprint;
  2. a full set of browser headers, including the Sec-Fetch and sec-ch-ua hints
     a real XHR would send;
  3. a warm-up request to the instrument's product page before the API is
     touched, so that whatever cookies Cloudflare sets are replayed on the API
     call, with a matching Referer.

Every request is retried with exponential backoff. On a 403 the session is torn
down and rebuilt against a different browser profile before the next attempt,
because a 403 means this particular session has been scored as a bot and
repeating it unchanged will only produce another 403.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

from curl_cffi import requests as curl_requests

from .config import (
    HISTORICAL_SPAN,
    IMPERSONATE_PROFILES,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_MAX_SECONDS,
    RETRYABLE_STATUS_CODES,
    Instrument,
)

HISTORICAL_URL = "https://www.ice.com/marketdata/api/productguide/charting/data/historical"


class IceHttpError(RuntimeError):
    """An ICE request failed after all retries.

    Carries the final HTTP status code (``None`` for network-level failures) so
    callers do not need to know which HTTP library is underneath.
    """

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _browser_headers(referer: str) -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Referer": referer,
        "Origin": "https://www.ice.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
    }


class IceClient:
    """Cloudflare-tolerant client for the ICE product guide API."""

    def __init__(self) -> None:
        self._profile_index = 0
        self._session: curl_requests.Session | None = None
        self._warmed_url: str | None = None

    # ------------------------------------------------------------------ session

    @property
    def profile(self) -> str:
        return IMPERSONATE_PROFILES[self._profile_index % len(IMPERSONATE_PROFILES)]

    def _new_session(self) -> curl_requests.Session:
        session = curl_requests.Session(impersonate=self.profile)
        # curl_cffi supplies the User-Agent and sec-ch-ua hints that match the
        # impersonated profile; overriding them would break the fingerprint.
        session.headers.update(
            {
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        return session

    def _session_or_new(self) -> curl_requests.Session:
        if self._session is None:
            self._session = self._new_session()
        return self._session

    def _rotate_session(self) -> None:
        """Discard the current session and build the next one.

        Called after a 403: the cookies we hold have been scored as a bot, so
        they are worse than useless on the next attempt.
        """
        if self._session is not None:
            try:
                self._session.close()
            except Exception:  # pragma: no cover - close is best effort
                pass
        self._session = None
        self._warmed_url = None
        self._profile_index += 1
        logging.info("Rotating browser profile to %s", self.profile)

    def warm_up(self, url: str, *, force: bool = False) -> None:
        """Load a normal ICE page so Cloudflare cookies are set for the session.

        A failure here is not fatal: the API call may still succeed, and if it
        does not, the API call's own error is the more useful one to report.
        """
        if self._warmed_url == url and not force:
            return
        session = self._session_or_new()
        try:
            response = session.get(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                },
                timeout=REQUEST_TIMEOUT,
            )
            logging.info(
                "Warm-up %s | status=%s | profile=%s | cookies=%s",
                url,
                response.status_code,
                self.profile,
                len(session.cookies),
            )
            self._warmed_url = url
        except Exception as exc:
            logging.warning("Warm-up request to %s failed (continuing): %s", url, exc)

    # ------------------------------------------------------------------ requests

    def _get_json(self, url: str, *, referer: str, params: dict[str, Any] | None = None) -> Any:
        last_status: int | None = None
        last_error: str = ""

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            session = self._session_or_new()
            try:
                response = session.get(
                    url,
                    params=params,
                    headers=_browser_headers(referer),
                    timeout=REQUEST_TIMEOUT,
                )
                status = response.status_code
                last_status = status

                if status < 400:
                    return response.json()

                body_start = (response.text or "")[:300].replace("\n", " ")
                blocked_by = response.headers.get("server", "")
                last_error = f"HTTP {status}"
                logging.warning(
                    "ICE request failed | attempt=%s/%s | status=%s | profile=%s | server=%s | url=%s | body_start=%s",
                    attempt,
                    RETRY_ATTEMPTS,
                    status,
                    self.profile,
                    blocked_by,
                    response.url,
                    body_start,
                )

                if status not in RETRYABLE_STATUS_CODES:
                    raise IceHttpError(f"ICE request failed with HTTP {status}: {url}", status)

                # 403/429 mean this session is burnt; start a fresh one.
                if status in (403, 429):
                    self._rotate_session()

            except IceHttpError:
                raise
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                last_status = None
                logging.warning(
                    "ICE request error | attempt=%s/%s | profile=%s | url=%s | %s",
                    attempt,
                    RETRY_ATTEMPTS,
                    self.profile,
                    url,
                    last_error,
                )
                self._rotate_session()

            if attempt < RETRY_ATTEMPTS:
                delay = min(
                    RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
                    RETRY_BACKOFF_MAX_SECONDS,
                )
                delay += random.uniform(0, delay * 0.25)  # jitter
                logging.info("Retrying in %.1fs (attempt %s/%s)", delay, attempt + 1, RETRY_ATTEMPTS)
                time.sleep(delay)
                # Re-establish Cloudflare cookies for the new session.
                self.warm_up(referer, force=True)

        raise IceHttpError(
            f"ICE request failed after {RETRY_ATTEMPTS} attempts ({last_error}): {url}",
            last_status,
        )

    # ------------------------------------------------------------------ endpoints

    def fetch_contract_metadata(self, instrument: Instrument) -> list[dict[str, Any]]:
        logging.info("Fetching contract metadata for %s", instrument.name)
        self.warm_up(instrument.product_url)
        payload = self._get_json(instrument.metadata_url, referer=instrument.product_url)
        if not isinstance(payload, list):
            raise IceHttpError(
                f"Unexpected metadata payload for {instrument.slug}: {type(payload)!r}"
            )
        return payload

    def fetch_historical(self, market_id: int, *, referer: str) -> dict[str, Any]:
        payload = self._get_json(
            HISTORICAL_URL,
            referer=referer,
            params={"marketId": market_id, "historicalSpan": HISTORICAL_SPAN},
        )
        if not isinstance(payload, dict):
            raise IceHttpError(
                f"Unexpected historical payload for market {market_id}: {type(payload)!r}"
            )
        return payload

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            finally:
                self._session = None
