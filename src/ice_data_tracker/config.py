from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

RIGA_TZ = ZoneInfo("Europe/Riga")
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
METADATA_DIR = DATA_DIR / "metadata"
HISTORICAL_DIR = DATA_DIR / "historical"
SOURCE_DIR = DATA_DIR / "source"
DERIVED_DIR = DATA_DIR / "derived"
STATE_DIR = DATA_DIR / "state"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "scrape_history.log"
SCHEDULE_STATE_FILE = STATE_DIR / "schedule_state.json"
CSV_SEPARATOR = ";"
CSV_DECIMAL = "."
CSV_ENCODING = "utf-8-sig"

REQUEST_TIMEOUT = 60
REQUEST_PAUSE_SECONDS = 0.75
TRACKING_HORIZON_MONTHS = 12
HISTORICAL_SPAN = 3

# --- Cloudflare / anti-bot -------------------------------------------------
# Browser profiles curl_cffi impersonates. The client rotates to the next one
# after a 403, because repeating a session Cloudflare has already scored as a
# bot just produces another 403. "chrome" tracks the newest Chrome that the
# installed curl_cffi knows about.
IMPERSONATE_PROFILES: tuple[str, ...] = ("chrome", "chrome136", "safari184", "firefox144")

# --- Retries ---------------------------------------------------------------
RETRY_ATTEMPTS = 4                  # 1 initial attempt + 3 retries
RETRY_BACKOFF_BASE_SECONDS = 4.0    # 4s, 8s, 16s (plus up to 25% jitter)
RETRY_BACKOFF_MAX_SECONDS = 60.0
RETRYABLE_STATUS_CODES = frozenset({403, 408, 429, 500, 502, 503, 504})

# --- Contract expiry -------------------------------------------------------
# A contract stops being requested this many business days after its last
# trading date. The grace period covers corrected final settlement prices,
# which ICE can publish a day or two after trading ceases. Data already
# collected for an expired contract is always kept.
EXPIRY_GRACE_BUSINESS_DAYS = 5


@dataclass(frozen=True)
class Instrument:
    slug: str
    name: str
    product_id: int
    hub_id: int
    product_url: str
    roll_rule: str  # see expiry.py: "brent" or "gasoil"

    @property
    def metadata_url(self) -> str:
        return (
            "https://www.ice.com/marketdata/api/productguide/charting/contract-data"
            f"?productId={self.product_id}&hubId={self.hub_id}"
        )


INSTRUMENTS: tuple[Instrument, ...] = (
    Instrument(
        slug="brent_crude",
        name="Brent Crude Futures",
        product_id=254,
        hub_id=403,
        product_url="https://www.ice.com/products/219/Brent-Crude-Futures/data",
        roll_rule="brent",
    ),
    Instrument(
        slug="low_sulphur_gasoil",
        name="Low Sulphur Gasoil Futures",
        product_id=5817,
        hub_id=9373,
        product_url="https://www.ice.com/products/34361119/Low-Sulphur-Gasoil-Futures/data",
        roll_rule="gasoil",
    ),
)