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
USER_AGENT = "Mozilla/5.0 (compatible; ice-data-tracker/1.0; +https://github.com/em-sig-git/ice-data-tracker)"
REQUEST_TIMEOUT = 60
TRACKING_HORIZON_MONTHS = 12
HISTORICAL_SPAN = 3


@dataclass(frozen=True)
class Instrument:
    slug: str
    name: str
    product_id: int
    hub_id: int
    product_url: str

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
    ),
    Instrument(
        slug="low_sulphur_gasoil",
        name="Low Sulphur Gasoil Futures",
        product_id=5817,
        hub_id=9373,
        product_url="https://www.ice.com/products/34361119/Low-Sulphur-Gasoil-Futures/data",
    ),
)
