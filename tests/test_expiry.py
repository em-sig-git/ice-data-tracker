"""Verify the last-trading-date rules against ICE's own published calendars.

Run with:  PYTHONPATH=src python -m pytest tests/ -q
       or: PYTHONPATH=src python tests/test_expiry.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ice_data_tracker.expiry import (  # noqa: E402
    compute_last_trading_date,
    is_contract_active,
    scrape_until_date,
)
from ice_expiry_reference import BRENT_LTD, GASOIL_LTD  # noqa: E402


def test_brent_matches_ice_published_expiries() -> None:
    wrong = {
        strip: (expected, compute_last_trading_date(strip, "brent").strftime("%Y-%m-%d"))
        for strip, expected in BRENT_LTD.items()
        if compute_last_trading_date(strip, "brent").strftime("%Y-%m-%d") != expected
    }
    assert not wrong, f"Brent LTD mismatches (ICE, computed): {wrong}"


def test_gasoil_matches_ice_published_expiries() -> None:
    wrong = {
        strip: (expected, compute_last_trading_date(strip, "gasoil").strftime("%Y-%m-%d"))
        for strip, expected in GASOIL_LTD.items()
        if compute_last_trading_date(strip, "gasoil").strftime("%Y-%m-%d") != expected
    }
    assert not wrong, f"Gasoil LTD mismatches (ICE, computed): {wrong}"


def test_brent_new_years_eve_rule() -> None:
    """Feb27 stops trading 30 Dec 2026, not 31 Dec.

    31 Dec 2026 is the last business day of December AND the business day
    before New Year's Day, so the rule steps back one business day.
    """
    assert compute_last_trading_date("Feb27", "brent").strftime("%Y-%m-%d") == "2026-12-30"


def test_late_summer_bank_holiday_is_a_market_holiday() -> None:
    """Oct37 Brent stops 28 Aug 2037: 31 Aug 2037 is the Late Summer Bank
    Holiday, which holidays.country_holidays("GB") omits without a subdivision."""
    assert compute_last_trading_date("Oct37", "brent").strftime("%Y-%m-%d") == "2037-08-28"


def test_grace_period_keeps_contract_active_after_expiry() -> None:
    ltd = compute_last_trading_date("Sep26", "gasoil").date()  # 2026-09-10
    assert ltd == date(2026, 9, 10)
    cutoff = scrape_until_date("Sep26", "gasoil", 5).date()
    assert cutoff == date(2026, 9, 17)  # 5 business days later

    assert is_contract_active("Sep26", "gasoil", ltd, 5)
    assert is_contract_active("Sep26", "gasoil", cutoff, 5)
    assert not is_contract_active("Sep26", "gasoil", date(2026, 9, 18), 5)


def test_expired_contract_is_skipped_but_far_future_is_not() -> None:
    today = date(2026, 9, 18)
    assert not is_contract_active("Apr26", "gasoil", today, 5)
    assert not is_contract_active("Jun26", "brent", today, 5)
    assert is_contract_active("Nov26", "brent", today, 5)
    assert is_contract_active("Sep27", "gasoil", today, 5)


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    print(f"\n{failures} failure(s)")
    sys.exit(1 if failures else 0)
