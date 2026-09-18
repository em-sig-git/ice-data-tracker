"""Contract expiry (last trading date) rules for the tracked ICE instruments.

This module is the single source of truth for "when does a contract stop
trading". It is used for two separate purposes:

  * ``continuous.py`` uses it to decide which contract is the front month on
    any given date;
  * ``main.py`` uses it to decide which contracts are still worth requesting
    from ICE (see :func:`is_contract_active`).

Official rules, quoted from the ICE contract specifications:

Brent Crude Futures
    "Trading shall cease at the end of the designated settlement period on the
    last Business Day of the second month preceding the relevant contract
    month." If that day is either the business day before Christmas Day or the
    business day before New Year's Day, trading ceases on the next preceding
    business day.
    https://www.ice.com/products/219/Brent-Crude-Futures

Low Sulphur Gasoil Futures
    "Trading shall cease at 12:00 hours London Time, 2 business days prior to
    the 14th calendar day of the delivery month."
    https://www.ice.com/products/34361119/Low-Sulphur-Gasoil-Futures

Both rules are verified against ICE's own published expiry calendars in
``tests/test_expiry.py`` (149 Brent contracts, 75 Gasoil contracts).

Business days follow the England & Wales bank holiday calendar, because ICE
Futures Europe is a London market. Note that ``holidays.country_holidays("GB")``
without a subdivision is NOT correct here: it returns only the holidays common
to all four UK nations and therefore omits Easter Monday and the Late Summer
Bank Holiday, both of which are London market holidays.
"""

from __future__ import annotations

import calendar
from datetime import date

import holidays
import pandas as pd

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# ICE Futures Europe is a London market.
HOLIDAY_COUNTRY = "GB"
HOLIDAY_SUBDIV = "England"

_HOLIDAYS_BY_YEAR: dict[int, set[pd.Timestamp]] = {}


def parse_market_strip(market_strip: str) -> tuple[int, int]:
    """'Feb27' -> (2027, 2)."""
    value = str(market_strip).strip()
    month_txt = value[:3].title()
    year_txt = value[3:]
    if month_txt not in MONTH_MAP:
        raise ValueError(f"Unsupported market_strip month: {market_strip}")
    return 2000 + int(year_txt), MONTH_MAP[month_txt]


def _holidays_for_year(year: int) -> set[pd.Timestamp]:
    cached = _HOLIDAYS_BY_YEAR.get(year)
    if cached is not None:
        return cached
    days = holidays.country_holidays(
        HOLIDAY_COUNTRY, subdiv=HOLIDAY_SUBDIV, years=[year]
    ).keys()
    holiday_set = {pd.Timestamp(day).normalize() for day in days}
    _HOLIDAYS_BY_YEAR[year] = holiday_set
    return holiday_set


def is_business_day(ts: pd.Timestamp) -> bool:
    normalized = ts.normalize()
    return normalized.weekday() < 5 and normalized not in _holidays_for_year(normalized.year)


def previous_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    out = ts - pd.Timedelta(days=1)
    while not is_business_day(out):
        out -= pd.Timedelta(days=1)
    return out


def next_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    out = ts + pd.Timedelta(days=1)
    while not is_business_day(out):
        out += pd.Timedelta(days=1)
    return out


def last_business_day_of_month(year: int, month: int) -> pd.Timestamp:
    ts = pd.Timestamp(year=year, month=month, day=calendar.monthrange(year, month)[1])
    while not is_business_day(ts):
        ts -= pd.Timedelta(days=1)
    return ts


def business_days_before(ts: pd.Timestamp, n: int) -> pd.Timestamp:
    out = ts
    for _ in range(n):
        out = previous_business_day(out)
    return out


def business_days_after(ts: pd.Timestamp, n: int) -> pd.Timestamp:
    out = ts
    for _ in range(n):
        out = next_business_day(out)
    return out


def _gasoil_last_trading_date(contract_year: int, contract_month: int) -> pd.Timestamp:
    """12:00 London, 2 business days prior to the 14th calendar day of the
    delivery month."""
    anchor = pd.Timestamp(year=contract_year, month=contract_month, day=14)
    return business_days_before(anchor, 2)


def _brent_last_trading_date(contract_year: int, contract_month: int) -> pd.Timestamp:
    """Last business day of the second month preceding the contract month,
    stepped back one business day if it is the business day before Christmas
    Day or before New Year's Day."""
    preceding_year = contract_year
    preceding_month = contract_month - 2
    if preceding_month <= 0:
        preceding_month += 12
        preceding_year -= 1

    ltd = last_business_day_of_month(preceding_year, preceding_month)

    # Only a December last-business-day can collide with either holiday, and
    # New Year's Day falls in the FOLLOWING calendar year.
    if preceding_month == 12:
        before_christmas = previous_business_day(pd.Timestamp(preceding_year, 12, 25))
        before_new_year = previous_business_day(pd.Timestamp(preceding_year + 1, 1, 1))
        if ltd in (before_christmas, before_new_year):
            ltd = previous_business_day(ltd)

    return ltd


_ROLL_RULES = {
    "gasoil": _gasoil_last_trading_date,
    "brent": _brent_last_trading_date,
}


def compute_last_trading_date(market_strip: str, roll_rule: str) -> pd.Timestamp:
    rule = _ROLL_RULES.get(roll_rule)
    if rule is None:
        raise ValueError(f"Unsupported roll_rule: {roll_rule}")
    year, month = parse_market_strip(market_strip)
    return rule(year, month)


def scrape_until_date(market_strip: str, roll_rule: str, grace_business_days: int) -> pd.Timestamp:
    """Last date on which this contract is still worth requesting from ICE.

    The grace period exists because ICE can publish corrected final settlement
    prices for a day or two after a contract stops trading.
    """
    ltd = compute_last_trading_date(market_strip, roll_rule)
    return business_days_after(ltd, grace_business_days)


def is_contract_active(
    market_strip: str,
    roll_rule: str,
    today: date,
    grace_business_days: int,
) -> bool:
    """True while the contract should still be scraped.

    Returning False does NOT mean the contract's data should be deleted. The
    metadata row and every settlement price already collected are kept; the
    scraper simply stops asking ICE about it.
    """
    cutoff = scrape_until_date(market_strip, roll_rule, grace_business_days)
    return pd.Timestamp(today).normalize() <= cutoff
