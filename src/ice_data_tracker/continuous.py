from __future__ import annotations

import calendar
from dataclasses import dataclass
from pathlib import Path

import holidays
import pandas as pd

from .config import DERIVED_DIR, HISTORICAL_DIR, METADATA_DIR, SOURCE_DIR
from .storage import read_csv_if_exists, write_csv

MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
}


@dataclass(frozen=True)
class ContinuousInstrument:
    slug: str
    display_name: str
    source_filename: str
    metadata_filename: str
    historical_filename: str
    roll_rule: str
    splice_last_seed_date: str
    derived_front_month_filename: str
    derived_continuous_filename: str


CONTINUOUS_INSTRUMENTS: tuple[ContinuousInstrument, ...] = (
    ContinuousInstrument(
        slug='brent_crude',
        display_name='Brent Crude Futures',
        source_filename='investing.com_historical_brent_crude_oil_2000-2026.csv',
        metadata_filename='brent_crude_contracts.csv',
        historical_filename='brent_crude_historical.csv',
        roll_rule='brent',
        splice_last_seed_date='2026-04-06',
        derived_front_month_filename='brent_crude_front_month_ice.csv',
        derived_continuous_filename='brent_crude_continuous_daily.csv',
    ),
    ContinuousInstrument(
        slug='low_sulphur_gasoil',
        display_name='Low Sulphur Gasoil Futures',
        source_filename='investing.com_historical_london_gas_oil_2000-2026.csv',
        metadata_filename='low_sulphur_gasoil_contracts.csv',
        historical_filename='low_sulphur_gasoil_historical.csv',
        roll_rule='gasoil',
        splice_last_seed_date='2026-04-06',
        derived_front_month_filename='low_sulphur_gasoil_front_month_ice.csv',
        derived_continuous_filename='low_sulphur_gasoil_continuous_daily.csv',
    ),
)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    renamed.columns = [str(c).strip().replace('\ufeff', '') for c in renamed.columns]
    return renamed


def _parse_market_strip(market_strip: str) -> tuple[int, int]:
    value = str(market_strip).strip()
    month_txt = value[:3].title()
    year_txt = value[3:]
    if month_txt not in MONTH_MAP:
        raise ValueError(f'Unsupported market_strip month: {market_strip}')
    year = 2000 + int(year_txt)
    month = MONTH_MAP[month_txt]
    return year, month


UK_HOLIDAYS_BY_YEAR: dict[int, set[pd.Timestamp]] = {}


def _uk_holidays_for_year(year: int) -> set[pd.Timestamp]:
    cached = UK_HOLIDAYS_BY_YEAR.get(year)
    if cached is not None:
        return cached
    holiday_set = {pd.Timestamp(day).normalize() for day in holidays.country_holidays('GB', years=[year]).keys()}
    UK_HOLIDAYS_BY_YEAR[year] = holiday_set
    return holiday_set


def _is_business_day(ts: pd.Timestamp) -> bool:
    normalized = ts.normalize()
    return normalized.weekday() < 5 and normalized not in _uk_holidays_for_year(normalized.year)


def _previous_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    out = ts - pd.Timedelta(days=1)
    while not _is_business_day(out):
        out -= pd.Timedelta(days=1)
    return out


def _last_business_day_of_month(year: int, month: int) -> pd.Timestamp:
    last_day = calendar.monthrange(year, month)[1]
    ts = pd.Timestamp(year=year, month=month, day=last_day)
    while not _is_business_day(ts):
        ts -= pd.Timedelta(days=1)
    return ts


def _business_days_before(ts: pd.Timestamp, n: int) -> pd.Timestamp:
    out = ts
    for _ in range(n):
        out = _previous_business_day(out)
    return out


def _business_day_before_fixed_holiday(ts: pd.Timestamp, month: int, day: int) -> pd.Timestamp:
    holiday = pd.Timestamp(year=ts.year, month=month, day=day)
    before = holiday - pd.Timedelta(days=1)
    while not _is_business_day(before):
        before -= pd.Timedelta(days=1)
    return before


def _gasoil_last_trading_date(contract_year: int, contract_month: int) -> pd.Timestamp:
    anchor = pd.Timestamp(year=contract_year, month=contract_month, day=14)
    return _business_days_before(anchor, 2)


def _brent_last_trading_date(contract_year: int, contract_month: int) -> pd.Timestamp:
    preceding_year = contract_year
    preceding_month = contract_month - 2
    if preceding_month <= 0:
        preceding_month += 12
        preceding_year -= 1

    ltd = _last_business_day_of_month(preceding_year, preceding_month)
    christmas_eve_business_day = _business_day_before_fixed_holiday(ltd, 12, 25)
    new_years_eve_business_day = _business_day_before_fixed_holiday(ltd, 1, 1)

    if ltd == christmas_eve_business_day or ltd == new_years_eve_business_day:
        ltd = _previous_business_day(ltd)

    return ltd


def _compute_last_trading_date(market_strip: str, roll_rule: str) -> pd.Timestamp:
    year, month = _parse_market_strip(market_strip)
    if roll_rule == 'gasoil':
        return _gasoil_last_trading_date(year, month)
    if roll_rule == 'brent':
        return _brent_last_trading_date(year, month)
    raise ValueError(f'Unsupported roll_rule: {roll_rule}')


def _load_investing_seed(source_path: Path, instrument: ContinuousInstrument) -> pd.DataFrame:
    if not source_path.exists():
        raise FileNotFoundError(f'Missing source file: {source_path}')
    df = pd.read_csv(source_path, sep=None, engine='python', encoding='utf-8-sig')
    if df.empty:
        raise FileNotFoundError(f'Missing source file: {source_path}')
    df = _normalize_columns(df)
    required = {'Date', 'Price'}
    if not required.issubset(df.columns):
        raise ValueError(f'{source_path.name} missing required columns: {sorted(required - set(df.columns))}')

    out = pd.DataFrame()
    out['date'] = pd.to_datetime(df['Date'].astype(str).str.strip(), dayfirst=True, errors='raise').dt.strftime('%Y-%m-%d')
    out['settlement_price'] = pd.to_numeric(df['Price'], errors='coerce')

    volume_column = None
    for candidate in ('Vol., thsd.', 'Vol., thsd', 'Vol.'):
        if candidate in df.columns:
            volume_column = candidate
            break

    if volume_column is not None:
        out['volume_thsd'] = pd.to_numeric(df[volume_column], errors='coerce')
    else:
        out['volume_thsd'] = pd.NA

    out['instrument_slug'] = instrument.slug
    out['instrument_name'] = instrument.display_name
    out['source'] = 'investing_seed'
    out['series_type'] = 'continuous'
    out['roll_rule'] = instrument.roll_rule
    out['market_id'] = pd.NA
    out['market_strip'] = pd.NA
    out['last_trading_date'] = pd.NA
    out['splice_last_seed_date'] = instrument.splice_last_seed_date
    out = out.dropna(subset=['settlement_price']).drop_duplicates(subset=['date'], keep='first')
    return out.sort_values('date').reset_index(drop=True)


def _load_metadata(metadata_path: Path, instrument: ContinuousInstrument) -> pd.DataFrame:
    df = read_csv_if_exists(metadata_path, dtype={'market_id': 'Int64'})
    if df.empty:
        raise FileNotFoundError(f'Missing metadata file: {metadata_path}')
    df = _normalize_columns(df)
    df = df[['market_id', 'market_strip']].drop_duplicates().copy()
    df['last_trading_date'] = df['market_strip'].apply(lambda x: _compute_last_trading_date(str(x), instrument.roll_rule).strftime('%Y-%m-%d'))
    return df.sort_values(['last_trading_date', 'market_id']).reset_index(drop=True)


def _load_historical(historical_path: Path) -> pd.DataFrame:
    df = read_csv_if_exists(historical_path, dtype={'market_id': 'Int64'})
    if df.empty:
        raise FileNotFoundError(f'Missing historical file: {historical_path}')
    df = _normalize_columns(df)
    df['date'] = pd.to_datetime(df['date'], errors='raise').dt.strftime('%Y-%m-%d')
    df['settlement_price'] = pd.to_numeric(df['settlement_price'], errors='coerce')
    return df.dropna(subset=['settlement_price']).copy()


def _build_ice_front_month_series(instrument: ContinuousInstrument) -> pd.DataFrame:
    metadata = _load_metadata(METADATA_DIR / instrument.metadata_filename, instrument)
    historical = _load_historical(HISTORICAL_DIR / instrument.historical_filename)

    merged = historical.merge(metadata, on=['market_id', 'market_strip'], how='inner', suffixes=('', '_meta'))
    merged['date_ts'] = pd.to_datetime(merged['date'])
    merged['last_trading_date_ts'] = pd.to_datetime(merged['last_trading_date'])
    eligible = merged.loc[merged['date_ts'] <= merged['last_trading_date_ts']].copy()

    if eligible.empty:
        return pd.DataFrame(columns=['date', 'settlement_price', 'market_id', 'market_strip', 'last_trading_date', 'instrument_slug', 'instrument_name', 'source', 'series_type', 'roll_rule'])

    eligible = eligible.sort_values(['date', 'last_trading_date', 'market_id'])
    front = eligible.groupby('date', as_index=False).first()
    front['instrument_slug'] = instrument.slug
    front['instrument_name'] = instrument.display_name
    front['source'] = 'ice_front_month'
    front['series_type'] = 'continuous'
    front['roll_rule'] = instrument.roll_rule
    front = front[['date', 'settlement_price', 'market_id', 'market_strip', 'last_trading_date', 'instrument_slug', 'instrument_name', 'source', 'series_type', 'roll_rule']]
    return front.sort_values('date').reset_index(drop=True)


def _stitch_seed_and_ice(seed_df: pd.DataFrame, ice_df: pd.DataFrame, splice_last_seed_date: str) -> pd.DataFrame:
    seed_part = seed_df.loc[seed_df['date'] <= splice_last_seed_date].copy()
    ice_part = ice_df.loc[ice_df['date'] > splice_last_seed_date].copy()

    combined = pd.concat([seed_part, ice_part], ignore_index=True, sort=False)
    combined = combined.sort_values('date').drop_duplicates(subset=['date'], keep='last').reset_index(drop=True)
    combined['splice_last_seed_date'] = splice_last_seed_date
    return combined


def build_continuous_for_instrument(instrument: ContinuousInstrument) -> tuple[pd.DataFrame, pd.DataFrame]:
    seed = _load_investing_seed(SOURCE_DIR / 'investing' / instrument.source_filename, instrument)
    ice_front = _build_ice_front_month_series(instrument)
    continuous = _stitch_seed_and_ice(seed, ice_front, instrument.splice_last_seed_date)
    return ice_front, continuous


def build_and_store_continuous_series() -> dict[str, pd.DataFrame]:
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)

    all_continuous: list[pd.DataFrame] = []
    outputs: dict[str, pd.DataFrame] = {}

    for instrument in CONTINUOUS_INSTRUMENTS:
        ice_front, continuous = build_continuous_for_instrument(instrument)

        write_csv(ice_front, DERIVED_DIR / instrument.derived_front_month_filename)
        write_csv(continuous, DERIVED_DIR / instrument.derived_continuous_filename)

        all_continuous.append(continuous)
        outputs[instrument.slug] = continuous

    if all_continuous:
        combined = pd.concat(all_continuous, ignore_index=True, sort=False)
        combined = combined.sort_values(['instrument_slug', 'date']).reset_index(drop=True)
    else:
        combined = pd.DataFrame()

    write_csv(combined, DERIVED_DIR / 'energy_futures_continuous_daily.csv')
    outputs['combined'] = combined
    return outputs
