from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from datetime import datetime, timezone

from .config import DERIVED_DIR, HISTORICAL_DIR, METADATA_DIR, SOURCE_DIR
from .expiry import compute_last_trading_date
from .storage import frames_have_same_values, read_csv_if_exists, write_csv


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
    ),
)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    renamed.columns = [str(c).strip().replace('\ufeff', '') for c in renamed.columns]
    return renamed

def _keep_monday_to_friday(df: pd.DataFrame, date_column: str = 'date') -> pd.DataFrame:
    out = df.copy()
    date_ts = pd.to_datetime(out[date_column], errors='coerce')
    out = out.loc[date_ts.dt.weekday < 5].copy()
    return out.reset_index(drop=True)

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
    out = _keep_monday_to_friday(out, 'date')

    return out.sort_values('date').reset_index(drop=True)


def _load_metadata(metadata_path: Path, instrument: ContinuousInstrument) -> pd.DataFrame:
    df = read_csv_if_exists(metadata_path, dtype={'market_id': 'Int64'})
    if df.empty:
        raise FileNotFoundError(f'Missing metadata file: {metadata_path}')
    df = _normalize_columns(df)
    df = df[['market_id', 'market_strip']].drop_duplicates().copy()
    df['last_trading_date'] = df['market_strip'].apply(
        lambda x: compute_last_trading_date(str(x), instrument.roll_rule).strftime('%Y-%m-%d')
    )
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
        return pd.DataFrame(columns=[
            'date', 'settlement_price', 'market_id', 'market_strip', 'last_trading_date',
            'instrument_slug', 'instrument_name', 'source', 'series_type', 'roll_rule'
        ])

    eligible = eligible.sort_values(['date', 'last_trading_date', 'market_id'])
    front = eligible.groupby('date', as_index=False).first()
    front['instrument_slug'] = instrument.slug
    front['instrument_name'] = instrument.display_name
    front['source'] = 'ice_front_month'
    front['series_type'] = 'continuous'
    front['roll_rule'] = instrument.roll_rule
    front = front[[
        'date', 'settlement_price', 'market_id', 'market_strip', 'last_trading_date',
        'instrument_slug', 'instrument_name', 'source', 'series_type', 'roll_rule'
    ]]
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

        all_continuous.append(continuous)
        outputs[instrument.slug] = continuous

    if all_continuous:
        combined = pd.concat(all_continuous, ignore_index=True, sort=False)
        combined = combined.sort_values(['instrument_slug', 'date']).reset_index(drop=True)
    else:
        combined = pd.DataFrame()

    # Only move last_update_timestamp when the data behind it changed. Stamping
    # it unconditionally rewrote all ~13,700 rows on every run, so the file
    # always looked 100% modified and the workflow's "nothing to commit" check
    # could never fire.
    combined_path = DERIVED_DIR / 'energy_futures_continuous_daily.csv'
    previous = read_csv_if_exists(combined_path)
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    if not previous.empty and 'last_update_timestamp' in previous.columns:
        previous_data = previous.drop(columns=['last_update_timestamp'])
        if frames_have_same_values(previous_data, combined):
            timestamp = str(previous['last_update_timestamp'].iloc[0])

    combined['last_update_timestamp'] = timestamp

    write_csv(combined, combined_path)
    outputs['combined'] = combined
    return outputs