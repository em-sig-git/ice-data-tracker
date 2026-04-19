from __future__ import annotations

import argparse
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .client import IceClient
from .config import (
    DATA_DIR,
    HISTORICAL_DIR,
    DERIVED_DIR,
    INSTRUMENTS,
    LOG_DIR,
    LOG_FILE,
    METADATA_DIR,
    RIGA_TZ,
    SCHEDULE_STATE_FILE,
    STATE_DIR,
    TRACKING_HORIZON_MONTHS,
    Instrument,
)
from .storage import load_json, read_csv_if_exists, save_json, upsert_by_columns, write_csv
from .continuous import build_and_store_continuous_series


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def ensure_directories() -> None:
    for path in (DATA_DIR, METADATA_DIR, HISTORICAL_DIR, DERIVED_DIR, STATE_DIR, LOG_DIR):
        path.mkdir(parents=True, exist_ok=True)


def month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def add_months(dt: datetime, months: int) -> datetime:
    total = (dt.year * 12 + dt.month - 1) + months
    year = total // 12
    month = total % 12 + 1
    return dt.replace(year=year, month=month, day=1)


def parse_end_date_ms(value: int | float) -> tuple[pd.Timestamp, str]:
    ts = pd.to_datetime(int(value), unit="ms", utc=True)
    return ts, ts.strftime("%Y-%m-%d")


def should_keep_contract(end_date_utc: pd.Timestamp, now_riga: datetime) -> bool:
    current_month = month_start(now_riga)
    horizon_month = add_months(current_month, TRACKING_HORIZON_MONTHS)
    contract_month = end_date_utc.tz_convert(RIGA_TZ).to_pydatetime().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return contract_month <= horizon_month


def metadata_to_dataframe(raw_items: list[dict], instrument: Instrument, now_riga: datetime) -> pd.DataFrame:
    rows: list[dict] = []
    for item in raw_items:
        market_id = item.get("marketId")
        market_strip = item.get("marketStrip")
        end_date = item.get("endDate")
        if market_id is None or market_strip is None or end_date is None:
            continue
        end_ts_utc, end_date_utc = parse_end_date_ms(end_date)
        if not should_keep_contract(end_ts_utc, now_riga):
            continue
        rows.append(
            {
                "instrument_slug": instrument.slug,
                "instrument_name": instrument.name,
                "product_id": instrument.product_id,
                "hub_id": instrument.hub_id,
                "market_id": int(market_id),
                "market_strip": str(market_strip),
                "end_date_utc": end_date_utc,
                "metadata_scraped_at_EET": now_riga.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "instrument_slug",
                "instrument_name",
                "product_id",
                "hub_id",
                "market_id",
                "market_strip",
                "end_date_utc",
                "metadata_scraped_at_EET",
            ]
        )
    return df.sort_values(["end_date_utc", "market_id"]).reset_index(drop=True)


def metadata_csv_path(instrument: Instrument) -> Path:
    return METADATA_DIR / f"{instrument.slug}_contracts.csv"


def history_csv_path(instrument: Instrument) -> Path:
    return HISTORICAL_DIR / f"{instrument.slug}_historical.csv"


def fetch_and_store_metadata(client: IceClient, instruments: Iterable[Instrument], now_riga: datetime) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for instrument in instruments:
        try:
            raw_items = client.fetch_contract_metadata(instrument)
            new_df = metadata_to_dataframe(raw_items, instrument, now_riga)
            path = metadata_csv_path(instrument)
            old_df = read_csv_if_exists(path, dtype={"market_id": "Int64", "product_id": "Int64", "hub_id": "Int64"})
            final_df = upsert_by_columns(
                existing=old_df,
                incoming=new_df,
                key_columns=["market_id"],
                sort_columns=["end_date_utc", "market_id"],
            )
            write_csv(final_df, path)
            logging.info("Metadata updated: %s (%s rows)", path.name, len(final_df))
            result[instrument.slug] = final_df
        except Exception as exc:
            logging.exception("Metadata scrape failed for %s: %s", instrument.slug, exc)
    return result


def parse_bar_date(value: str) -> str:
    dt = datetime.strptime(value, "%a %b %d %H:%M:%S %Y")
    return dt.strftime("%Y-%m-%d")


def historical_payload_to_dataframe(payload: dict, instrument: Instrument, market_id: int, market_strip: str, end_date_utc: str, now_riga: datetime) -> pd.DataFrame:
    bars = payload.get("bars", [])
    rows: list[dict] = []
    for bar in bars:
        if not isinstance(bar, list) or len(bar) < 2:
            continue
        date_raw, settle_price = bar[0], bar[1]
        if date_raw is None:
            continue
        rows.append(
            {
                "date": parse_bar_date(str(date_raw)),
                "settlement_price": settle_price,
                "market_id": int(market_id),
                "market_strip": market_strip,
                "instrument_slug": instrument.slug,
                "instrument_name": instrument.name,
                "end_date_utc": end_date_utc,
                "scraped_at_EET": now_riga.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "settlement_price",
                "market_id",
                "market_strip",
                "instrument_slug",
                "instrument_name",
                "end_date_utc",
                "scraped_at_EET",
            ]
        )
    return df.sort_values(["date"]).reset_index(drop=True)


def fetch_and_store_historical(client: IceClient, metadata_tables: dict[str, pd.DataFrame], now_riga: datetime) -> int:
    success_count = 0

    for instrument in INSTRUMENTS:
        metadata_df = metadata_tables.get(instrument.slug)
        if metadata_df is None or metadata_df.empty:
            logging.warning("No tracked contracts for %s", instrument.slug)
            continue

        frames: list[pd.DataFrame] = []
        contract_success_count = 0

        for row in metadata_df.to_dict(orient="records"):
            market_id = int(row["market_id"])
            market_strip = str(row["market_strip"])
            end_date_utc = str(row["end_date_utc"])

            try:
                payload = client.fetch_historical(market_id)
                new_df = historical_payload_to_dataframe(
                    payload=payload,
                    instrument=instrument,
                    market_id=market_id,
                    market_strip=market_strip,
                    end_date_utc=end_date_utc,
                    now_riga=now_riga,
                )

                if not new_df.empty:
                    frames.append(new_df)

                contract_success_count += 1

            except Exception as exc:
                logging.exception(
                    "Historical scrape failed for %s %s (%s): %s",
                    instrument.slug,
                    market_strip,
                    market_id,
                    exc,
                )

        if not frames:
            logging.warning("No historical data collected for %s", instrument.slug)
            continue

        incoming_df = pd.concat(frames, ignore_index=True)
        path = history_csv_path(instrument)
        old_df = read_csv_if_exists(path, dtype={"market_id": "Int64"})

        final_df = upsert_by_columns(
            existing=old_df,
            incoming=incoming_df,
            key_columns=["date", "market_id"],
            sort_columns=["date", "end_date_utc", "market_id"],
        )

        write_csv(final_df, path)

        logging.info(
            "Historical updated: %s | contracts_ok=%s | rows=%s",
            path.name,
            contract_success_count,
            len(final_df),
        )

        success_count += contract_success_count

    return success_count


def load_or_refresh_metadata(client: IceClient, now_riga: datetime, force_refresh: bool) -> dict[str, pd.DataFrame]:
    metadata_tables: dict[str, pd.DataFrame] = {}
    missing_any = False
    for instrument in INSTRUMENTS:
        path = metadata_csv_path(instrument)
        if path.exists() and not force_refresh:
            metadata_tables[instrument.slug] = read_csv_if_exists(
                path,
                dtype={"market_id": "Int64", "product_id": "Int64", "hub_id": "Int64"},
            )
        else:
            missing_any = True
    if force_refresh or missing_any:
        refreshed = fetch_and_store_metadata(client, INSTRUMENTS, now_riga)
        metadata_tables.update(refreshed)
    return metadata_tables


def due_metadata_run(now_riga: datetime, state: dict) -> bool:
    slot = now_riga.strftime("%Y-%m-%d")
    last = state.get("last_metadata_run_local_date")
    return now_riga.day in {1, 15} and now_riga.hour == 9 and last != slot


def due_historical_run(now_riga: datetime, state: dict) -> bool:
    slot = now_riga.strftime("%Y-%m-%d %H")
    last = state.get("last_historical_run_local_hour")
    return now_riga.hour in {9, 21} and last != slot


def update_schedule_state(state: dict, *, metadata_run: bool, historical_run: bool, now_riga: datetime) -> None:
    if metadata_run:
        state["last_metadata_run_local_date"] = now_riga.strftime("%Y-%m-%d")
    if historical_run:
        state["last_historical_run_local_hour"] = now_riga.strftime("%Y-%m-%d %H")
    state["last_runner_check_local"] = now_riga.strftime("%Y-%m-%d %H:%M:%S")
    save_json(SCHEDULE_STATE_FILE, state)


def run_scheduled() -> None:
    now_riga = datetime.now(RIGA_TZ).replace(minute=0, second=0, microsecond=0)
    state = load_json(SCHEDULE_STATE_FILE)
    client = IceClient()
    metadata_run = False
    historical_run = False

    if due_metadata_run(now_riga, state):
        metadata_tables = fetch_and_store_metadata(client, INSTRUMENTS, now_riga)
        metadata_run = bool(metadata_tables)
    else:
        metadata_tables = load_or_refresh_metadata(client, now_riga, force_refresh=False)

    if due_historical_run(now_riga, state):
        if not metadata_tables:
            metadata_tables = load_or_refresh_metadata(client, now_riga, force_refresh=False)
        historical_success_count = fetch_and_store_historical(client, metadata_tables, now_riga)
        historical_run = historical_success_count > 0
        if historical_run:
            build_and_store_continuous_series()

    if not metadata_run and not historical_run:
        logging.info("No scheduled scrape due at %s", now_riga.isoformat())

    update_schedule_state(state, metadata_run=metadata_run, historical_run=historical_run, now_riga=now_riga)


def run_manual(mode: str) -> None:
    now_riga = datetime.now(RIGA_TZ).replace(second=0, microsecond=0)
    client = IceClient()

    if mode == "metadata":
        fetch_and_store_metadata(client, INSTRUMENTS, now_riga)
        return

    metadata_tables = load_or_refresh_metadata(client, now_riga, force_refresh=(mode == "all"))

    if mode == "historical":
        fetch_and_store_historical(client, metadata_tables, now_riga)
        build_and_store_continuous_series()
        return

    if mode == "all":
        fetch_and_store_historical(client, metadata_tables, now_riga)
        build_and_store_continuous_series()
        return

    if mode == "build_continuous":
        build_and_store_continuous_series()
        return

    raise ValueError(f"Unsupported mode: {mode}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ICE market data tracker")
    parser.add_argument(
        "--mode",
        choices=["scheduled", "metadata", "historical", "all", "build_continuous"],
        default="scheduled",
        help="Run the scheduler gate or force a concrete scrape mode.",
    )
    return parser.parse_args()


def main() -> None:
    ensure_directories()
    setup_logging()
    args = parse_args()
    logging.info("Run started: mode=%s", args.mode)
    if args.mode == "scheduled":
        run_scheduled()
    else:
        run_manual(args.mode)
    logging.info("Run finished: mode=%s", args.mode)


if __name__ == "__main__":
    main()
