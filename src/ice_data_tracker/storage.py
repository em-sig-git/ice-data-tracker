from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .config import CSV_DECIMAL, CSV_ENCODING, CSV_SEPARATOR


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_csv_if_exists(path: Path, *, parse_dates: list[str] | None = None, dtype: dict | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, sep=CSV_SEPARATOR, decimal=CSV_DECIMAL, encoding=CSV_ENCODING, parse_dates=parse_dates, dtype=dtype)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    df.to_csv(path, sep=CSV_SEPARATOR, decimal=CSV_DECIMAL, encoding=CSV_ENCODING, index=False)


def upsert_by_columns(existing: pd.DataFrame, incoming: pd.DataFrame, key_columns: list[str], sort_columns: list[str]) -> pd.DataFrame:
    if existing.empty:
        result = incoming.copy()
    elif incoming.empty:
        result = existing.copy()
    else:
        combined = pd.concat([incoming, existing], ignore_index=True)
        result = combined.drop_duplicates(subset=key_columns, keep="first")
    return result.sort_values(sort_columns).reset_index(drop=True)


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
