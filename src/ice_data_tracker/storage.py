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


def normalize_for_comparison(series: pd.Series) -> pd.Series:
    """Canonical string form of a column, for 'did this value actually change?'.

    Values are compared the way they round-trip through the CSV: numbers are
    normalised (so 88.10 read back as 88.1 still compares equal) and everything
    else is compared as trimmed text.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    out = series.astype("string").str.strip()
    mask = numeric.notna()
    if mask.any():
        out[mask] = numeric[mask].map(lambda v: format(float(v), ".10g")).astype("string")
    return out.fillna("")


def frames_have_same_values(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    """True when two frames hold the same values, ignoring dtype and formatting."""
    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    for column in left.columns:
        a = normalize_for_comparison(left[column]).reset_index(drop=True)
        b = normalize_for_comparison(right[column]).reset_index(drop=True)
        if not a.equals(b):
            return False
    return True


def upsert_keeping_timestamps(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    key_columns: list[str],
    sort_columns: list[str],
    timestamp_column: str,
) -> pd.DataFrame:
    """Upsert rows, but only move a row's timestamp when its data really changed.

    Same result as :func:`upsert_by_columns` for every column except
    ``timestamp_column``: incoming data always wins, so a faulty comparison can
    never resurrect a stale price. The only thing carried over from the existing
    row is its timestamp, and only when every other value is unchanged.

    Without this, re-fetching a contract's full three-year span twice a day
    rewrote the timestamp on thousands of rows whose prices had not moved,
    which made every run produce a whole-file diff.
    """
    if existing.empty:
        return incoming.copy().sort_values(sort_columns).reset_index(drop=True)
    if incoming.empty:
        return existing.copy().sort_values(sort_columns).reset_index(drop=True)
    if timestamp_column not in incoming.columns or timestamp_column not in existing.columns:
        return upsert_by_columns(existing, incoming, key_columns, sort_columns)

    value_columns = [
        c for c in incoming.columns if c not in key_columns and c != timestamp_column
    ]
    shared = [c for c in value_columns if c in existing.columns]

    old = existing.drop_duplicates(subset=key_columns, keep="first")
    lookup = old[key_columns + shared + [timestamp_column]].add_suffix("__old")
    lookup.columns = key_columns + [f"{c}__old" for c in shared] + [f"{timestamp_column}__old"]

    merged = incoming.merge(lookup, on=key_columns, how="left")

    matched = merged[f"{timestamp_column}__old"].notna()
    unchanged = matched.copy()
    for column in shared:
        unchanged &= normalize_for_comparison(merged[column]).eq(
            normalize_for_comparison(merged[f"{column}__old"])
        )

    merged.loc[unchanged, timestamp_column] = merged.loc[unchanged, f"{timestamp_column}__old"]
    result_incoming = merged[incoming.columns]

    # Rows we did not re-request this run (e.g. expired contracts) are kept as-is.
    incoming_keys = set(map(tuple, incoming[key_columns].astype(str).itertuples(index=False)))
    keep_mask = ~old[key_columns].astype(str).apply(tuple, axis=1).isin(incoming_keys)
    untouched = old.loc[keep_mask]

    combined = pd.concat([result_incoming, untouched], ignore_index=True)
    combined = combined.drop_duplicates(subset=key_columns, keep="first")
    return combined.sort_values(sort_columns).reset_index(drop=True)
