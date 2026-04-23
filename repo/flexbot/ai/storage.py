from __future__ import annotations

from pathlib import Path
import logging

import pandas as pd


def read_table(preferred_path: Path) -> pd.DataFrame:
    actual = resolve_existing_path(preferred_path)
    if actual is None:
        return pd.DataFrame()
    if actual.suffix == ".parquet":
        try:
            return pd.read_parquet(actual)
        except Exception as exc:
            logging.warning("PARQUET_READ_FAILED path=%s err=%s", actual, exc)
            csv_alt = actual.with_suffix(".csv")
            if csv_alt.exists():
                logging.warning("STORAGE_FALLBACK_READ path=%s", csv_alt)
                return pd.read_csv(csv_alt)
            jsonl_alt = actual.with_suffix(".jsonl")
            if jsonl_alt.exists():
                logging.warning("STORAGE_FALLBACK_READ path=%s", jsonl_alt)
                return pd.read_json(jsonl_alt, lines=True)
            return pd.DataFrame()
    if actual.suffix == ".jsonl":
        return pd.read_json(actual, lines=True)
    return pd.read_csv(actual)


def write_table(df: pd.DataFrame, preferred_path: Path) -> Path:
    preferred_path.parent.mkdir(parents=True, exist_ok=True)
    if preferred_path.suffix == ".parquet":
        try:
            df.to_parquet(preferred_path, index=False)
            return preferred_path
        except Exception as exc:
            csv_fallback = preferred_path.with_suffix(".csv")
            logging.warning(
                "PARQUET_WRITE_FAILED path=%s err=%s fallback=%s",
                preferred_path,
                exc,
                csv_fallback,
            )
            df.to_csv(csv_fallback, index=False)
            return csv_fallback
    if preferred_path.suffix == ".jsonl":
        df.to_json(preferred_path, orient="records", lines=True)
        return preferred_path
    df.to_csv(preferred_path, index=False)
    return preferred_path


def resolve_existing_path(preferred_path: Path) -> Path | None:
    if preferred_path.exists():
        return preferred_path
    if preferred_path.suffix == ".parquet":
        for suffix in (".csv", ".jsonl"):
            alt = preferred_path.with_suffix(suffix)
            if alt.exists():
                return alt
    return None
