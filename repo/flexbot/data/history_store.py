from __future__ import annotations

from pathlib import Path
import logging
import pandas as pd


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _resolve_read_path(path: Path) -> Path | None:
    if path.exists():
        return path
    if path.suffix == ".parquet":
        for suffix in (".csv", ".jsonl"):
            alt = path.with_suffix(suffix)
            if alt.exists():
                return alt
    return None


def load_frame(path: Path) -> pd.DataFrame:
    actual = _resolve_read_path(path)
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


def save_frame(path: Path, df: pd.DataFrame) -> None:
    ensure_parent(path)
    if path.suffix == ".parquet":
        try:
            df.to_parquet(path, index=False)
            return
        except Exception as exc:
            fallback = path.with_suffix(".csv")
            logging.warning(
                "PARQUET_WRITE_FAILED path=%s err=%s fallback=%s",
                path,
                exc,
                fallback,
            )
            df.to_csv(fallback, index=False)
            return
    else:
        df.to_csv(path, index=False)
