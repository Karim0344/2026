from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from flexbot.core.config import BotConfig
from flexbot.data.history_store import load_frame, save_frame
from flexbot.mt5 import client

Timeframe = Literal["M1", "M5", "M15", "H1", "H4"]


class HistoricalDataRecorder:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg

    def _history_path(self, symbol: str, timeframe: str) -> Path:
        return Path(self.cfg.store_history_path) / symbol / f"{timeframe}.parquet"

    def fetch_mt5_history(self, symbol: str, timeframe: Timeframe, bars: int) -> pd.DataFrame:
        rates = client.copy_rates(symbol=symbol, timeframe=timeframe, bars=bars)
        if rates is None or len(rates) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(rates)
        for col in ("spread", "real_volume"):
            if col not in df.columns:
                df[col] = 0

        out = df[["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]].copy()
        out["time"] = pd.to_datetime(out["time"], unit="s", utc=True)
        out = self._add_calendar_features(out)
        return out.sort_values("time").reset_index(drop=True)

    def append_history_store(self, symbol: str, timeframe: Timeframe, df: pd.DataFrame) -> pd.DataFrame:
        path = self._history_path(symbol, timeframe)
        if df.empty:
            return load_frame(path)

        existing = load_frame(path)
        if not existing.empty and "time" in existing:
            existing["time"] = pd.to_datetime(existing["time"], utc=True)

        merged = pd.concat([existing, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"], keep="last").sort_values("time")
        save_frame(path, merged.reset_index(drop=True))
        return merged.reset_index(drop=True)

    def load_history(self, symbol: str, timeframe: Timeframe) -> pd.DataFrame:
        path = self._history_path(symbol, timeframe)
        df = load_frame(path)
        if not df.empty and "time" in df:
            df["time"] = pd.to_datetime(df["time"], utc=True)
        return df

    def refresh_history(self, symbol: str, timeframe: Timeframe) -> pd.DataFrame:
        bars_map = {
            "M5": self.cfg.history_bars_m5,
            "M15": self.cfg.history_bars_m15,
            "H1": self.cfg.history_bars_h1,
        }
        bars = int(bars_map.get(timeframe, self.cfg.history_bars_m5))
        latest = self.fetch_mt5_history(symbol=symbol, timeframe=timeframe, bars=bars)
        return self.append_history_store(symbol=symbol, timeframe=timeframe, df=latest)

    @staticmethod
    def _add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["weekday"] = out["time"].dt.weekday
        out["hour"] = out["time"].dt.hour
        out["minute"] = out["time"].dt.minute
        out["week_number"] = out["time"].dt.isocalendar().week.astype(int)
        out["month"] = out["time"].dt.month
        out["session_name"] = out["hour"].map(_session_from_hour)
        return out


def _session_from_hour(hour: int) -> str:
    if 0 <= hour < 7:
        return "Asia"
    if 7 <= hour < 13:
        return "London"
    if 13 <= hour < 17:
        return "London/NY_overlap"
    return "New_York"
