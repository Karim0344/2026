import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from flexbot.trading.paper_tracker import PaperTrade


def _append_jsonl(path: str, payload: dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def log_trade_open(trade: PaperTrade, path: str) -> None:
    payload = asdict(trade)
    payload["event"] = "open"
    payload["event_time_utc"] = datetime.now(timezone.utc).isoformat()
    _append_jsonl(path, payload)


def log_trade_close(trade: PaperTrade, result_r: float, path: str) -> None:
    payload = asdict(trade)
    payload["event"] = "close"
    payload["result_r"] = float(result_r)
    payload["event_time_utc"] = datetime.now(timezone.utc).isoformat()
    _append_jsonl(path, payload)
