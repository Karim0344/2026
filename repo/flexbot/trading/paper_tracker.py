from dataclasses import asdict, dataclass
import json
import os


@dataclass
class PaperTrade:
    batch_id: str
    symbol: str
    timeframe: str
    is_long: bool
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    created_bar_time: int
    status: str = "open"
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    sl_hit: bool = False
    closed_bar_time: int = 0
    signal_reason: str = ""


def load_paper_trades(path: str = "paper_trades.json") -> list[PaperTrade]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [PaperTrade(**item) for item in data]
    except Exception:
        return []


def save_paper_trades(trades: list[PaperTrade], path: str = "paper_trades.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(t) for t in trades], f, ensure_ascii=False, indent=2)


def upsert_paper_trade(trade: PaperTrade, path: str = "paper_trades.json") -> None:
    trades = load_paper_trades(path)
    for idx, existing in enumerate(trades):
        if existing.batch_id == trade.batch_id:
            trades[idx] = trade
            save_paper_trades(trades, path)
            save_paper_stats(trades)
            return
    trades.append(trade)
    save_paper_trades(trades, path)
    save_paper_stats(trades)


def _update_trade_with_bar(
    trade: PaperTrade, bar_time: int, bar_high: float, bar_low: float
) -> tuple[PaperTrade, bool]:
    if trade.status != "open":
        return trade, False

    changed = False
    if trade.is_long:
        if bar_low <= trade.sl:
            trade.sl_hit = True
            trade.status = "sl_hit"
            trade.closed_bar_time = bar_time
            return trade, True
        if bar_high >= trade.tp1:
            trade.tp1_hit = True
            changed = True
        if bar_high >= trade.tp2:
            trade.tp2_hit = True
            changed = True
        if bar_high >= trade.tp3:
            trade.tp3_hit = True
            trade.status = "tp3_hit"
            trade.closed_bar_time = bar_time
            return trade, True
        if trade.tp2_hit and trade.status != "tp2_hit":
            trade.status = "tp2_hit"
            changed = True
        elif trade.tp1_hit and trade.status != "tp1_hit":
            trade.status = "tp1_hit"
            changed = True
    else:
        if bar_high >= trade.sl:
            trade.sl_hit = True
            trade.status = "sl_hit"
            trade.closed_bar_time = bar_time
            return trade, True
        if bar_low <= trade.tp1:
            trade.tp1_hit = True
            changed = True
        if bar_low <= trade.tp2:
            trade.tp2_hit = True
            changed = True
        if bar_low <= trade.tp3:
            trade.tp3_hit = True
            trade.status = "tp3_hit"
            trade.closed_bar_time = bar_time
            return trade, True
        if trade.tp2_hit and trade.status != "tp2_hit":
            trade.status = "tp2_hit"
            changed = True
        elif trade.tp1_hit and trade.status != "tp1_hit":
            trade.status = "tp1_hit"
            changed = True

    return trade, changed


def save_paper_stats(trades: list[PaperTrade], path: str = "paper_stats.json") -> None:
    closed = [t for t in trades if t.status in {"sl_hit", "tp3_hit"}]
    total_closed = len(closed)
    wins = sum(1 for t in closed if t.status == "tp3_hit")
    losses = sum(1 for t in closed if t.status == "sl_hit")
    avg_r = ((wins * 3.0) + (losses * -1.0)) / total_closed if total_closed else 0.0

    per_reason: dict[str, dict[str, float | int]] = {}
    for t in closed:
        key = t.signal_reason or "unknown"
        if key not in per_reason:
            per_reason[key] = {"count": 0, "wins": 0, "losses": 0, "avg_r": 0.0}
        per_reason[key]["count"] += 1
        if t.status == "tp3_hit":
            per_reason[key]["wins"] += 1
        else:
            per_reason[key]["losses"] += 1

    for key, val in per_reason.items():
        count = int(val["count"])
        if count > 0:
            val["avg_r"] = ((int(val["wins"]) * 3.0) + (int(val["losses"]) * -1.0)) / count

    stats = {
        "total": len(trades),
        "open": sum(1 for t in trades if t.status == "open"),
        "closed": total_closed,
        "winrate": (wins / total_closed) if total_closed else 0.0,
        "avg_r": avg_r,
        "sl_hit": sum(1 for t in trades if t.sl_hit),
        "tp1_hit": sum(1 for t in trades if t.tp1_hit),
        "tp2_hit": sum(1 for t in trades if t.tp2_hit),
        "tp3_hit": sum(1 for t in trades if t.tp3_hit),
        "per_reason": per_reason,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def update_open_paper_trades(
    symbol: str,
    timeframe: str,
    bar_time: int,
    bar_high: float,
    bar_low: float,
    path: str = "paper_trades.json",
) -> list[PaperTrade]:
    trades = load_paper_trades(path)
    changed = False
    updates: list[PaperTrade] = []
    for idx, trade in enumerate(trades):
        if trade.symbol != symbol or trade.timeframe != timeframe:
            continue
        updated, was_changed = _update_trade_with_bar(trade, bar_time, bar_high, bar_low)
        if was_changed:
            trades[idx] = updated
            updates.append(updated)
            changed = True
    if changed:
        save_paper_trades(trades, path)
        save_paper_stats(trades)
    return updates
