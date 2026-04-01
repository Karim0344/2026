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
            save_paper_stats(path=path)
            return
    trades.append(trade)
    save_paper_trades(trades, path)
    save_paper_stats(path=path)


def _trade_realized_r(trade: PaperTrade) -> float:
    if trade.status == "sl_hit":
        return -1.0
    if trade.status == "tp3_hit":
        return 3.0
    if trade.status == "tp2_hit":
        return 2.0
    if trade.status == "tp1_hit":
        return 1.0
    return 0.0


def compute_paper_stats(path: str = "paper_trades.json") -> dict:
    trades = load_paper_trades(path)

    total = len(trades)
    open_count = 0
    sl_count = 0
    tp1_count = 0
    tp2_count = 0
    tp3_count = 0
    closed_count = 0
    wins = 0
    total_r = 0.0

    for t in trades:
        if t.status == "open":
            open_count += 1
            continue

        closed_count += 1
        rr = _trade_realized_r(t)
        total_r += rr

        if t.status == "sl_hit":
            sl_count += 1
        elif t.status == "tp1_hit":
            tp1_count += 1
            wins += 1
        elif t.status == "tp2_hit":
            tp2_count += 1
            wins += 1
        elif t.status == "tp3_hit":
            tp3_count += 1
            wins += 1

    winrate = (wins / closed_count * 100.0) if closed_count > 0 else 0.0
    avg_r = (total_r / closed_count) if closed_count > 0 else 0.0

    return {
        "total": total,
        "open": open_count,
        "closed": closed_count,
        "wins": wins,
        "losses": sl_count,
        "tp1": tp1_count,
        "tp2": tp2_count,
        "tp3": tp3_count,
        "winrate": round(winrate, 2),
        "avg_r": round(avg_r, 2),
        "total_r": round(total_r, 2),
    }


def save_paper_stats(path: str = "paper_trades.json", stats_path: str = "paper_stats.json") -> dict:
    stats = compute_paper_stats(path=path)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    return stats


def load_paper_stats(stats_path: str = "paper_stats.json") -> dict:
    default = {
        "total": 0,
        "open": 0,
        "closed": 0,
        "wins": 0,
        "losses": 0,
        "tp1": 0,
        "tp2": 0,
        "tp3": 0,
        "winrate": 0.0,
        "avg_r": 0.0,
        "total_r": 0.0,
    }
    if not os.path.exists(stats_path):
        return default
    try:
        with open(stats_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _update_trade_with_bar(
    trade: PaperTrade,
    bar_time: int,
    bar_high: float,
    bar_low: float,
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
        save_paper_stats(path=path)

    return updates
