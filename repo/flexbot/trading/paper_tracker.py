from dataclasses import asdict, dataclass, field
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
    confidence_score: int = 0
    features: dict = field(default_factory=dict)
    initial_r: float = 0.0
    mfe_r: float = 0.0
    mae_r: float = 0.0
    exit_reason: str = ""
    result_r: float = 0.0


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
    if trade.result_r:
        return float(trade.result_r)
    if trade.status == "sl_hit":
        return -1.0
    if trade.status == "tp3_hit":
        return 3.0
    if trade.status == "tp2_hit":
        return 2.0
    if trade.status == "tp1_hit":
        return 1.0
    return 0.0


def _trade_strategy_type(trade: PaperTrade) -> str:
    reason = (trade.signal_reason or "").upper()
    if reason.startswith("RANGE"):
        return "range"
    if "LONG" in reason:
        return "trend_long"
    if "SHORT" in reason:
        return "trend_short"
    return "other"


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
    by_strategy: dict[str, dict] = {}
    by_side: dict[str, dict] = {"long": {"count": 0, "wins": 0, "total_r": 0.0}, "short": {"count": 0, "wins": 0, "total_r": 0.0}}

    for t in trades:
        if t.status == "open":
            open_count += 1
            continue

        closed_count += 1
        rr = _trade_realized_r(t)
        total_r += rr
        strategy_type = _trade_strategy_type(t)
        if strategy_type not in by_strategy:
            by_strategy[strategy_type] = {"count": 0, "wins": 0, "total_r": 0.0}
        by_strategy[strategy_type]["count"] += 1
        by_strategy[strategy_type]["total_r"] += rr
        if rr > 0:
            by_strategy[strategy_type]["wins"] += 1

        side_key = "long" if t.is_long else "short"
        by_side[side_key]["count"] += 1
        by_side[side_key]["total_r"] += rr
        if rr > 0:
            by_side[side_key]["wins"] += 1

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

    strategy_summary = {}
    for key, raw in by_strategy.items():
        count = raw["count"]
        strategy_summary[key] = {
            "count": count,
            "winrate": round((raw["wins"] / count * 100.0), 2) if count else 0.0,
            "avg_r": round((raw["total_r"] / count), 3) if count else 0.0,
        }

    side_summary = {}
    for key, raw in by_side.items():
        count = raw["count"]
        side_summary[key] = {
            "count": count,
            "winrate": round((raw["wins"] / count * 100.0), 2) if count else 0.0,
            "avg_r": round((raw["total_r"] / count), 3) if count else 0.0,
        }

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
        "by_strategy": strategy_summary,
        "by_side": side_summary,
    }


def save_paper_stats(path: str = "paper_trades.json", stats_path: str = "paper_stats.json") -> dict:
    stats = compute_paper_stats(path=path)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    return stats


def load_paper_stats(stats_path: str = "paper_stats.json") -> dict:
    if not os.path.exists(stats_path):
        return {
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
            "by_strategy": {},
            "by_side": {
                "long": {"count": 0, "winrate": 0.0, "avg_r": 0.0},
                "short": {"count": 0, "winrate": 0.0, "avg_r": 0.0},
            },
        }
    try:
        with open(stats_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
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
            "by_strategy": {},
            "by_side": {
                "long": {"count": 0, "winrate": 0.0, "avg_r": 0.0},
                "short": {"count": 0, "winrate": 0.0, "avg_r": 0.0},
            },
        }


def _update_trade_with_bar(
    trade: PaperTrade,
    bar_time: int,
    bar_high: float,
    bar_low: float,
) -> tuple[PaperTrade, bool]:
    if trade.status != "open":
        return trade, False

    changed = False
    r_value = trade.initial_r if trade.initial_r > 0 else abs(trade.entry - trade.sl)
    if r_value > 0:
        favorable = (bar_high - trade.entry) if trade.is_long else (trade.entry - bar_low)
        adverse = (trade.entry - bar_low) if trade.is_long else (bar_high - trade.entry)
        new_mfe = max(float(trade.mfe_r), favorable / r_value)
        new_mae = max(float(trade.mae_r), adverse / r_value)
        if new_mfe != trade.mfe_r or new_mae != trade.mae_r:
            trade.mfe_r = round(new_mfe, 4)
            trade.mae_r = round(new_mae, 4)
            changed = True

    if trade.is_long:
        if bar_low <= trade.sl:
            trade.sl_hit = True
            trade.status = "sl_hit"
            trade.exit_reason = "SL"
            trade.result_r = -1.0
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
            trade.exit_reason = "TP3"
            trade.result_r = 3.0
            trade.closed_bar_time = bar_time
            return trade, True

        if trade.tp2_hit and trade.status != "tp2_hit":
            trade.status = "tp2_hit"
            trade.exit_reason = "TP2"
            trade.result_r = 2.0
            changed = True
        elif trade.tp1_hit and trade.status != "tp1_hit":
            trade.status = "tp1_hit"
            trade.exit_reason = "TP1"
            trade.result_r = 1.0
            changed = True

    else:
        if bar_high >= trade.sl:
            trade.sl_hit = True
            trade.status = "sl_hit"
            trade.exit_reason = "SL"
            trade.result_r = -1.0
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
            trade.exit_reason = "TP3"
            trade.result_r = 3.0
            trade.closed_bar_time = bar_time
            return trade, True

        if trade.tp2_hit and trade.status != "tp2_hit":
            trade.status = "tp2_hit"
            trade.exit_reason = "TP2"
            trade.result_r = 2.0
            changed = True
        elif trade.tp1_hit and trade.status != "tp1_hit":
            trade.status = "tp1_hit"
            trade.exit_reason = "TP1"
            trade.result_r = 1.0
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
