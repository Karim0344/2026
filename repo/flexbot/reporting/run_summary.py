from __future__ import annotations

import json
from pathlib import Path


def save_run_summary(summary: dict, report_dir: str) -> Path:
    p = Path(report_dir)
    p.mkdir(parents=True, exist_ok=True)
    target = p / "run_summary.json"
    target.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return target
