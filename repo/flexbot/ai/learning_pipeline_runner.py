from __future__ import annotations

from flexbot.core.config import load_bot_config
from flexbot.ai.learning_pipeline import LearningPipeline


def main() -> None:
    cfg = load_bot_config("config.json")
    pipeline = LearningPipeline(cfg)
    pipeline.run(symbol=cfg.symbol)


if __name__ == "__main__":
    main()
