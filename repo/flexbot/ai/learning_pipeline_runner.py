from __future__ import annotations

from flexbot.core.config import BotConfig
from flexbot.ai.learning_pipeline import LearningPipeline


def main() -> None:
    cfg = BotConfig()
    pipeline = LearningPipeline(cfg)
    pipeline.run(symbol=cfg.symbol)


if __name__ == "__main__":
    main()
