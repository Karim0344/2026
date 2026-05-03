from flexbot.core.config import load_bot_config


def test_load_bot_config_reads_config_json(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text("{\n  \"symbol\": \"EURUSD\",\n  \"timeframe\": \"M15\"\n}", encoding="utf-8")
    cfg = load_bot_config(str(cfg_file))
    assert cfg.symbol == "EURUSD"
    assert cfg.timeframe == "M15"
