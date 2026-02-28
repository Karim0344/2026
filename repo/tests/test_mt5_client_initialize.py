import importlib
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch


class TestMt5Initialize(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fake_mt5 = SimpleNamespace(
            TIMEFRAME_M1=1,
            TIMEFRAME_M5=5,
            TIMEFRAME_M15=15,
            TIMEFRAME_H1=60,
            TIMEFRAME_H4=240,
        )
        sys.modules.setdefault("MetaTrader5", fake_mt5)
        cls.client = importlib.import_module("flexbot.mt5.client")

    def test_fallback_to_no_path_after_failure(self):
        calls = []

        def fake_initialize(**kwargs):
            calls.append(kwargs)
            return len(calls) > 1

        with patch("flexbot.mt5.client.os.path.exists", return_value=True), patch(
            "flexbot.mt5.client.mt5.initialize", side_effect=fake_initialize, create=True
        ), patch("flexbot.mt5.client.mt5.last_error", return_value=(-1, "boom"), create=True), patch(
            "flexbot.mt5.client.mt5.terminal_info", return_value=SimpleNamespace(path="C:/MT5/terminal64.exe"), create=True
        ):
            used = self.client.initialize(terminal_path="C:/MT5/terminal64.exe", retries=2)

        self.assertEqual(used, "C:/MT5/terminal64.exe")
        self.assertIn("path", calls[0])
        self.assertNotIn("path", calls[1])

    def test_authorization_failure_raises(self):
        with patch("flexbot.mt5.client.mt5.initialize", return_value=True, create=True), patch(
            "flexbot.mt5.client.mt5.login", return_value=False, create=True
        ), patch("flexbot.mt5.client.mt5.last_error", return_value=(-6, "auth failed"), create=True), patch(
            "flexbot.mt5.client.mt5.shutdown", create=True
        ), patch("flexbot.mt5.client.mt5.terminal_info", return_value=None, create=True):
            with self.assertRaises(RuntimeError):
                self.client.initialize(login=123, password="bad", server="srv")


if __name__ == "__main__":
    unittest.main()
