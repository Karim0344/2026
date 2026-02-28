FLEXBOT PYTHON v1 (REBUILD)

Dit is een volledig opnieuw opgebouwde bot met:
- 1% risk per batch (3 trades: TP1=1R, TP2=2R, TP3=3R)
- Max 1 batch tegelijk
- Daily stop: -3% equity
- Stop na 3 losing batches op rij (reset volgende dag)
- SL -> BE zodra TP1 gesloten is met winst
- ATR trailing op TP2/TP3 na BE
- Entry alleen op candle-close (nieuwe bar) op gekozen timeframe
- Management realtime via timer-loop

Gebruik:
1) Open MT5 terminal, log in, zet Algo Trading aan.
2) Start deze bot via start_bot.bat
3) GUI: kies symbool exact zoals in MT5 (let op suffix zoals XAUUSD#, BTCUSDm, etc.)
4) Klik Start.

Logs:
- flexbot.log (in dezelfde map)

Let op:
- Dit is v1: mechanische edge (MA200 trend + MA50 pullback + RSI + trigger)
- Later kan de Signal Engine vervangen worden door AI zonder execution te breken.


CONFIG (optional)
- Edit config.json to set terminal_path (full path to terminal64.exe) if you have multiple MT5 installs.
- auto_resolve_symbol will try to map XAUUSD -> XAUUSD# / GOLD etc if needed.

Troubleshooting MT5 connection
- If MT5 initialize fails, keep one MT5 terminal open and logged in, then set `terminal_path` in `config.json` to the exact `terminal64.exe`.
- If you use account credentials from config/CLI, set `mt5_login`, `mt5_password`, and `mt5_server` correctly. Wrong values trigger an authorization failure.
- IPC timeout errors are retried automatically. If you still fail, close duplicate terminals and relaunch MT5 as the same OS user as the bot.
- For missing prices / market closed, open Market Watch -> Show All and open a chart for the symbol to force quote subscription.
- Run `python tools/mt5_smoketest.py --symbol XAUUSD` to print terminal path in use, account/server, and tick availability.
