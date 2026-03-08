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
4) (Optioneel) vul MT5 login/password/server in als je een specifiek account wilt forceren.
5) Klik Start.

Logs:
- flexbot.log (in dezelfde map)

Let op:
- Dit is v1: mechanische edge (MA200 trend + MA50 pullback + RSI + trigger)
- Later kan de Signal Engine vervangen worden door AI zonder execution te breken.


CONFIG (optional)
- Edit config.json to set terminal_path (full path to terminal64.exe) if you have multiple MT5 installs.
- Optional auth keys: `mt5_login`, `mt5_password`, `mt5_server` (also available in GUI).
- auto_resolve_symbol will try to map XAUUSD -> XAUUSD# / GOLD etc if needed.

Troubleshooting MT5 connection
- On start the bot validates both `mt5.terminal_info()` and `mt5.account_info()`; if either is missing it fails fast (no worker threads are started) and closes the MT5 session cleanly.
- If MT5 initialize fails, keep one MT5 terminal open and logged in, then set `terminal_path` in `config.json` to the exact `terminal64.exe`.
- If you use account credentials from config/GUI/CLI, set `mt5_login`, `mt5_password`, and `mt5_server` correctly. Wrong values trigger an authorization failure.
- IPC timeout errors are retried automatically. If you still fail, close duplicate terminals and relaunch MT5 as the same OS user as the bot.
- Market-closed / no-tick warnings are throttled to reduce log spam.
- Run `python tools/mt5_smoketest.py --symbol XAUUSD` to print terminal/account/server details and tick availability for the selected symbol.

Paper test checklist
- Open de juiste broker MT5 terminal.
- Log eerst in op het juiste account.
- Houd MT5 open tijdens de hele validatie.
- Run de smoketest: `python tools/mt5_smoketest.py --symbol XAUUSD`.
- Houd `paper_mode=true` voor de eerste validatie.
- Zet `paper_mode` pas uit na stabiele paper testing.

