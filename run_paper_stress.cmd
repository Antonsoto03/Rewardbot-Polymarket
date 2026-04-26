@echo off
setlocal

.\.venv\Scripts\python.exe liquidity_rewards_bot.py ^
  --mode paper ^
  --ranking-file ranking_snapshot_live.json ^
  --cycles 300 ^
  --refresh-seconds 0.10 ^
  --top-n 15 ^
  --active-markets 4 ^
  --rotation-step 2 ^
  --market-hold-cycles 4 ^
  --distance-to-mid-bps 6 ^
  --min-distance-to-mid-bps 2 ^
  --quote-size 180 ^
  --max-position-per-market 700 ^
  --global-exposure-limit 7000 ^
  --inventory-mode dynamic ^
  --inventory-unwind-threshold 1 ^
  --inventory-unwind-size 14 ^
  --no-fill-tighten-cycles 10 ^
  --tighten-step-bps 4 ^
  --fill-base-prob 0.08 ^
  --fill-sensitivity 0.35 ^
  --fill-max-prob 0.85 ^
  --simulated-taker-fee-pct 1.0 ^
  --max-fills-per-minute 220 ^
  --max-cancels-per-minute 80 ^
  --max-drawdown-pct 18 ^
  --daily-loss-limit-usd 120 ^
  --disable-state-persistence ^
  --random-seed 42 ^
  --log-level INFO ^
  %*

endlocal
