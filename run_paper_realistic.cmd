@echo off
setlocal

.\.venv\Scripts\python.exe liquidity_rewards_bot.py ^
  --mode paper ^
  --ranking-file ranking_snapshot_live.json ^
  --cycles 180 ^
  --refresh-seconds 0.20 ^
  --top-n 8 ^
  --active-markets 2 ^
  --rotation-step 1 ^
  --market-hold-cycles 8 ^
  --distance-to-mid-bps 10 ^
  --min-distance-to-mid-bps 4 ^
  --quote-size 80 ^
  --max-position-per-market 350 ^
  --global-exposure-limit 1800 ^
  --inventory-mode dynamic ^
  --inventory-unwind-threshold 2 ^
  --inventory-unwind-size 8 ^
  --fill-base-prob 0.03 ^
  --fill-sensitivity 0.20 ^
  --fill-max-prob 0.40 ^
  --simulated-taker-fee-pct 1.0 ^
  --max-drawdown-pct 12 ^
  --daily-loss-limit-usd 40 ^
  --disable-state-persistence ^
  --random-seed 42 ^
  --log-level INFO ^
  %*

endlocal
