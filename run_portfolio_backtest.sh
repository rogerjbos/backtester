#!/bin/bash
# ./run_portfolio_backtest.sh > run.log 2>&1

echo "universe,sector,priority_strategy,signals,portfolio_size,stop_loss_pct,lookback_days,initial_value,final_value,total_return_pct,realized_pnl,unrealized_pnl,commissions,total_trades,winning_trades,win_rate_pct,avg_win_pct,avg_loss_pct,profit_factor,max_drawdown_pct,sharpe_ratio,avg_holding_days" > results.csv

# cargo build --release
for univ in LC MC SC Micro; do
  for sector in ALL Technology "Consumer Cyclical"  Healthcare Energy "Communication Services" "Financial Services" "Real Estate" "Consumer Defensive" Utilities "Basic Materials"; do
    ./target/release/portfolio_backtest --signal-date 20260204 \
    --universe "${univ}1" \
    --sector "$sector" \
    --signals donchian_indicator \
    --priority-strategy donchian_indicator \
    --verbose 0 \
    --oneline >> results_donchian_indicator.csv
  done
done