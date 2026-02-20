#!/bin/bash
# ./run_portfolio_backtest.sh > run_portfolio.log 2>&1

output="portfolio_results/results_20250216_3.csv"
signal_date="20260208"
lookback_days=1
start_date="2000-01-02"

printf "universe,sector,cagr,total_return_pct,initial_value,final_value,realized_pnl,unrealized_pnl,commissions,total_trades,winning_trades,win_rate_pct,avg_win_pct,avg_loss_pct,profit_factor,max_drawdown_pct,sharpe_ratio,avg_holding_days,portfolio_size,stop_loss_pct,lookback_days,signal_date,start_date,priority_strategy,signals\n" > "$output"

# cargo build --release

# Define all universes
universes=("LC" "MC" "SC")
# universes=("SC")

# Define all sectors
sectors=(
  "ALL" "Technology" "Consumer Cyclical" "Healthcare" "Energy"
  "Communication Services" "Financial Services" "Real Estate"
  "Consumer Defensive" "Utilities" "Basic Materials"
)
# sectors=("Energy")

# Define all signals
# signals=(
#   "donchian_indicator" "slingshot" "pattern_marubozu" "trend_fol_bottle_stoch"
#   "adx_indicator" "stick_sandwich" "trend_fol_2trouble_rsi" "pattern_three_line_strike"
#   "tasuki" "pattern_engulfing" "elder_impulse_2" "hikkake" "tf14_catapult"
#   "candlestick_double_trouble_2.0" "heikin_ashi_double_trouble" "candlestick_double_trouble"
#   "harami_strict" "tf6_supertrend_flip" "contrarian_real_range_extremes" "bottle"
#   "harami_flexible" "contrarian_stochastic_extremes" "contrarian_stochastic_duration"
#   "tower" "trend_fol_3candle_ma" "donchian_indicator_inverse" "pattern_td_waldo_6"
#   "tf3_rsi_ma" "euphoria" "heikin_ashi_euphoria" "tf1_ma" "contrarian_disparity_extremes"
#   "mirror" "contrarian_rsi_divergences" "pattern_hammer" "pattern_td_waldo_5"
#   "contrarian_bbands" "contrarian_piercing_stoch" "hammer" "pattern_td_waldo_8"
#   "three_candles" "blockade" "contrarian_demarker_extremes" "contrarian_fisher_extremes"
#   "pattern_differentials" "contrarian_aug_bbands" "trend_fol_h_trend_intensity" "barrier"
#   "tf9_tii" "contrarian_dual_bbands" "pattern_td_open" "spinning_top" "doppleganger"
#   "contrarian_countdown_duration" "tf10_ma" "pattern_td_waldo_2" "tweezers"
# )
signals=(
  "contrarian_stochastic_extremes,tf6_supertrend_flip,contrarian_stochastic_duration,pattern_marubozu,pattern_three_line_strike,pattern_engulfing"
)

priority_signal="pattern_marubozu"

for univ in "${universes[@]}"; do
  for sector in "${sectors[@]}"; do
    for signal in "${signals[@]}"; do
      ./target/release/portfolio_backtest --signal-date "$signal_date" \
      --start-date "$start_date" \
      --universe "$univ" \
      --sector "$sector" \
      --signals "$signal" \
      --priority-strategy "$priority_signal" \
      --verbose 0 \
      --lookback-days $lookback_days \
      --oneline >> "$output"
    done
  done
done
