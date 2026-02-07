# Portfolio Backtester Usage Guide

## Overview

The portfolio backtester builds and manages a portfolio based on buy/sell signals from individual strategy decision files. It implements:

- **Position limits**: Configurable maximum number of holdings
- **Priority-based ranking**: Selects top stocks based on a priority strategy's signal count
- **Daily rebalancing**: Checks for new opportunities every trading day
- **Stop-loss protection**: Automatic exit when price drops below threshold
- **Entry/exit timing**: Buys day after buy signal, sells day after sell signal
- **Cash management**: Holds cash when insufficient buy signals available

## Command Line Arguments

### Required Arguments

```bash
--input <FOLDER>              # Path to folder with decision files
--priority-strategy <NAME>    # Strategy name for ranking stocks
--universe <NAME>             # MC1, MC2, SC1, LC1, or Crypto
```

### Optional Arguments

```bash
--portfolio-size <N>          # Max positions (default: 20)
--stop-loss-pct <DECIMAL>     # Stop-loss as decimal (0.10 = 10%, default: 0.0)
--output <FILE>               # Trade results file (default: portfolio_trades.csv)
--daily-output <FILE>         # Daily portfolio values (default: portfolio_daily.csv)
--tickers <LIST>              # Comma-separated ticker filter (optional)
-v, -vv, -vvv                 # Verbosity level
```

## Usage Examples

### Basic Usage

```bash
./target/release/portfolio_backtest \
  --input output/testing_20260130 \
  --priority-strategy candlestick_double_trouble \
  --universe MC1
```

### With Stop-Loss

```bash
./target/release/portfolio_backtest \
  --input output/testing_20260130 \
  --portfolio-size 20 \
  --priority-strategy candlestick_double_trouble \
  --universe MC1 \
  --stop-loss-pct 0.10 \
  -vv
```

### Multiple Universes

Run separately for each universe (stocks and crypto never mixed):

```bash
# Mid-cap stocks
./target/release/portfolio_backtest \
  --input output/testing_20260130 \
  --priority-strategy candlestick_double_trouble \
  --universe MC1 \
  --output portfolio_MC1_trades.csv

# Crypto
./target/release/portfolio_backtest \
  --input output_crypto/testing_20260130 \
  --priority-strategy candlestick_double_trouble \
  --universe Crypto \
  --output portfolio_crypto_trades.csv
```

### Filter Specific Tickers

```bash
./target/release/portfolio_backtest \
  --input output/testing_20260130 \
  --priority-strategy candlestick_double_trouble \
  --universe MC1 \
  --tickers AAPL,MSFT,GOOGL
```

## How It Works

### 1. Signal Loading

Reads all decision files from input folder:
- Format: `TICKER_strategy_name_decisions.csv`
- Columns: ticker, strategy, date, action
- Actions: "buy" or "sell"

### 2. Stock Ranking

When multiple buy signals compete for limited slots:
1. Count signals from priority strategy for each ticker
2. Rank tickers by signal count (descending)
3. Fill available slots with top-ranked stocks

Example: If 50 stocks have buy signals but only 5 slots available, the 5 stocks with the most priority strategy signals get selected.

### 3. Daily Portfolio Management

For each trading date:

1. **Check stop-losses**: Close positions where current price ≤ stop-loss price
2. **Process sell signals**: Close positions when ANY strategy gives sell signal (exit day after signal)
3. **Process buy signals**:
   - Rank candidates by priority strategy
   - Fill open slots with top candidates
   - Equal weight allocation: `portfolio_value / portfolio_size`
   - Entry day after buy signal

### 4. Position Sizing

- **Equal weighting**: Each position targets 1/N of portfolio value
- **Share calculation**: `shares = floor(position_cash / entry_price)`
- **Cash holdings**: Remainder stays in cash earning 0%

### 5. Risk Management

- **Stop-loss**: Set at `entry_price * (1 - stop_loss_pct)`
- **No shorting**: Sell signals only apply to existing positions
- **Position limits**: Never exceed `portfolio_size` holdings

## Output Files

### Trades File (portfolio_trades.csv)

Columns:
- `ticker`: Stock symbol
- `entry_date`: Date position opened
- `entry_price`: Entry price
- `exit_date`: Date position closed
- `exit_price`: Exit price
- `shares`: Number of shares
- `return_pct`: Return percentage
- `exit_reason`: "signal", "stop_loss", or "final"

### Daily Values File (portfolio_daily.csv)

Columns:
- `date`: Trading date
- `portfolio_value`: Total portfolio value
- `cash`: Cash balance
- `equity_value`: Value of all positions
- `position_count`: Number of open positions

## Performance Metrics

The backtester calculates and displays:

### Trade Statistics
- Total trades, win rate, avg win/loss
- Profit factor
- Exit reason breakdown (signal vs stop-loss)

### Portfolio Performance
- Total return
- Max drawdown
- Sharpe ratio (annualized)
- Daily return statistics

### Holding Statistics
- Average/min/max holding periods
- Average position count
- Cash vs equity exposure

## Strategy Selection

### Choosing Priority Strategy

The priority strategy determines which stocks get selected when slots are limited. Consider:

1. **Best performing strategy**: Use the strategy with highest returns
2. **Most reliable strategy**: Use the strategy with highest win rate
3. **Momentum strategy**: Use a strategy that captures recent price action
4. **Combination approach**: Create a custom ranking combining multiple factors

### Available Strategies

Any strategy from your backtester output can be used. Examples:
- `candlestick_double_trouble`
- `macd_crossover`
- `rsi_oversold`
- `bollinger_squeeze`
- etc.

## Tips and Best Practices

### 1. Portfolio Size

- **10-15 positions**: More concentrated, higher volatility
- **20-30 positions**: Balanced diversification
- **50+ positions**: Lower risk, closer to market performance

### 2. Stop-Loss Percentage

- **5-7%**: Tight stop, limits losses but may trigger frequently
- **10-15%**: Moderate stop, good balance
- **20%+**: Loose stop, gives positions room to recover

### 3. Universe Selection

- Keep stocks and crypto separate (different volatility profiles)
- Match universe to data availability
- Consider market cap categories:
  - MC1/MC2: Mid-caps (more volatile, higher growth potential)
  - SC1: Small-caps (highest risk/reward)
  - LC1: Large-caps (lower risk, stable)

### 4. Performance Analysis

Compare results across:
- Different priority strategies
- Different portfolio sizes
- Different stop-loss levels
- Different universes

## Troubleshooting

### No Trades Generated

- Check that input folder contains decision files
- Verify universe matches data files in `data/testing/`
- Try lowering portfolio size
- Remove ticker filter if applied

### Poor Performance

- Try different priority strategies
- Adjust stop-loss percentage
- Increase/decrease portfolio size
- Review individual trades for patterns

### Price Data Missing

- Ensure universe parameter matches available data files
- Check data files have consistent ticker names
- Verify date ranges overlap with decision files

## Integration with Main Backtester

1. Build the portfolio backtester:
   ```bash
   cargo build --bin portfolio_backtest --release
   ```

2. Use output folder for portfolio backtest:
   ```bash
   cargo build --bin portfolio_backtest --release
   ./target/release/portfolio_backtest \
    --signal-date 20260204 \
    --start-date 2024-01-02 \
    --portfolio-size 20 \
    --commission 0 \
    --signals  donchian_indicator \
    --priority-strategy donchian_indicator \
    --stop-loss-pct 0.1 \
    --universe MC1 \
    --prefix mytest \
    --verbose 1 \
    --accounting-reports \
    --rebalance \
    --rebalance-threshold 0.10



   cargo build --bin portfolio_backtest --release
   ./target/release/portfolio_backtest \
    --universe LC \
    --sector Energy \
    --signals donchian_indicator \
    --priority-strategy donchian_indicator \
    --lookback-days 5


   ```

3. Compare individual strategy performance with portfolio performance

## Future Enhancements

Potential additions:
- Multiple priority strategies with weighted ranking
- Dynamic position sizing based on signal strength
- Rebalancing frequency options (weekly/monthly)
- Benchmark comparison (e.g., S&P 500)
- Transaction cost modeling
- Slippage simulation
- Risk parity weighting
- Correlation-based diversification
