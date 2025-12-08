# Portfolio Backtest Tool

A tool for combining signals from multiple trading strategies and running portfolio-level backtests.

## Overview

The `portfolio_backtest` binary reads individual decision files (buy/sell signals) from multiple strategies and combines them using various methods to create a portfolio trading strategy. This allows you to test ensemble approaches and strategy combinations.

## Usage

```bash
./target/debug/portfolio_backtest [OPTIONS]
```

### Options

- `-i, --input <FOLDER>`: Input folder containing decision files (default: `output/testing`)
- `-c, --combine <METHOD>`: Strategy combination method (default: `majority`)
  - `average`: Average all signals (buy=+1, sell=-1) and scale position size
  - `majority`: Act only when majority of strategies agree
  - `unanimous`: Act only when ALL strategies agree
- `-n, --min-agree <NUM>`: Minimum number of strategies that must agree (overrides majority threshold)
- `-t, --tickers <TICKERS>`: Filter by specific tickers (comma-separated, optional)
- `-o, --output <FILE>`: Output CSV file for results (default: `portfolio_results.csv`)
- `-v, --verbose`: Enable verbose logging (-v for info, -vv for debug)

## Workflow

### Step 1: Generate Decision Files

First, run backtests to generate decision files. In testing mode, these are automatically saved to `output/testing/` (stocks) or `output_crypto/testing/` (crypto):

```bash
# Run multiple strategies for crypto
./target/debug/backtester -u Crypto -t btc,eth,sol -v

# Run multiple strategies for stocks
./target/debug/backtester -u LC -t AAPL,MSFT,GOOGL -v

# Run specific strategy only
./target/debug/backtester -u Crypto -t btc,eth -s tf12_vama -v
```

This creates decision files with naming pattern: `{ticker}_{strategy}_decisions.csv`

### Step 2: Run Portfolio Backtest

Combine the signals and test the portfolio:

```bash
# Test crypto portfolio with majority voting
./target/debug/portfolio_backtest -i output_crypto/testing -v

# Test specific tickers only
./target/debug/portfolio_backtest -i output_crypto/testing -t btc,eth -v

# Use average combination method
./target/debug/portfolio_backtest -i output_crypto/testing -c average -v

# Require unanimous agreement
./target/debug/portfolio_backtest -i output_crypto/testing -c unanimous -v

# Require at least 3 strategies to agree
./target/debug/portfolio_backtest -i output/testing -n 3 -v
```

## Combination Methods

### Average

Calculates the average signal strength across all strategies:
- Position = (buy_signals - sell_signals) / total_strategies
- Allows fractional positions (-1.0 to +1.0)
- More nuanced, takes all opinions into account

**Example**: 3 strategies, 2 buy + 1 sell = position of 0.33 (1/3 long)

### Majority (Default)

Acts only when majority of strategies agree:
- Position = 1.0 if majority buy, -1.0 if majority sell, 0.0 otherwise
- Binary positions (all-in or all-out)
- Filters noise by requiring consensus

**Example**: 5 strategies, need 3+ agreeing to take position

### Unanimous

Acts only when ALL strategies agree:
- Position = 1.0 only if all buy, -1.0 only if all sell
- Highest conviction trades only
- Very conservative approach

**Example**: 4 strategies, only trade when all 4 agree

## Output

### Console Output

```
=== Portfolio Performance Metrics ===
Total Trades: 181
Winning Trades: 65
Win Rate: 35.91%
Total Return: 411.09%
Average Return per Trade: 2.27%
Average Win: 18.23%
Average Loss: -6.67%
Profit Factor: 1.531
```

### CSV Output

The results CSV contains one row per trade:

- `ticker`: Ticker symbol
- `entry_date`: Trade entry date
- `entry_price`: Entry price
- `exit_date`: Trade exit date (None if still open)
- `exit_price`: Exit price (None if still open)
- `size`: Position size (1.0 = full long, -1.0 = full short, 0.5 = half long, etc.)
- `return_pct`: Trade return percentage

## Examples

### Example 1: Ensemble Crypto Strategy

```bash
# Generate signals from multiple strategies
./target/debug/backtester -u Crypto -t btc,eth -v

# Combine using majority vote
./target/debug/portfolio_backtest -i output_crypto/testing -c majority -v
```

### Example 2: Conservative Stock Portfolio

```bash
# Generate signals for multiple stocks
./target/debug/backtester -u LC -t IBM,AAPL,MSFT -v

# Only trade when ALL strategies agree
./target/debug/portfolio_backtest -i output/testing -c unanimous -v
```

### Example 3: Weighted Ensemble

```bash
# Generate signals
./target/debug/backtester -u Crypto -t btc -v

# Use average to get fractional positions based on signal strength
./target/debug/portfolio_backtest -i output_crypto/testing -c average -v
```

## Notes

- Decision files must be in the format: `{ticker}_{strategy}_decisions.csv`
- The tool automatically detects universe (Crypto vs stocks) from input folder path
- Price data is read from the appropriate CSV file in `data/testing/`
- Multiple decision files for the same ticker are combined into portfolio decisions
- The tool handles cases where strategies have different numbers of signals
- Positions are closed when opposing signals are generated or at the end of the data

## Advanced Usage

### Testing Different Thresholds

You can experiment with different agreement thresholds:

```bash
# Need at least 2 strategies to agree (out of any number)
./target/debug/portfolio_backtest -i output/testing -n 2 -v

# Need at least 4 strategies (even if you have 10)
./target/debug/portfolio_backtest -i output/testing -n 4 -v
```

### Filtering Specific Tickers

If you have decision files for many tickers but want to test only a subset:

```bash
# Only test BTC and ETH from a folder with many tickers
./target/debug/portfolio_backtest -i output_crypto/testing -t btc,eth -v
```

## Integration with Main Backtester

The portfolio backtester works seamlessly with the main backtester's testing mode output:

1. Main backtester (`--mode testing`) generates individual decision files
2. Portfolio backtester reads these files and combines signals
3. Results are independent: you can run portfolio backtest multiple times with different parameters without re-running the main backtester

This separation allows you to:
- Generate signals once
- Test multiple portfolio combination strategies quickly
- Compare ensemble approaches without re-computing indicators
