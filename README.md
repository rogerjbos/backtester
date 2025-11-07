# Backtester

A high-performance Rust-based backtesting system for trading strategies across multiple asset classes including stocks and cryptocurrencies.

## Overview

This backtesting framework evaluates trading strategies against historical price data using a comprehensive set of technical indicators and pattern recognition algorithms. The system supports parallel processing, multiple universes, and both production and testing modes.

## Features

- **Multi-Asset Support**: Stocks and cryptocurrencies
- **Parallel Processing**: Concurrent backtesting across multiple tickers
- **Comprehensive Indicators**: 100+ technical indicators and candlestick patterns
- **Multiple Universes**: SC (Small Cap), MC (Mid Cap), LC (Large Cap), Micro, Crypto
- **Flexible Modes**: Production, Testing, and Demo modes
- **Performance Analytics**: Detailed metrics including Sharpe ratio, drawdown, win rate
- **Database Integration**: ClickHouse for data storage and retrieval

## Quick Start

### Prerequisites

- Rust 1.70+
- ClickHouse database (for production/testing modes)
- CSV files (for demo mode)

### Installation

```bash
git clone <repository-url>
cd backtester
cargo build --release
```

### Basic Usage

```bash
# Demo mode (uses local CSV files - no database required)
cargo run MC1 demo

# Testing mode (downloads from database)
cargo run MC1 testing

# Production mode (full production run)
cargo run MC1 production

# Custom tickers
cargo run Crypto testing btc,eth,sol
```

## Command Line Arguments

```
cargo run [UNIVERSE] [MODE] [TICKERS] [PATH]

UNIVERSE:
  MC1, MC2          - Mid Cap stocks
  SC1, SC2, SC3, SC4 - Small Cap stocks
  LC1, LC2          - Large Cap stocks
  Micro1-4          - Micro Cap stocks
  Crypto            - Cryptocurrencies
  Stocks            - All stock universes
  MC, SC, LC        - Multiple universes

MODE:
  demo      - Use local CSV files (no database)
  testing   - Download and test (database required)
  production - Full production run (database required)

TICKERS: (optional)
  Comma-separated list of specific tickers to test

PATH: (optional)
  Custom path to data directory
```

## Universes

| Universe | Description | Example Tickers |
|----------|-------------|-----------------|
| MC1 | Mid Cap 1 | AAPL, MSFT, GOOGL |
| MC2 | Mid Cap 2 | NVDA, TSLA, AMZN |
| SC1-4 | Small Cap | Various small cap stocks |
| LC1-2 | Large Cap | Major blue-chip companies |
| Micro1-4 | Micro Cap | Smallest public companies |
| Crypto | Cryptocurrencies | BTC, ETH, SOL, DOT |

## Output Structure

```
output/
├── production/     # Production backtest results
├── testing/        # Testing backtest results
└── crypto/         # Crypto-specific results

performance/
├── stocks/         # Stock performance summaries
└── crypto/         # Crypto performance summaries

score/              # Scoring results for ranking strategies
decisions/          # Individual trade decisions
```

## Architecture

### Core Components

- **Signals**: Technical indicators and pattern recognition
- **Backtesting Engine**: Core simulation logic
- **Data Pipeline**: Price data loading and preprocessing
- **Performance Analytics**: Risk and return calculations
- **Database Layer**: ClickHouse integration

### Key Metrics

- **Profit Factor**: Gross profit / Gross loss
- **Sharpe Ratio**: Risk-adjusted returns
- **Maximum Drawdown**: Peak-to-trough decline
- **Win Rate**: Percentage of profitable trades
- **Calmar Ratio**: Annual return / Maximum drawdown

## Development

### Adding New Signals

1. Create signal function in `src/signals/`
2. Add to appropriate module (`sample.rs`, etc.)
3. Register in `select_backtests()` function
4. Update signal parameters as needed

### Signal Function Signature

```rust
pub fn my_signal(df: DataFrame, param: f64) -> BuySell {
    // Implementation
    BuySell {
        buy: vec![/* buy signals */],
        sell: vec![/* sell signals */],
    }
}
```

### Testing

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_perf

# Run with output
cargo test -- --nocapture
```

## Configuration

### Environment Variables

- `CLICKHOUSE_USER_PATH`: Base path for ClickHouse data (default: `/srv`)

### Data Format

CSV files should contain OHLCV data:
```csv
Date,Ticker,Universe,Open,High,Low,Close,Volume
2024-01-01,AAPL,MC1,150.00,155.00,149.00,154.00,1000000
```

## Performance

- **Parallel Processing**: 10 tickers processed concurrently by default
- **Memory Efficient**: Lazy loading with Polars DataFrames
- **Fast Execution**: Optimized Rust performance
- **Scalable**: Handles large datasets efficiently

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

[License information]

## Support

For questions or issues:
- Check the existing tests and documentation
- Review the signal implementations for examples
- Examine the backtesting logic in `lib.rs`

---

**Note**: This system is designed for research and educational purposes. Always validate strategies with out-of-sample testing before live trading.</content>
<parameter name="filePath">/Users/rogerbos/rust_home/backtester/README.md