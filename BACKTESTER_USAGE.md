# Main Backtester Usage

The main `backtester` binary runs full strategy backtests with flexible filtering and verbose logging.

## Command Line Options

```bash
./target/debug/backtester --help
```

### Arguments
- `-u, --universe <UNIVERSE>` - Universe to backtest: 'Crypto', 'SC', 'MC', 'LC', 'Micro', 'Stocks' (default: Crypto)
- `-m, --mode <MODE>` - Execution mode: 'production', 'testing', or 'demo' (default: testing)
- `-t, --tickers <TICKERS>` - Filter by specific ticker(s), comma-separated (case-insensitive - auto-converts to lowercase for crypto, uppercase for stocks)
- `-s, --strategy <STRATEGY>` - Filter by specific strategy (optional)
- `-p, --path <PATH>` - Working directory path (optional, uses CLICKHOUSE_USER_PATH or /srv/rust_home/backtester)
- `-v, --verbose` - Enable verbose logging (-v info, -vv debug, -vvv trace)

## Usage Examples

### Basic run (crypto, testing mode)
```bash
./target/debug/backtester
# Same as: ./target/debug/backtester -u Crypto -m testing
```

### Run with info logging
```bash
./target/debug/backtester -v
```

### Test specific ticker(s)
```bash
./target/debug/backtester -u Crypto -t BTC,ETH -v
# Crypto: case-insensitive, auto-converts to lowercase (BTC → btc)

./target/debug/backtester -u LC2 -t ibm -s hammer -v
# Stocks: case-insensitive, auto-converts to uppercase (ibm → IBM)
```

### Run stocks universe
```bash
./target/debug/backtester -u Stocks -m testing -v
```

### Run specific universe (small cap)
```bash
./target/debug/backtester -u SC -m testing -v
```

### Demo mode (uses existing data, no downloads)
```bash
./target/debug/backtester -u Crypto -m demo -v
```

### Production mode (writes to production folders)
```bash
./target/debug/backtester -u Crypto -m production -v
```

### Test single ticker with debug logging
```bash
./target/debug/backtester -u Crypto -t btc -vv
```

### Multiple tickers
```bash
./target/debug/backtester -u Crypto -t BTC,ETH,SOL -v
```

### Test specific strategy only
```bash
./target/debug/backtester -u Crypto -s tf12_vama -v
```

### Test single ticker with single strategy
```bash
./target/debug/backtester -u Crypto -t BTC -s tf12_vama -vv
```

## Modes

### testing (default)
- Runs backtests in testing mode
- Deletes prior testing output files
- Downloads fresh price data
- Generates decisions and performance files in testing folders

### production
- Runs backtests in production mode
- Deletes prior production output files
- Downloads fresh price data
- Writes to production folders
- Inserts scores into ClickHouse database

### demo
- Uses existing data files without downloading
- Useful for quick testing with cached data
- Does not delete any files

## Universes

- **Crypto** - All crypto assets
- **SC** - Small cap stocks (SC1, SC2, SC3, SC4)
- **MC** - Mid cap stocks (MC1, MC2)
- **MC1** - Mid cap 1 only
- **LC** - Large cap stocks (LC1, LC2)
- **Micro** - Micro cap stocks (Micro1, Micro2, Micro3, Micro4)
- **Stocks** - All stock universes combined

## Output

Results are written to:
- `output/testing/` or `output/production/` for stocks
- `output_crypto/testing/` or `output_crypto/production/` for crypto
- `decisions/crypto/` or `decisions/stocks/` for decision files

Summary performance files:
- `summary_performance.csv` in the appropriate output folder

## Logging Levels

### No verbosity (warnings only)
Only shows warnings and errors

### `-v` (info level)
Shows:
- Mode and universe information
- File deletion operations
- Backtest start/completion messages
- Price file creation progress

### `-vv` (debug level)
Shows all info-level logs plus:
- Custom ticker lists
- Detailed processing information

### `-vvv` (trace level)
Shows all debug-level logs plus internal library traces

## Strategy Names

Common strategies you can filter by include:
- `tf12_vama` - Trend following with VAMA
- `tf9_tii` - Trend following with TII
- `hammer` - Hammer candlestick pattern
- `three_candles` - Three candles pattern
- `pattern_marubozu` - Marubozu pattern
- `donchian_indicator` - Donchian channel indicator
- `heikin_ashi` - Heikin Ashi candles
- `macd_change` - MACD change signal
- And many more...

Use `-s <strategy_name>` to test only that strategy. The strategy name must match exactly (case-sensitive).

## Notes

- Strategy filtering is now fully implemented! Use `-s <strategy_name>` to test a single strategy
- Ticker names are case-insensitive:
  - **Crypto**: Automatically converted to lowercase (BTC → btc)
  - **Stocks**: Automatically converted to uppercase (ibm → IBM)
- The backtester will automatically test local (192.168.86.246) then remote (192.168.86.56) ClickHouse connections
- Use demo mode for quick testing without re-downloading price data
- When filtering by strategy, the strategy name must match exactly (case-sensitive)
