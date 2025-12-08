# Backtester Decisions Usage

The `backtester_decisions` binary allows you to analyze trading decisions with flexible filtering and verbose logging.

## Command Line Options

```bash
./target/debug/backtester_decisions --help
```

### Required Arguments
- `-a, --asset-type <ASSET_TYPE>` - Asset type: 'stocks' or 'crypto'

### Optional Filters
- `-t, --ticker <TICKER>` - Filter by specific ticker (case-insensitive - auto-converts to lowercase for crypto, uppercase for stocks)
- `-s, --strategy <STRATEGY>` - Filter by specific strategy  
- `-u, --universe <UNIVERSE>` - Not applicable for decisions analysis (use --asset-type instead)

### Logging
- `-v` - Info level logging (shows progress and key operations)
- `-vv` - Debug level logging (shows detailed data for each ticker/strategy)
- `-vvv` - Trace level logging (shows all internal operations)

## Usage Examples

### Basic run (crypto, minimal output)
```bash
./target/debug/backtester_decisions --asset-type crypto
```

### Run with info logging
```bash
./target/debug/backtester_decisions --asset-type crypto -v
```

### Test specific ticker with verbose logging
```bash
./target/debug/backtester_decisions \
  --asset-type crypto \
  --ticker BTC \
  --verbose --verbose
```

### Test specific strategy with debug logging
```bash
./target/debug/backtester_decisions \
  -a crypto \
  -s tf12_vama \
  -vv
```

### Test specific ticker + strategy combination
```bash
./target/debug/backtester_decisions \
  --asset-type crypto \
  --ticker ETH \
  --strategy tf12_vama \
  -vv
```

### Analyze all tickers for a specific strategy
```bash
./target/debug/backtester_decisions \
  --asset-type crypto \
  --strategy tf12_vama \
  -v
```

### Filter by ticker with debug logging (case-insensitive matching)
```bash
# Crypto - converts to lowercase
./target/debug/backtester_decisions \
  -a crypto \
  -t BTC \
  -vv

# Stocks - converts to uppercase  
./target/debug/backtester_decisions \
  -a stocks \
  -t ibm \
  -vv
```

## Output

Results are written to:
- `performance_results_crypto.csv` for crypto
- `performance_results_stocks.csv` for stocks

The CSV contains:
- ticker
- strategy
- st_cum_return (short-term cumulative return)
- st_accuracy (short-term accuracy)
- mt_cum_return (medium-term cumulative return)
- mt_accuracy (medium-term accuracy)
- lt_cum_return (long-term cumulative return)
- lt_accuracy (long-term accuracy)
- bh_cum_return (buy-and-hold cumulative return)
- bh_accuracy (buy-and-hold accuracy)
- buy_and_hold_return (overall buy-and-hold return)

## Logging Levels

### No verbosity (warnings only)
Only shows warnings and errors

### `-v` (info level)
Shows:
- Connection testing
- File loading progress
- Number of tickers/strategies found
- Processing progress (every 100 combinations)
- Final results count

### `-vv` (debug level)
Shows all info-level logs plus:
- List of all tickers and strategies
- Processing details for each ticker/strategy combo
- Statistics calculated for each combination
- Data shape information

### `-vvv` (trace level)
Shows all debug-level logs plus internal library traces
