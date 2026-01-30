# Backtester Program - Comprehensive Technical Documentation

## Overview

The Backtester is a high-performance Rust-based trading strategy evaluation system designed to test and analyze quantitative trading strategies across multiple asset classes including stocks and cryptocurrencies. The system evaluates over 100+ technical indicators and candlestick patterns against historical price data to identify profitable trading strategies.

## Architecture

### Core Components

1. **Signal Engine**: Collection of 100+ trading strategies and technical indicators
2. **Backtesting Engine**: Core simulation logic that executes trades based on signals
3. **Data Pipeline**: Price data loading, preprocessing, and storage
4. **Performance Analytics**: Risk and return calculations with comprehensive metrics
5. **Database Layer**: ClickHouse integration for data persistence and retrieval
6. **Parallel Processing**: Concurrent execution across multiple tickers and strategies

### Key Data Structures

#### Backtest Result
```rust
pub struct Backtest {
    pub ticker: String,           // Asset symbol (e.g., "AAPL", "btc")
    pub universe: String,         // Market segment (e.g., "MC1", "Crypto")
    pub strategy: String,         // Strategy name (e.g., "hammer", "tf12_vama")
    pub expectancy: f64,          // Expected value per trade
    pub profit_factor: f64,       // Gross profit / Gross loss ratio
    pub hit_ratio: f64,           // Win rate percentage
    pub realized_risk_reward: f64,// Average risk-reward ratio
    pub avg_gain: f64,            // Average winning trade
    pub avg_loss: f64,            // Average losing trade
    pub max_gain: f64,            // Largest winning trade
    pub max_loss: f64,            // Largest losing trade
    pub sharpe_ratio: f64,        // Risk-adjusted returns
    pub sortino_ratio: f64,       // Downside risk-adjusted returns
    pub max_drawdown: f64,        // Peak-to-trough decline
    pub calmar_ratio: f64,        // Annual return / Max drawdown
    pub win_loss_ratio: f64,      // Winners vs losers ratio
    pub recovery_factor: f64,     // Net profit / Max drawdown
    pub profit_per_trade: f64,    // Average profit per trade
    pub buys: i32,                // Number of buy signals
    pub sells: i32,               // Number of sell signals
    pub trades: i32,              // Total number of trades
    pub date: String,             // Date of backtest
    pub buy: i32,                 // Current buy position
    pub sell: i32,                // Current sell position
}
```

#### Trading Decision
```rust
pub struct Decision {
    pub date: String,    // Date of decision
    pub action: String,  // "BUY" or "SELL"
}
```

#### Signal Output
```rust
pub struct BuySell {
    pub buy: Vec<i32>,   // Indices where buy signals occur
    pub sell: Vec<i32>,  // Indices where sell signals occur
}
```

## Trading Strategies and Signals

The system implements multiple categories of trading strategies:

### 1. Candlestick Pattern Recognition (MFPR - Mastering Financial Pattern Recognition)

- **hammer**: Single candlestick reversal pattern
- **three_candles**: Three-candle pattern recognition
- **tweezers**: Top/bottom reversal pattern
- **hikkake**: Inside bar breakout pattern
- **tower**: Multi-candle tower formation
- **slingshot**: Reversal pattern with momentum
- **double_trouble**: Double candlestick pattern
- **engulfing**: Bullish/bearish engulfing patterns
- **doji**: Doji candlestick variations
- **marubozu**: Full body candlesticks
- **piercing**: Piercing pattern
- **tasuki**: Tasuki gap patterns
- **stick_sandwich**: Three-candle sandwich pattern
- **bottle**: Bottleneck reversal pattern
- **star**: Morning/evening star patterns
- **mirror**: Mirror image patterns
- **blockade**: Blockade formation
- **euphoria**: Euphoria reversal pattern

### 2. Trend Following Strategies

- **adx_indicator**: Average Directional Index
- **donchian_indicator**: Donchian channel breakouts
- **squeeze_momentum**: Bollinger Band squeeze with momentum
- **vertical_horizontal_cross**: Vertical/horizontal filter cross
- **heikin_ashi**: Heikin-Ashi candlestick smoothing
- **tf12_vama**: Variable moving average trend following
- **tf9_tii**: Trend intensity index
- **aroon_oscillator**: Aroon oscillator
- **macd_change**: MACD signal changes
- **tf1_ma**: Moving average crossover
- **tf2_ma**: Dual moving average system
- **tf3_rsi_ma**: RSI with moving average filter
- **tf4_macd**: MACD-based trend following
- **tf5_ma_slope**: Moving average slope analysis
- **tf6_supertrend_flip**: Supertrend indicator
- **tf7_psar_ma**: Parabolic SAR with moving average

### 3. Contrarian Strategies (Mean Reversion)

- **contrarian_rsi_extremes**: RSI overbought/oversold
- **contrarian_stochastic_extremes**: Stochastic overbought/oversold
- **contrarian_stochastic_divergences**: Stochastic divergences
- **contrarian_stochastic_duration**: Stochastic duration analysis
- **contrarian_stochastic_cross**: Stochastic signal crosses
- **contrarian_rsi_divergences**: RSI divergences
- **contrarian_piercing_stoch**: Piercing with stochastic filter
- **contrarian_engulfing_bbands**: Engulfing with Bollinger Bands

### 4. Book of Trading Strategies (BOTS)

- **pattern_marubozu**: Marubozu pattern variations
- **pattern_hammer**: Hammer pattern variations
- **key_reversal**: Key reversal patterns
- **pattern_piercing**: Piercing pattern variations
- **pattern_td_camouflage**: Tom DeMark camouflage
- **pattern_td_clopwin**: Tom DeMark close within
- **pattern_td_waldo_2/5/6/8**: Tom DeMark Waldo patterns

## Universe Segmentation

The system organizes assets into hierarchical universes based on market capitalization and asset type:

### Stock Universes
- **LC1/LC2**: Large Cap stocks (major blue-chip companies)
- **MC1/MC2**: Mid Cap stocks (established mid-sized companies)
- **SC1/SC2/SC3/SC4**: Small Cap stocks (smaller public companies)
- **Micro1/Micro2/Micro3/Micro4**: Micro Cap stocks (smallest public companies)

### Cryptocurrency Universe
- **Crypto**: All cryptocurrencies (excluding stablecoins)

### Universe-Specific Strategy Allocation

Different universes use optimized strategy sets based on market characteristics:

#### Production Strategies (Large/Mid Cap Focus)
- Trend following indicators (ADX, Donchian)
- Candlestick patterns (hammer, marubozu, three_candles)
- Momentum indicators (squeeze, vertical_horizontal_cross)

#### Crypto Strategies (Volatility-Focused)
- Enhanced momentum indicators
- Heikin-Ashi smoothing techniques
- Contrarian RSI/Stochastic strategies
- Tom DeMark patterns

#### Micro/Small Cap Strategies (High Volatility)
- Contrarian mean-reversion strategies
- Candlestick pattern recognition
- Piercing and engulfing patterns

## Execution Modes

### 1. Demo Mode
- **Purpose**: Quick testing and development
- **Data Source**: Local CSV files (no database required)
- **Output**: Temporary results for validation
- **Use Case**: Strategy development and debugging

### 2. Testing Mode
- **Purpose**: Full backtesting with fresh data
- **Data Source**: Downloads from ClickHouse database
- **Output**: Testing results with performance metrics
- **Use Case**: Strategy validation and optimization

### 3. Production Mode
- **Purpose**: Live-ready strategy evaluation
- **Data Source**: Production database tables
- **Output**: Production results with scoring insertion
- **Use Case**: Final strategy selection and deployment

## Data Pipeline

### Price Data Format
The system expects OHLCV (Open, High, Low, Close, Volume) data in CSV format:

```csv
Date,Ticker,Universe,Open,High,Low,Close,Volume
2024-01-01,AAPL,MC1,150.00,155.00,149.00,154.00,1000000
2024-01-01,btc,Crypto,45000.00,46000.00,44000.00,45500.00,2500.50
```

### Data Sources

#### ClickHouse Database Tables
- **tiingo.crypto_prices**: Cryptocurrency price data
- **tiingo.stock_prices**: Stock price data
- **tiingo.strategy**: Strategy metadata and parameters
- **tiingo.fundamentals_list**: Stock fundamental data
- **tiingo.coingecko_metadata**: Cryptocurrency metadata

#### Local CSV Files (Demo Mode)
- Stored in `data/` directory
- Pre-processed OHLCV data
- Used for quick testing without database dependency

### Data Processing Steps

1. **Universe Selection**: Filter assets by market segment
2. **Data Download**: Retrieve price data from ClickHouse
3. **Preprocessing**: Clean and format data for analysis
4. **Signal Generation**: Apply technical indicators and patterns
5. **Backtesting**: Simulate trades based on signals
6. **Performance Calculation**: Compute risk and return metrics
7. **Results Storage**: Save results to database/files

## Backtesting Logic

### Signal Processing
Each strategy function receives a Polars DataFrame containing price data and returns buy/sell signals:

```rust
pub type SignalFunctionWithParam = fn(DataFrame, f64) -> BuySell;
```

The function analyzes price action and returns indices where buy or sell signals occur.

### Trade Simulation
The backtesting engine processes signals chronologically:

1. **Entry Logic**: Buy signals initiate long positions
2. **Exit Logic**: Sell signals close existing positions
3. **Position Management**: One position per asset at a time
4. **Risk Management**: No position sizing or stop-losses (pure signal testing)

### Performance Metrics Calculation

#### Core Metrics
- **Profit Factor**: `total_profits / total_losses`
- **Win Rate**: `winning_trades / total_trades`
- **Expectancy**: `average_win * win_rate - average_loss * loss_rate`
- **Sharpe Ratio**: `(returns - risk_free_rate) / volatility`

#### Risk Metrics
- **Maximum Drawdown**: Peak-to-trough portfolio decline
- **Sortino Ratio**: Returns adjusted for downside volatility
- **Calmar Ratio**: Annual return divided by maximum drawdown
- **Recovery Factor**: Net profit divided by maximum drawdown

#### Trade Metrics
- **Average Gain/Loss**: Mean profit/loss per trade
- **Risk-Reward Ratio**: Average gain divided by average loss
- **Win/Loss Ratio**: Number of winners vs losers

## Parallel Processing

### Batch Processing
- **Batch Size**: 10 tickers processed concurrently
- **Universe Processing**: Sequential universe processing with parallel ticker execution
- **Memory Management**: Lazy loading with Polars DataFrames
- **Resource Optimization**: Efficient memory usage for large datasets

### Performance Characteristics
- **Concurrent Execution**: Multiple tickers tested simultaneously
- **Memory Efficient**: Streaming data processing
- **Fast Execution**: Optimized Rust performance
- **Scalable**: Handles large datasets efficiently

## Output Structure

### Directory Organization
```
backtester/
├── output/
│   ├── production/     # Production backtest results
│   ├── testing/        # Testing backtest results
│   └── crypto/         # Crypto-specific results
├── performance/
│   ├── stocks/         # Stock performance summaries
│   └── crypto/         # Crypto performance summaries
├── decisions/
│   ├── stocks/         # Individual stock trade decisions
│   └── crypto/         # Individual crypto trade decisions
├── score/              # Strategy scoring results
└── data/               # Price data files
```

### File Formats

#### Backtest Results (CSV)
Contains comprehensive performance metrics for each strategy-ticker combination.

#### Decision Files (CSV)
Individual trade decisions with timestamps and actions:
```csv
date,action
2024-01-15,BUY
2024-01-20,SELL
```

#### Performance Summaries (CSV)
Aggregated results across tickers and strategies.

#### Score Files (Database)
Strategy rankings inserted into ClickHouse for portfolio optimization.

## Command Line Interface

### Main Backtester
```bash
cargo run [UNIVERSE] [MODE] [OPTIONS]
```

#### Parameters
- **Universe**: `Crypto`, `SC`, `MC`, `LC`, `Micro`, `Stocks`
- **Mode**: `demo`, `testing`, `production`
- **Tickers**: Comma-separated list of specific assets
- **Strategy**: Filter by specific strategy name
- **Path**: Custom working directory
- **Verbose**: Logging level control

### Decision Analyzer
```bash
cargo run --bin backtester_decisions [OPTIONS]
```

Analyzes trading decisions and generates reports.

### Portfolio Backtester
```bash
cargo run --bin portfolio_backtest [OPTIONS]
```

Combines multiple strategies into portfolio backtests.

## Database Integration

### ClickHouse Schema
- **tiingo.crypto_prices**: Cryptocurrency OHLCV data
- **tiingo.stock_prices**: Stock OHLCV data
- **tiingo.strategy**: Strategy definitions and parameters
- **tiingo.fundamentals_list**: Stock fundamental data
- **tiingo.coingecko_metadata**: Cryptocurrency metadata
- **tiingo.buy_sell_decisions**: Trading decision storage
- **tiingo.strategy_scores**: Strategy performance scores

### Connection Management
- **Local Connection**: Direct database access (192.168.86.46:8123)
- **Remote Connection**: Network database access (192.168.86.56:8123)
- **Authentication**: User/password authentication
- **Timeout Handling**: 5-second connection timeouts

## Configuration

### Environment Variables
- `CLICKHOUSE_USER_PATH`: Base path for data files (default: `/srv`)
- `PG`: ClickHouse password (required for database access)

### Build Configuration
- **Rust Edition**: 2021
- **Dependencies**: Polars, ClickHouse, Tokio, Serde, Chrono
- **Features**: Parallel processing, async operations, JSON serialization

## Performance Optimization

### Technical Optimizations
- **Zero-Copy Operations**: Efficient data handling with Polars
- **Lazy Evaluation**: Deferred computation for large datasets
- **Memory Pooling**: Optimized memory allocation
- **SIMD Operations**: Vectorized computations where possible

### Algorithmic Optimizations
- **Signal Preprocessing**: Efficient indicator calculations
- **Trade Simulation**: Optimized position tracking
- **Metrics Calculation**: Vectorized statistical computations
- **File I/O**: Batched write operations

## Error Handling and Logging

### Logging Levels
- **Error**: Critical failures and system errors
- **Warn**: Non-critical issues and warnings
- **Info**: General operational information
- **Debug**: Detailed debugging information
- **Trace**: Comprehensive execution tracing

### Error Recovery
- **Connection Failures**: Automatic retry with backoff
- **Data Errors**: Graceful handling of missing or corrupted data
- **File System Errors**: Robust file operation error handling
- **Memory Issues**: Efficient memory management and cleanup

## Testing and Validation

### Unit Tests
- Individual signal function validation
- Data processing pipeline testing
- Performance calculation verification

### Integration Tests
- Full backtesting workflow validation
- Database integration testing
- File I/O operation verification

### Performance Benchmarks
- Execution time measurement
- Memory usage profiling
- Scalability testing across different data sizes

## Usage Examples

### Basic Crypto Backtest
```bash
cargo run Crypto testing
```

### Specific Stock Tickers
```bash
cargo run MC1 testing --tickers AAPL,MSFT,GOOGL
```

### Single Strategy Test
```bash
cargo run Crypto testing --strategy hammer
```

### Production Run with Verbose Logging
```bash
cargo run Crypto production --verbose
```

### Demo Mode for Development
```bash
cargo run MC1 demo --verbose
```

## Future Enhancements

### Planned Features
- **Machine Learning Integration**: ML-based signal optimization
- **Real-time Processing**: Live trading signal generation
- **Advanced Risk Management**: Position sizing and stop-losses
- **Multi-timeframe Analysis**: Cross-timeframe signal validation
- **Portfolio Optimization**: Modern portfolio theory implementation
- **Alternative Data**: News sentiment and social media integration

### Performance Improvements
- **GPU Acceleration**: CUDA/OpenCL signal processing
- **Distributed Computing**: Multi-node backtesting clusters
- **Memory Optimization**: Further reduction in memory footprint
- **Algorithm Parallelization**: Enhanced concurrent processing

This documentation provides a comprehensive overview of the backtester system's architecture, functionality, and operation. The system represents a sophisticated quantitative trading research platform capable of evaluating complex trading strategies across multiple asset classes with high performance and reliability.</content>
<parameter name="filePath">/Users/rogerbos/rust_home/backtester/BACKTESTER_COMPREHENSIVE_DOCS.md