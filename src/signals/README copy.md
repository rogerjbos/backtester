# Trading Signals

This directory contains trading signal implementations used by the backtesting system.

## Files

- **sample.rs** - Example implementations showing the structure of technical indicators (SMA, EMA, RSI, MACD)

## Usage

All signal files follow a similar pattern:
- Functions accept Polars `Series` as input (OHLCV data)
- Return `Vec<f64>` or tuples of vectors for multi-output indicators
- Handle edge cases with `NaN` values for insufficient data

See `sample.rs` for concrete examples of implementation patterns.
