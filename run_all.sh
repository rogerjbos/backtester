#!/bin/bash
#./run_all.sh > run.log 2>&1

echo "Starting Crypto backtest at $(date)"
./target/release/backtester -u Crypto -m testing
CRYPTO_EXIT=$?
echo "Crypto backtest finished with exit code $CRYPTO_EXIT at $(date)"

echo "Starting Stocks backtest at $(date)"
./target/release/backtester -u Stocks -m testing
STOCKS_EXIT=$?
echo "Stocks backtest finished with exit code $STOCKS_EXIT at $(date)"

echo "All backtests complete. Stocks: $STOCKS_EXIT, Crypto: $CRYPTO_EXIT"

