#!/bin/bash
#./run_stocks.sh > run.log 2>&1

echo "Starting Stocks backtest at $(date)"
# ./target/release/backtester -u SC -m testing -o 20260218
# ./target/release/backtester -u MC -m testing -o 20260218
./target/release/backtester -u LC -m testing -o 20260218
# ./target/release/backtester -u Micro -m testing -o 20260218
STOCKS_EXIT=$?
echo "Stocks backtest finished with exit code $STOCKS_EXIT at $(date)"

echo "Starting Crypto backtest at $(date)"
./target/release/backtester -u Crypto -m testing -o 20260218
CRYPTO_EXIT=$?
echo "Crypto backtest finished with exit code $CRYPTO_EXIT at $(date)"
