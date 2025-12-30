#!/bin/bash
#./run_stocks.sh > run.log 2>&1
echo "Starting Stocks backtest at $(date)"
./target/release/backtester -u Stocks -m testing
STOCKS_EXIT=$?
echo "Stocks backtest finished with exit code $STOCKS_EXIT at $(date)"

