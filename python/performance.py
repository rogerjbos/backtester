import pandas as pd
import numpy as np

# Set pandas display options
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Read the performance results
df = pd.read_csv('../performance_results_crypto.csv')

print("=" * 80)
print("CRYPTO PERFORMANCE ANALYSIS")
print("=" * 80)
print(f"\nTotal rows: {len(df):,}")
print(f"Unique tickers: {df['ticker'].nunique()}")
print(f"Unique strategies: {df['strategy'].nunique()}")

# ============================================================================
# 1) Average all strategy results and sort by best st_accuracy
# ============================================================================
print("\n" + "=" * 80)
print("1) STRATEGIES RANKED BY SHORT-TERM ACCURACY")
print("=" * 80)

strategy_avg = df.groupby('strategy').agg({
    'st_cum_return': 'mean',
    'st_accuracy': 'mean',
    'mt_cum_return': 'mean',
    'mt_accuracy': 'mean',
    'lt_cum_return': 'mean',
    'lt_accuracy': 'mean',
    'bh_cum_return': 'mean',
    'bh_accuracy': 'mean',
    'buy_and_hold_return': 'mean'
}).reset_index()

# Sort by st_accuracy (descending)
strategy_avg_sorted = strategy_avg.sort_values('st_accuracy', ascending=False)

print("\nTop 20 Strategies by Short-Term Accuracy:")
print(strategy_avg_sorted.head(20).to_string(index=False))

print("\n\nTop 20 Strategies by Short-Term Cumulative Return:")
strategy_by_return = strategy_avg.sort_values('st_cum_return', ascending=False)
print(strategy_by_return.head(20).to_string(index=False))

# ============================================================================
# 2) For each ticker, show the top strategies
# ============================================================================
print("\n" + "=" * 80)
print("2) TOP 3 STRATEGIES PER TICKER (by ST Accuracy)")
print("=" * 80)

# Get top 3 strategies per ticker by st_accuracy
top_strategies_per_ticker = (
    df.sort_values(['ticker', 'st_accuracy'], ascending=[True, False])
    .groupby('ticker')
    .head(3)
    .sort_values(['ticker', 'st_accuracy'], ascending=[True, False])
)

print(f"\nShowing top 3 strategies for each ticker (first 10 tickers)...")
tickers_to_show = df['ticker'].unique()[:10]
for ticker in tickers_to_show:
    ticker_data = top_strategies_per_ticker[top_strategies_per_ticker['ticker'] == ticker]
    print(f"\n{ticker}:")
    for idx, row in ticker_data.iterrows():
        print(f"  {row['strategy']:40s} | ST Acc: {row['st_accuracy']:.4f} | "
              f"ST Return: {row['st_cum_return']:8.2f} | "
              f"BH Return: {row['buy_and_hold_return']:8.2f}")

# ============================================================================
# 3) Additional Analysis: Best ticker-strategy combinations
# ============================================================================
print("\n" + "=" * 80)
print("3) BEST TICKER-STRATEGY COMBINATIONS")
print("=" * 80)

print("\nTop 20 by Short-Term Accuracy:")
top_by_accuracy = df.nlargest(20, 'st_accuracy')[
    ['ticker', 'strategy', 'st_accuracy', 'st_cum_return', 'mt_accuracy', 'lt_accuracy', 'buy_and_hold_return']
]
print(top_by_accuracy.to_string(index=False))

print("\n\nTop 20 by Short-Term Cumulative Return:")
top_by_st_return = df.nlargest(20, 'st_cum_return')[
    ['ticker', 'strategy', 'st_cum_return', 'st_accuracy', 'mt_cum_return', 'lt_cum_return', 'buy_and_hold_return']
]
print(top_by_st_return.to_string(index=False))

# ============================================================================
# 4) Summary Statistics
# ============================================================================
print("\n" + "=" * 80)
print("4) SUMMARY STATISTICS")
print("=" * 80)

print("\nOverall Statistics:")
summary_stats = df[['st_accuracy', 'mt_accuracy', 'lt_accuracy', 'bh_accuracy', 
                     'st_cum_return', 'mt_cum_return', 'lt_cum_return', 'bh_cum_return',
                     'buy_and_hold_return']].describe()
print(summary_stats)

# Save top strategies per ticker to CSV
output_file = '../top_strategies_per_ticker.csv'
top_strategies_per_ticker.to_csv(output_file, index=False)
print(f"\n\nTop 3 strategies per ticker saved to: {output_file}")

# Save strategy averages to CSV
strategy_output_file = '../strategy_averages.csv'
strategy_avg_sorted.to_csv(strategy_output_file, index=False)
print(f"Strategy averages saved to: {strategy_output_file}")
