
dataset = "baseline_20251109"
asset_type = "stocks"
ticker = "AAPL"
api_base_url = "https://api.rogerjbos.com/api"
api_key = "NiN9834uij09uij2oijeNiN"


import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

import os
import clickhouse_connect
chr = clickhouse_connect.get_client(
    host="192.168.86.246",
    user="roger", 
    password=os.getenv("PG")
)

def get_api_data(endpoint, params=None):
    """Fetch data from API with error handling"""
    if endpoint.startswith("backtester_decisions"):
        base_url = "https://rogerjbos.com/api"
    else:
        base_url = "https://api.rogerjbos.com/api"
    
    url = f"{base_url}/{endpoint.lstrip('/')}"
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        response = requests.get(url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {endpoint}: {e}")
        return None
      
tickers = get_api_data(f"backtester_decisions/{asset_type}/tickers", {"dataset": dataset})


ticker = "AAPL"

decisions_data = get_api_data(f"backtester_decisions/{asset_type}/{ticker}", {"dataset": dataset})


if decisions_data:
    decisions_df = pd.DataFrame(decisions_data)
    strategies = decisions_df['strategy'].unique().tolist()[0] # HERE
    print(f"Loaded {len(decisions_df)} decisions for {len(strategies)} strategies")
else:
    decisions_df = pd.DataFrame()
    strategies = []
    print("No decisions data found")


# Load returns data
if asset_type == "stocks":
    query = returns_endpoint = "stock_prices"
    symbol_param = "ticker"
else:
    returns_endpoint = "crypto_prices"
    symbol_param = "ticker"

 
start_date = '2005-01-01' #decisions_df['date'].min()
end_date = datetime.now().strftime("%Y-%m-%d")
returns_data = get_api_data(returns_endpoint, {'ticker': ticker, 'start_date': start_date, 'end_date': end_date})


if returns_data:
    returns_df = pd.DataFrame(returns_data)
    returns_df.shape[0]
    returns_df['date'].head(1)
    returns_df['date'].tail(1)
    
    returns_df = returns_df[['date', 'close']].copy()
    returns_df['date'] = pd.to_datetime(returns_df['date'])
    returns_df = returns_df.sort_values('date')
    returns_df['daily_return'] = returns_df['close'].pct_change() * 100
    returns_df['symbol'] = ticker
    returns_df = returns_df.drop_duplicates(subset='date', keep='first')
    returns_df = returns_df[returns_df['daily_return'].abs() <= 50]
    print(f"Loaded {len(returns_df)} returns records")
else:
    returns_df = pd.DataFrame()
    print("No returns data found")


def calculate_strategy_data(decisions_df, returns_df, asset_type, ticker):
    """Calculate strategy performance data similar to the React component"""

    if decisions_df.empty or returns_df.empty:
        return {}

    # Group decisions by strategy
    strategy_data = {}

    st = 20
    mt = 100
    lt = 250  
    for strategy_name in decisions_df['strategy'].unique():
        strategy_decisions = decisions_df[decisions_df['strategy'] == strategy_name].copy()
        strategy_decisions['date'] = pd.to_datetime(strategy_decisions['date'])
        df = returns_df.copy()
        df = pd.merge(df, strategy_decisions[['date','action']], how='left', left_on='date', right_on='date')
        
        # Fill bh, st, mt, lt columns
        df = df.sort_values('date')
        bh = 0
        st_val = 0
        mt_val = 0
        lt_val = 0
        last_buy_date = None
        bh_list = []
        st_list = []
        mt_list = []
        lt_list = []
        for _, row in df.iterrows():
            action = row['action'] if pd.notna(row['action']) else None
            date = row['date']
            if action == 'buy':
                if last_buy_date is None:  # first buy
                    bh = 1
                st_val = 1
                mt_val = 1
                lt_val = 1
                last_buy_date = date
            elif action == 'sell':
                bh = 0
                st_val = 0
                mt_val = 0
                lt_val = 0
                last_buy_date = None
            else:
                if last_buy_date:
                    days_since_buy = (date - last_buy_date).days
                    if days_since_buy > st:
                        st_val = 0
                    if days_since_buy > mt:
                        mt_val = 0
                    if days_since_buy > lt:
                        lt_val = 0
            bh_list.append(bh)
            st_list.append(st_val)
            mt_list.append(mt_val)
            lt_list.append(lt_val)
        df['bh'] = bh_list
        df['st'] = st_list
        df['mt'] = mt_list
        df['lt'] = lt_list

        # Calculate cumulative returns and accuracy for each position column
        position_stats = {}
        for col in ['bh', 'st', 'mt', 'lt']:
            df[f'{col}_cum_return'] = (df[col] * df['daily_return']).cumsum()
            num_positive = ((df[col] == 1) & (df['daily_return'] > 0)).sum()
            total_days = (df[col] == 1).sum()
            accuracy = num_positive / total_days if total_days > 0 else 0
            position_stats[f'{col}_cum_return'] = df[f'{col}_cum_return'].iloc[-1] if not df.empty else 0
            position_stats[f'{col}_accuracy'] = accuracy

        returns_list = []

        # Calculate periods
        first_decision_date = strategy_decisions['date'].min()

        # Not held period before first decision
        if not returns_df.empty:
            data_start_date = returns_df['date'].min()
            if first_decision_date > data_start_date:
                period_returns = returns_df[
                    (returns_df['date'] >= data_start_date) &
                    (returns_df['date'] <= first_decision_date)
                ]

                if not period_returns.empty:
                    log_returns = np.log(1 + period_returns['daily_return'].fillna(0) / 100)
                    cumulative_return = (np.exp(log_returns.sum()) - 1) * 100
                    duration = (first_decision_date - data_start_date).days

                    if duration > 0:
                        returns_list.append({
                            'periodType': 'not_held',
                            'startDate': data_start_date.strftime('%Y-%m-%d'),
                            'endDate': first_decision_date.strftime('%Y-%m-%d'),
                            'return': cumulative_return,
                            'duration': duration,
                            'periodLabel': f'Not Held {len([r for r in returns_list if r["periodType"] == "not_held"]) + 1}'
                        })

        # Process decisions
        position_held = False
        last_position_change = first_decision_date

        for _, decision in strategy_decisions.iterrows():
            decision_date = decision['date']

            # Calculate return for previous period
            if position_held and decision_date > last_position_change:
                period_returns = returns_df[
                    (returns_df['date'] >= last_position_change) &
                    (returns_df['date'] <= decision_date)
                ]

                if not period_returns.empty:
                    log_returns = np.log(1 + period_returns['daily_return'].fillna(0) / 100)
                    cumulative_return = (np.exp(log_returns.sum()) - 1) * 100
                    duration = (decision_date - last_position_change).days

                    if duration > 0:
                        returns_list.append({
                            'periodType': 'held',
                            'startDate': last_position_change.strftime('%Y-%m-%d'),
                            'endDate': decision_date.strftime('%Y-%m-%d'),
                            'return': cumulative_return,
                            'duration': duration,
                            'periodLabel': f'Held {len([r for r in returns_list if r["periodType"] == "held"]) + 1}'
                        })

            elif not position_held and decision_date > last_position_change:
                period_returns = returns_df[
                    (returns_df['date'] >= last_position_change) &
                    (returns_df['date'] <= decision_date)
                ]

                if not period_returns.empty:
                    log_returns = np.log(1 + period_returns['daily_return'].fillna(0) / 100)
                    cumulative_return = (np.exp(log_returns.sum()) - 1) * 100
                    duration = (decision_date - last_position_change).days

                    if duration > 0:
                        returns_list.append({
                            'periodType': 'not_held',
                            'startDate': last_position_change.strftime('%Y-%m-%d'),
                            'endDate': decision_date.strftime('%Y-%m-%d'),
                            'return': cumulative_return,
                            'duration': duration,
                            'periodLabel': f'Not Held {len([r for r in returns_list if r["periodType"] == "not_held"]) + 1}'
                        })

            # Update position
            action = str(decision['action']).lower()
            if action == 'buy':
                position_held = True
            elif action == 'sell':
                position_held = False

            last_position_change = decision_date

        # Final period
        last_decision = strategy_decisions.iloc[-1]
        last_decision_date = last_decision['date']
        end_date = datetime.now()

        if position_held and end_date > last_decision_date:
            period_returns = returns_df[
                (returns_df['date'] >= last_decision_date) &
                (returns_df['date'] <= end_date)
            ]

            if not period_returns.empty:
                log_returns = np.log(1 + period_returns['daily_return'].fillna(0) / 100)
                cumulative_return = (np.exp(log_returns.sum()) - 1) * 100
                duration = (end_date - last_decision_date).days

                if duration > 0:
                    returns_list.append({
                        'periodType': 'held',
                        'startDate': last_decision_date.strftime('%Y-%m-%d'),
                        'endDate': end_date.strftime('%Y-%m-%d'),
                        'return': cumulative_return,
                        'duration': duration,
                        'periodLabel': f'Held {len([r for r in returns_list if r["periodType"] == "held"]) + 1}'
                    })

        # Calculate daily returns with period classification
        daily_returns = []

        if not returns_df.empty:
            # Create periods for classification
            periods = []

            if not strategy_decisions.empty:
                first_decision_date = strategy_decisions['date'].min()
                data_start_date = returns_df['date'].min()

                if first_decision_date > data_start_date:
                    periods.append({
                        'startDate': data_start_date,
                        'endDate': first_decision_date,
                        'type': 'not_held'
                    })

                position_held = False
                last_position_change = first_decision_date

                for _, decision in strategy_decisions.iterrows():
                    decision_date = decision['date']

                    if position_held and decision_date > last_position_change:
                        periods.append({
                            'startDate': last_position_change,
                            'endDate': decision_date,
                            'type': 'held'
                        })
                    elif not position_held and decision_date > last_position_change:
                        periods.append({
                            'startDate': last_position_change,
                            'endDate': decision_date,
                            'type': 'not_held'
                        })

                    action = str(decision['action']).lower()
                    if action == 'buy':
                        position_held = True
                    elif action == 'sell':
                        position_held = False

                    last_position_change = decision_date

                # Final period
                last_decision_date = strategy_decisions['date'].max()
                data_end_date = returns_df['date'].max()

                if position_held and data_end_date > last_decision_date:
                    periods.append({
                        'startDate': last_decision_date,
                        'endDate': data_end_date,
                        'type': 'held'
                    })

            # Classify each daily return
            for _, day in returns_df.iterrows():
                day_date = day['date']
                period = next((p for p in periods if p['startDate'] <= day_date <= p['endDate']), None)

                if period:
                    daily_returns.append({
                        'date': day_date.strftime('%Y-%m-%d'),
                        'dailyReturn': day['daily_return'] if pd.notna(day['daily_return']) else 0,
                        'periodType': period['type'],
                        'timestamp': day_date.timestamp() * 1000  # milliseconds for JS
                    })

        # Calculate overall statistics
        held_returns = [r for r in returns_list if r['periodType'] == 'held']
        held_cumulative_return = sum(r['return'] for r in held_returns)

        # Buy and hold return
        buy_and_hold_return = 0
        if not returns_df.empty and len(returns_df) > 1:
            log_returns = np.log(1 + returns_df['daily_return'].fillna(0) / 100)
            buy_and_hold_return = (np.exp(log_returns.sum()) - 1) * 100

        strategy_data[strategy_name] = {
            'tradeReturns': returns_list,
            'dailyReturns': sorted(daily_returns, key=lambda x: x['timestamp']),
            'overallStats': {
                'heldCumulativeReturn': held_cumulative_return,
                'buyAndHoldReturn': buy_and_hold_return,
                **position_stats
            }
        }

    return strategy_data

# Calculate strategy data
if not decisions_df.empty and not returns_df.empty:
    strategy_data = calculate_strategy_data(decisions_df, returns_df, asset_type, ticker)
    print(f"Calculated data for {len(strategy_data)} strategies")
else:
    strategy_data = {}
    print("No strategy data to calculate")


# **Dataset:** `{python} dataset`
# **Asset Type:** `{python} asset_type`
# **Ticker:** `{python} ticker`

# Data Summary

if not decisions_df.empty:
    print(f"### Decisions Data")
    print(f"- Total decisions: {len(decisions_df)}")
    print(f"- Date range: {decisions_df['date'].min()} to {decisions_df['date'].max()}")
    print(f"- Strategies: {', '.join(strategies)}")
    print()


if strategy_data:
    print(f"### Strategy Analysis")
    print(f"- Strategies analyzed: {len(strategy_data)}")
    
    # Collect statistics for all strategies
    stats_list = []
    
    for strategy_name, data in strategy_data.items():
        print(f"### {strategy_name}")
        print()

        # Statistics
        stats = data['overallStats']
        daily_returns = data['dailyReturns']
        trade_returns = data['tradeReturns']

        print("#### Key Statistics")
        
        # Collect stats for this strategy
        strategy_stats = {
            'Strategy': strategy_name,
            'Total Days': len(daily_returns),
            'Held Days': len([d for d in daily_returns if d['periodType'] == 'held']),
            'Total Periods': len(trade_returns),
            'Held Periods': len([t for t in trade_returns if t['periodType'] == 'held']),
            'Held Return (%)': stats['heldCumulativeReturn'],
            'Buy & Hold Return (%)': stats['buyAndHoldReturn'],
            'BH Cumulative Return (%)': stats['bh_cum_return'],
            'BH Accuracy': stats['bh_accuracy'],
            'ST Cumulative Return (%)': stats['st_cum_return'],
            'ST Accuracy': stats['st_accuracy'],
            'MT Cumulative Return (%)': stats['mt_cum_return'],
            'MT Accuracy': stats['mt_accuracy'],
            'LT Cumulative Return (%)': stats['lt_cum_return'],
            'LT Accuracy': stats['lt_accuracy']
        }
        
        stats_list.append(strategy_stats)

    # Display the combined statistics dataframe
    if stats_list:
        stats_df = pd.DataFrame(stats_list)
else:
    print("No strategy data available for analysis.")
