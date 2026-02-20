#!/usr/bin/env python3
"""
Fix CSV file by properly quoting the signals column.
This script reads a malformed CSV where the signals column contains unquoted commas
and writes a properly formatted CSV with quoted fields.
"""
import csv
import sys
from pathlib import Path

def fix_csv(input_file: str, output_file: str = None):
    """Fix CSV by reading with flexible parsing and writing with proper quoting."""

    if output_file is None:
        output_file = input_file.replace('.csv', '_fixed.csv')

    input_path = Path(input_file)
    if not input_path.exists():
        print(f"Error: File not found: {input_file}")
        return False

    # Expected columns (first 25)
    expected_cols = [
        'universe', 'sector', 'cagr', 'total_return_pct', 'initial_value',
        'final_value', 'realized_pnl', 'unrealized_pnl', 'commissions',
        'total_trades', 'winning_trades', 'win_rate_pct', 'avg_win_pct',
        'avg_loss_pct', 'profit_factor', 'max_drawdown_pct', 'sharpe_ratio',
        'avg_holding_days', 'portfolio_size', 'stop_loss_pct', 'lookback_days',
        'signal_date', 'start_date', 'priority_strategy', 'signals'
    ]

    fixed_rows = []

    # Read the malformed CSV
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Process header
    header_line = lines[0].strip()
    header_parts = header_line.split(',')

    # Process data rows
    for line in lines[1:]:
        parts = line.strip().split(',')

        # If we have more than 25 parts, the extras are from the signals column
        if len(parts) > 25:
            # Take the first 24 columns as-is
            row_data = parts[:24]
            # Join the remaining parts as the signals column
            signals = ','.join(parts[24:])
            row_data.append(signals)
        else:
            row_data = parts

        # Create a dictionary with proper column names
        row_dict = {expected_cols[i]: row_data[i] for i in range(len(row_data))}
        fixed_rows.append(row_dict)

    # Write properly formatted CSV with quoting
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=expected_cols, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(fixed_rows)

    print(f"Fixed CSV written to: {output_file}")
    print(f"Processed {len(fixed_rows)} rows")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_csv.py <input_file> [output_file]")
        print("Example: python fix_csv.py results_20250216_2.csv")
        print("If output_file is not specified, '_fixed' will be appended to the input filename")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if fix_csv(input_file, output_file):
        sys.exit(0)
    else:
        sys.exit(1)
