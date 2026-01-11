#!/usr/bin/env python3
"""
Calculate ES trades based on EMA bounce/rejection strategy for both
long and short. Created from scratch using only the provided
instructions.
"""

import csv
import math
from datetime import datetime, timedelta, time
from collections import defaultdict


def round_up_quarter(value):
    """Round up to nearest 0.25"""
    return math.ceil(value * 4) / 4


def round_down_quarter(value):
    """Round down to nearest 0.25"""
    return math.floor(value * 4) / 4


def load_ema_data(filename):
    """Load EMA datapoints as sorted list"""
    ema_list = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ema_list.append({
                'dt': datetime.strptime(row['dt'], '%Y-%m-%d %H:%M:%S'),
                'value': float(row['value'])
            })
    return sorted(ema_list, key=lambda x: x['dt'])


def load_hourly_candles(filename):
    """Load hourly candles"""
    candles = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            candles.append({
                'c_datetime': datetime.strptime(
                    row['c_datetime'], '%Y-%m-%d %H:%M:%S'),
                'c_open': float(row['c_open']),
                'c_high': float(row['c_high']),
                'c_low': float(row['c_low']),
                'c_close': float(row['c_close'])
            })
    return sorted(candles, key=lambda x: x['c_datetime'])


def load_minute_candles(filename):
    """Load 1-minute candles"""
    candles = {}
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.strptime(row['c_datetime'], '%Y-%m-%d %H:%M:%S')
            candles[dt] = {
                'c_datetime': dt,
                'c_open': float(row['c_open']),
                'c_high': float(row['c_high']),
                'c_low': float(row['c_low']),
                'c_close': float(row['c_close'])
            }
    return candles


def find_prior_ema(hour_dt, ema_list):
    """Find the most recent EMA datapoint before hour_dt"""
    prior_ema = None
    for ema in ema_list:
        if ema['dt'] < hour_dt:
            prior_ema = ema
        else:
            break
    return prior_ema


def calculate_trades(direction='long', offset_ticks=0,
                     profit_dollars=40, stop_dollars=20):
    """Main calculation function"""
    print(f"\nCalculating {direction.upper()} trades "
          f"(offset_ticks={offset_ticks})...")

    ema_list = load_ema_data('set1_ind_dps_e1h9ema.csv')
    hourly_candles = load_hourly_candles('set1_e1h_candles.csv')
    minute_candles = load_minute_candles('set1_1m_candles.csv')

    trades = []

    print(f"Processing {len(hourly_candles)} hourly candles...")

    for h_candle in hourly_candles:
        hour_dt = h_candle['c_datetime']

        # Rule 4: No trade should ever be opened between 15:55 and 17:59
        hour_time = hour_dt.time()
        if time(15, 55) <= hour_time <= time(17, 59):
            continue

        # Find the most recent prior EMA
        prior_ema = find_prior_ema(hour_dt, ema_list)
        if prior_ema is None:
            continue

        # Calculate entry target based on direction
        if direction == 'long':
            entry_target = (round_up_quarter(prior_ema['value']) -
                            (offset_ticks * 0.25))
        else:  # short
            entry_target = (round_down_quarter(prior_ema['value']) +
                            (offset_ticks * 0.25))

        # Check if hour opened correctly for this direction
        if direction == 'long':
            if h_candle['c_open'] <= entry_target:
                continue
        else:  # short
            if h_candle['c_open'] >= entry_target:
                continue

        # Look for first time price triggered entry
        entry_time = None
        for i in range(60):
            check_dt = hour_dt + timedelta(minutes=i)
            if check_dt not in minute_candles:
                continue

            m_candle = minute_candles[check_dt]
            if direction == 'long':
                # Long: need price to go at least 0.25 below entry target
                if m_candle['c_low'] <= entry_target - 0.25:
                    entry_time = check_dt
                    break
            else:  # short
                # Short: need price to go at least 0.25 above entry target
                if m_candle['c_high'] >= entry_target + 0.25:
                    entry_time = check_dt
                    break

        if entry_time is None:
            continue

        # Trade found - set up parameters
        entry_price = entry_target

        if direction == 'long':
            profit_target = entry_price + profit_dollars
            stop_target = entry_price - stop_dollars
        else:  # short
            profit_target = entry_price - profit_dollars
            stop_target = entry_price + stop_dollars

        high_price = entry_price
        low_price = entry_price
        exit_price = None
        exit_time = None
        exit_reason = None

        # Find next 15:55:00 after entry
        if (entry_time.hour < 15 or
                (entry_time.hour == 15 and entry_time.minute < 55)):
            exit_deadline = datetime.combine(
                entry_time.date(), time(15, 55))
        else:
            exit_deadline = datetime.combine(
                entry_time.date() + timedelta(days=1), time(15, 55))

        # Track the trade minute by minute
        current_dt = entry_time
        max_iterations = 2000  # Safety limit
        iterations = 0

        while exit_price is None and iterations < max_iterations:
            iterations += 1

            if current_dt not in minute_candles:
                current_dt += timedelta(minutes=1)
                continue

            m_candle = minute_candles[current_dt]
            is_entry_candle = (current_dt == entry_time)
            is_autoclose_candle = (current_dt == exit_deadline)

            # Check for autoclose first
            if is_autoclose_candle:
                exit_price = m_candle['c_open']
                exit_time = current_dt
                exit_reason = 'time'
                # Special case 3: Don't update high/low from 15:55 candle
                break

            if direction == 'long':
                # LONG TRADE LOGIC

                # Check stop (priority over profit)
                if m_candle['c_low'] <= stop_target:
                    exit_price = stop_target
                    low_price = stop_target
                    exit_time = current_dt
                    exit_reason = 'stop'

                    # If profit also hit, record high as profit_target
                    if m_candle['c_high'] >= profit_target + 0.25:
                        high_price = profit_target
                    else:
                        high_price = max(high_price, m_candle['c_high'])
                    break

                # Update low price (if stop not hit)
                low_price = min(low_price, m_candle['c_low'])

                # Check profit
                if is_entry_candle:
                    # Entry candle must close at or beyond profit_target
                    if m_candle['c_close'] >= profit_target:
                        exit_price = profit_target
                        high_price = profit_target
                        exit_time = current_dt
                        exit_reason = 'profit'
                        break
                    else:
                        # Track high using close for entry candle
                        high_price = max(high_price, m_candle['c_close'])
                else:
                    # Normal candle: check if high >= profit_target + 0.25
                    if m_candle['c_high'] >= profit_target + 0.25:
                        exit_price = profit_target
                        high_price = profit_target
                        exit_time = current_dt
                        exit_reason = 'profit'
                        break
                    else:
                        high_price = max(high_price, m_candle['c_high'])

            else:  # SHORT TRADE LOGIC

                # Check stop (priority over profit)
                if m_candle['c_high'] >= stop_target:
                    exit_price = stop_target
                    high_price = stop_target
                    exit_time = current_dt
                    exit_reason = 'stop'

                    # If profit also hit, record low as profit_target
                    if m_candle['c_low'] <= profit_target - 0.25:
                        low_price = profit_target
                    else:
                        low_price = min(low_price, m_candle['c_low'])
                    break

                # Update high price (if stop not hit)
                high_price = max(high_price, m_candle['c_high'])

                # Check profit
                if is_entry_candle:
                    # Entry candle must close at or beyond profit_target
                    if m_candle['c_close'] <= profit_target:
                        exit_price = profit_target
                        low_price = profit_target
                        exit_time = current_dt
                        exit_reason = 'profit'
                        break
                    else:
                        # Track low using close for entry candle
                        low_price = min(low_price, m_candle['c_close'])
                else:
                    # Normal candle: check if low <= profit_target - 0.25
                    if m_candle['c_low'] <= profit_target - 0.25:
                        exit_price = profit_target
                        low_price = profit_target
                        exit_time = current_dt
                        exit_reason = 'profit'
                        break
                    else:
                        low_price = min(low_price, m_candle['c_low'])

            # Move to next minute
            current_dt += timedelta(minutes=1)

            # Safety check: don't go past deadline
            if current_dt > exit_deadline:
                break

        # Skip if no valid exit found
        if exit_price is None:
            continue

        # Record the trade
        if direction == 'long':
            profitable = exit_price > entry_price
        else:  # short
            profitable = exit_price < entry_price

        pnl = ((exit_price - entry_price) if direction == 'long'
               else (entry_price - exit_price))

        trade = {
            'hour_dt': hour_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'prev_e1h_9ema_time': prior_ema['dt'].strftime(
                '%Y-%m-%d %H:%M:%S'),
            'prev_e1h_9ema_value': prior_ema['value'],
            'entry_target': entry_target,
            'entry_time': entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            'entry_price': entry_price,
            'profit_target': profit_target,
            'stop_target': stop_target,
            'high_price': high_price,
            'low_price': low_price,
            'exit_time': exit_time.strftime('%Y-%m-%d %H:%M:%S'),
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'profitable': profitable,
            'pnl': pnl
        }
        trades.append(trade)

    print(f"Found {len(trades)} {direction} trades")
    return trades


def write_trades_to_csv(trades, filename, direction):
    """Write trades to CSV file"""
    if not trades:
        print(f"No {direction} trades to write")
        return

    fieldnames = [
        'hour_dt',
        'prev_e1h_9ema_time',
        'prev_e1h_9ema_value',
        'entry_target',
        'entry_time',
        'entry_price',
        'profit_target',
        'stop_target',
        'high_price',
        'low_price',
        'exit_time',
        'exit_price',
        'exit_reason',
        'profitable',
        'pnl'
    ]

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)

    print(f"Wrote {len(trades)} {direction} trades to {filename}")

    # Print summary statistics
    profitable_trades = sum(1 for t in trades if t['profitable'])
    total_pnl = sum(t['pnl'] for t in trades)
    avg_pnl = total_pnl / len(trades) if trades else 0

    print(f"\n{direction.upper()} Calculation Results:")
    print(f"  Total trades: {len(trades)}")
    print(f"  Profitable: {profitable_trades} "
          f"({100*profitable_trades/len(trades):.1f}%)")
    print(f"  Total P&L: ${total_pnl:.2f}")
    print(f"  Average P&L: ${avg_pnl:.2f}")

    # Exit reason breakdown
    exit_reasons = defaultdict(int)
    for t in trades:
        exit_reasons[t['exit_reason']] += 1
    print("  Exit reasons:")
    for reason, count in sorted(exit_reasons.items()):
        print(f"    {reason}: {count} ({100*count/len(trades):.1f}%)")


if __name__ == '__main__':
    print("Loading data files...")

    # Calculate long trades
    long_trades = calculate_trades(direction='long', offset_ticks=0,
                                   profit_dollars=40, stop_dollars=20)
    write_trades_to_csv(long_trades, 'ai_trades_long.csv', 'long')

    # Calculate short trades
    short_trades = calculate_trades(direction='short', offset_ticks=40,
                                    profit_dollars=40, stop_dollars=20)
    write_trades_to_csv(short_trades, 'ai_trades_short.csv', 'short')

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Long trades: {len(long_trades)}")
    print(f"Short trades: {len(short_trades)}")
    print(f"Total trades: {len(long_trades) + len(short_trades)}")
