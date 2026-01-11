#!/usr/bin/env python3
"""
Compare AI trades with set1 trades for both long and short directions
"""

import csv
from datetime import datetime, timedelta
from collections import defaultdict


def parse_datetime(dt_str):
    """Parse datetime string"""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def load_ai_trades(filename):
    """Load AI-calculated trades"""
    trades = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append({
                'entry_time': parse_datetime(row['entry_time']),
                'entry_price': float(row['entry_price']),
                'exit_time': parse_datetime(row['exit_time']),
                'exit_price': float(row['exit_price']),
                'high_price': float(row['high_price']),
                'low_price': float(row['low_price']),
                'profitable': row['profitable'] == 'True',
                'profit_target': float(row['profit_target']),
                'stop_target': float(row['stop_target']),
                'pnl': float(row['pnl']),
                'exit_reason': row['exit_reason'],
                'prev_hour_9ema': float(
                    row.get('prev_hour_9ema',
                            row.get('prev_e1h_9ema_value', 0))),
            })
    return trades


def load_set1_trades(filename):
    """Load set1 trades"""
    trades = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append({
                'open_dt': parse_datetime(row['open_dt']),
                'close_dt': parse_datetime(row['close_dt']),
                'entry_price': float(row['entry_price']),
                'exit_price': float(row['exit_price']),
                'high_price': float(row['high_price']),
                'low_price': float(row['low_price']),
                'profitable': row['profitable'] == 'True',
                'prof_target': float(row['prof_target']),
                'stop_target': float(row['stop_target']),
                'prev_hour_9ema': (float(row.get('prev_hour_9ema', 0))
                                   if row.get('prev_hour_9ema') else 0),
            })
    return trades


def compare_trades(ai_filename, set1_filename, direction):
    """Compare trades for a specific direction"""
    print(f"\n{'='*80}")
    print(f"{direction.upper()} TRADES COMPARISON")
    print('='*80)

    ai_trades = load_ai_trades(ai_filename)
    set1_trades = load_set1_trades(set1_filename)

    # Calculate statistics
    ai_profitable = sum(1 for t in ai_trades if t['profitable'])
    ai_total_pnl = sum(t['pnl'] for t in ai_trades)

    set1_profitable = sum(1 for t in set1_trades if t['profitable'])
    set1_total_pnl = sum(
        (t['exit_price'] - t['entry_price']) if direction == 'long'
        else (t['entry_price'] - t['exit_price']) for t in set1_trades)

    print("\nCALCULATION RESULTS:")
    ai_win_rate = (100*ai_profitable/len(ai_trades)
                   if ai_trades else 0)
    print(f"  AI:   {len(ai_trades)} trades, {ai_profitable} profitable "
          f"({ai_win_rate:.1f}%), ${ai_total_pnl:.2f} total P&L")
    set1_win_rate = (100*set1_profitable/len(set1_trades)
                     if set1_trades else 0)
    print(f"  Set1: {len(set1_trades)} trades, {set1_profitable} "
          f"profitable ({set1_win_rate:.1f}%), "
          f"${set1_total_pnl:.2f} total P&L")

    # Match trades by entry time (within 5 minutes tolerance)
    matched_trades = []
    ai_only = []
    set1_only = list(set1_trades)

    for ai_trade in ai_trades:
        matched = False
        for set1_trade in set1_only[:]:
            time_diff = abs(
                (ai_trade['entry_time'] - set1_trade['open_dt'])
                .total_seconds())
            if time_diff <= 300:  # 5 minute tolerance
                matched_trades.append((ai_trade, set1_trade))
                set1_only.remove(set1_trade)
                matched = True
                break
        if not matched:
            ai_only.append(ai_trade)

    print("\nTRADE MATCHING:")
    print(f"  Matched trades: {len(matched_trades)}")
    print(f"  AI only: {len(ai_only)}")
    print(f"  Set1 only: {len(set1_only)}")

    # Compare matched trades
    identical_count = 0
    field_differences = defaultdict(int)

    for ai_trade, set1_trade in matched_trades:
        is_identical = True

        # Compare each field
        if abs(ai_trade['entry_price'] - set1_trade['entry_price']) > 0.01:
            field_differences['entry_price'] += 1
            is_identical = False

        if abs(ai_trade['exit_price'] - set1_trade['exit_price']) > 0.01:
            field_differences['exit_price'] += 1
            is_identical = False

        if abs(ai_trade['high_price'] - set1_trade['high_price']) > 0.01:
            field_differences['high_price'] += 1
            is_identical = False

        if abs(ai_trade['low_price'] - set1_trade['low_price']) > 0.01:
            field_differences['low_price'] += 1
            is_identical = False

        if abs(ai_trade['profit_target'] - set1_trade['prof_target']) > 0.01:
            field_differences['profit_target'] += 1
            is_identical = False

        if abs(ai_trade['stop_target'] - set1_trade['stop_target']) > 0.01:
            field_differences['stop_target'] += 1
            is_identical = False

        entry_time_diff = abs(
            (ai_trade['entry_time'] - set1_trade['open_dt'])
            .total_seconds())
        if entry_time_diff > 1:
            field_differences['entry_time'] += 1
            is_identical = False

        exit_time_diff = abs(
            (ai_trade['exit_time'] - set1_trade['close_dt'])
            .total_seconds())
        if exit_time_diff > 1:
            field_differences['exit_time'] += 1
            is_identical = False

        if ai_trade['profitable'] != set1_trade['profitable']:
            field_differences['profitable'] += 1
            is_identical = False

        if is_identical:
            identical_count += 1

    print("\nMATCH QUALITY:")
    match_pct = (100*identical_count/len(matched_trades)
                 if matched_trades else 0)
    print(f"  Identical trades: {identical_count}/{len(matched_trades)} "
          f"({match_pct:.1f}%)")

    if field_differences:
        print("\n  Field differences:")
        for field, count in sorted(field_differences.items()):
            print(f"    {field}: {count} trades")
    else:
        print("  ✓ All matched trades are 100% identical!")

    # Show sample of non-matching trades
    if ai_only:
        print("\n  Sample AI-only trades (first 3):")
        for trade in ai_only[:3]:
            print(f"    {trade['entry_time']}: "
                  f"entry=${trade['entry_price']}, "
                  f"exit=${trade['exit_price']}, pnl=${trade['pnl']:.2f}")

    if set1_only:
        print("\n  Sample Set1-only trades (first 3):")
        for trade in set1_only[:3]:
            pnl = ((trade['exit_price'] - trade['entry_price'])
                   if direction == 'long'
                   else (trade['entry_price'] - trade['exit_price']))
            print(f"    {trade['open_dt']}: "
                  f"entry=${trade['entry_price']}, "
                  f"exit=${trade['exit_price']}, pnl=${pnl:.2f}")

    return {
        'total_ai': len(ai_trades),
        'total_set1': len(set1_trades),
        'matched': len(matched_trades),
        'identical': identical_count,
        'ai_only': len(ai_only),
        'set1_only': len(set1_only),
        'match_rate': (100 * identical_count / len(matched_trades)
                       if matched_trades else 0)
    }


if __name__ == '__main__':
    print("\n" + "="*80)
    print("COMPARING AI TRADES VS SET1 TRADES")
    print("="*80)

    # Compare long trades
    long_results = compare_trades(
        'ai_trades_long.csv', 'set1_trades_long.csv', 'long')

    # Compare short trades
    short_results = compare_trades(
        'ai_trades_short.csv', 'set1_trades_short.csv', 'short')

    # Overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print('='*80)
    print("\nLONG TRADES:")
    print(f"  Match rate: {long_results['match_rate']:.1f}%")
    print(f"  Identical: {long_results['identical']}/"
          f"{long_results['matched']} matched trades")

    print("\nSHORT TRADES:")
    print(f"  Match rate: {short_results['match_rate']:.1f}%")
    print(f"  Identical: {short_results['identical']}/"
          f"{short_results['matched']} matched trades")

    total_matched = long_results['matched'] + short_results['matched']
    total_identical = long_results['identical'] + short_results['identical']

    print("\nCOMBINED:")
    total_ai = long_results['total_ai']+short_results['total_ai']
    total_set1 = long_results['total_set1']+short_results['total_set1']
    print(f"  Total trades: AI={total_ai}, Set1={total_set1}")
    combined_rate = (100*total_identical/total_matched
                     if total_matched else 0)
    print(f"  Match rate: {combined_rate:.1f}%")
    print(f"  Identical: {total_identical}/{total_matched} matched trades")
    print("="*80)
