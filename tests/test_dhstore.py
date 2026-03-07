"""Tests for TradeSeries and Trade storage integrity checks."""
import pytest
from dhtrader import (
    delete_backtests_by_field, get_backtests_by_field, get_trades_by_field,
    get_tradeseries_by_field, review_trades,
    review_tradeseries, store_trades,
    store_tradeseries, Trade, TradeSeries)


def clear_storage_by_bt_id(bt_id):
    """Delete all Backtests, TradeSeries, and Trades with bt_id."""
    delete_backtests_by_field(symbol="ES", field="bt_id", value=bt_id,
                              include_tradeseries=True, include_trades=True)
    r = get_backtests_by_field(field="bt_id", value=bt_id)
    assert len(r) == 0
    r = get_tradeseries_by_field(field="bt_id", value=bt_id)
    assert len(r) == 0
    r = get_trades_by_field(field="bt_id", value=bt_id)
    assert len(r) == 0


@pytest.mark.storage
def test_TradeSeries_and_Trade_integrity_checks():
    """Verify storage integrity checks fail for invalid TradeSeries/Trade

    data.
    """
    # Name fake backtest and cleanup stored orphans from past failed runs
    bt = "DELETEME_TRADESERIES_INTEGRITY_TEST"
    clear_storage_by_bt_id(bt)

    # Store a control TradeSeries that has compliant Trades
    ts_good = TradeSeries(start_dt="2025-01-05 00:00:00",
                          end_dt="2025-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_control_pass",
                          params_str="",
                          ts_id=f"{bt}_control_pass",
                          bt_id=bt)
    ts_good.add_trade(Trade(
        open_dt="2025-01-05 10:03:24", direction="long",
        close_dt="2025-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.add_trade(Trade(
        open_dt="2025-01-05 11:23:25", direction="long",
        close_dt="2025-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    store_tradeseries([ts_good], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2

    # Confirm TradeSEries storage integrity check against test bt_id passes
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None

    # Confirm Trade storage integrity check against bt_id passes
    r = review_trades(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None

    # Clean from storage and verify gone
    ts_good.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with multiple Trades opening in the same bar
    ts_fail = TradeSeries(start_dt="2025-01-05 00:00:00",
                          end_dt="2025-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_same_bar_fail",
                          params_str="",
                          ts_id=f"{bt}_same_bar_fail",
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2025-01-05 10:03:24", direction="long",
        close_dt="2025-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2025-01-05 10:23:25", direction="long",
        close_dt="2025-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2

    # Confirm TradeSeries integrity check fails test bt_id fails
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {"trade_overlaps": [
        {"issue_type": "Trade timeframe bar overlap",
         "ts_id": f"{bt}_same_bar_fail",
         "timeframe": "e1h",
         "trade_open": "2025-01-05 10:23:25",
         "trade_open_tf": "2025-01-05 10:00:00",
         "prev_trade_open": "2025-01-05 10:03:24",
         "prev_trade_close": "2025-01-05 10:45:32",
         "prev_trade_close_tf": "2025-01-05 10:00:00"}]}

    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with a Trade opening in the same bar that the prior
    # Trade closed in, after running past the original opening bar
    ts_fail = TradeSeries(start_dt="2025-01-05 00:00:00",
                          end_dt="2025-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_next_bar_fail",
                          params_str="",
                          ts_id=f"{bt}_next_bar_fail",
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2025-01-05 10:03:24", direction="long",
        close_dt="2025-01-05 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2025-01-05 11:53:18", direction="long",
        close_dt="2025-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm storage integrity check against test bt_id fails
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {'trade_overlaps': [
        {'issue_type': 'Trade timeframe bar overlap',
         'ts_id': 'DELETEME_TRADESERIES_INTEGRITY_TEST_next_bar_fail',
         'timeframe': 'e1h',
         'trade_open': '2025-01-05 11:53:18',
         'trade_open_tf': '2025-01-05 11:00:00',
         'prev_trade_open': '2025-01-05 10:03:24',
         'prev_trade_close': '2025-01-05 11:45:32',
         'prev_trade_close_tf': '2025-01-05 11:00:00'}]}
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Add a multiday Trade and confirm Trades integrity check fails
    # Store TradeSeries with a Trade that spans multiple days
    ts_fail = TradeSeries(start_dt="2025-01-06 00:00:00",
                          end_dt="2025-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_multiday_fail",
                          params_str="",
                          ts_id=f"{bt}_multiday_fail",
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2025-01-06 10:03:24", direction="long",
        close_dt="2025-01-07 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2025-01-07 11:53:18", direction="long",
        close_dt="2025-01-07 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm Trade integrity check against test bt_id fails
    r = review_trades(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == ["1 invalid multiday trades found"]
    # Confirm Trade multi_ok list passes otherwise invalid Trades
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      multi_ok=["DELETEME"])
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Confirm duplicate trades fail during Trade integrity check
    ts_fail = TradeSeries(start_dt="2025-01-06 00:00:00",
                          end_dt="2025-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_multiday_fail",
                          params_str="",
                          ts_id=f"{bt}_multiday_fail",
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2025-01-06 10:03:24", direction="long",
        close_dt="2025-01-06 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2025-01-06 10:03:24", direction="long",
        close_dt="2025-01-06 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    # While we stored 2 trades, the second should have overwritten the first
    # by design.  Therefor we only see the second Trade in storage
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    assert stored[0].open_dt == "2025-01-06 10:03:24"
    assert stored[0].close_dt == "2025-01-06 12:32:15"
    # Store the trades
    store_trades([ts_fail.trades[0]])
    stored = get_trades_by_field(field="bt_id", value=bt)
    # Now we should still have only 1 trade, but it's the first not the second
    # this time due to another overwrite
    assert len(stored) == 1
    assert stored[0].open_dt == "2025-01-06 10:03:24"
    assert stored[0].close_dt == "2025-01-06 11:45:32"
    # Change the name attribute to allow it to store itself without replacing
    ts_fail.trades[0].name = "{bt}_multiday_fail_duplicate"
    store_trades([ts_fail.trades[0]])
    # Confirm integrity check now fails due to duplicate trade
    r = review_trades(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == ["1 duplicate trades found"]
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Cleanup storage by bt_id in case anything was left behind somehow
    clear_storage_by_bt_id(bt)
