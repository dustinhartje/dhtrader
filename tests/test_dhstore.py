"""Tests for TradeSeries and backtests integrity storage checks."""
import pytest
from dhtrader import (
    Backtest,
    delete_backtests_by_field, get_backtests_by_field, get_trades_by_field,
    get_tradeseries_by_field, review_trades,
    review_tradeseries, store_trades,
    store_backtests, store_tradeseries, Trade, TradeSeries)


def clear_storage_by_name(name: str):
    """Delete all Backtests, TradeSeries, and Trades with the given name."""
    delete_backtests_by_field(symbol="ES", field="name", value=name,
                              include_tradeseries=True, include_trades=True)
    r = get_backtests_by_field(field="name", value=name)
    assert len(r) == 0
    r = get_tradeseries_by_field(field="name", value=name)
    assert len(r) == 0
    r = get_trades_by_field(field="name", value=name)
    assert len(r) == 0


@pytest.fixture
def cleanup_dhstore_storage():
    """Register integrity-test names for pre- and post-test cleanup.

    The returned helper records each supplied name, immediately clears any
    matching Backtests, TradeSeries, and Trades before the test continues,
    then clears the full registered set again during fixture teardown.
    """
    names = set()

    def register(*new_names):
        for name in new_names:
            names.add(name)
        for name in sorted(names):
            clear_storage_by_name(name)

    yield register

    for name in sorted(names):
        clear_storage_by_name(name)


@pytest.mark.storage
def test_Backtest_TradeSeries_and_Trade_integrity_checks(
        cleanup_dhstore_storage):
    """Verify storage integrity checks fail for invalid Backtests, TradeSeries,
    and Trades.
    """
    bt = "DELETEME_DHSTORE_TESTS"

    # Store a control TradeSeries that has compliant Trades
    control_name = f"{bt}_control_pass"
    cleanup_dhstore_storage(control_name)
    ts_good = TradeSeries(start_dt="2099-01-05 00:00:00",
                          end_dt="2099-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=control_name,
                          params_str="",
                          ts_id=control_name,
                          bt_id=bt)
    ts_good.add_trade(Trade(
        open_dt="2099-01-05 10:03:24", direction="long",
        close_dt="2099-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=control_name,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.add_trade(Trade(
        open_dt="2099-01-05 11:23:25", direction="long",
        close_dt="2099-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=control_name,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    store_tradeseries([ts_good], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2

    # Confirm storage integrity check against sample bt_id passes
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None

    # Confirm Trade storage integrity check against bt_id passes, ignoring
    # test bt for orphan checks
    r = review_trades(bt_id=bt, check_integrity=True, orphan_ok=[bt])
    print(r)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None

    # Clean from storage and verify gone
    ts_good.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with multiple Trades opening in the same bar
    same_bar_name = f"{bt}_same_bar_fail"
    cleanup_dhstore_storage(same_bar_name)
    ts_fail = TradeSeries(start_dt="2099-01-05 00:00:00",
                          end_dt="2099-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=same_bar_name,
                          params_str="",
                          ts_id=same_bar_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 10:03:24", direction="long",
        close_dt="2099-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=same_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 10:23:25", direction="long",
        close_dt="2099-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=same_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2

    # Confirm storage integrity check fails for sample bt_id
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {"trade_overlaps": [
        {"issue_type": "Trade timeframe bar overlap",
         "ts_id": same_bar_name,
         "timeframe": "e1h",
         "trade_open": "2099-01-05 10:23:25",
         "trade_open_tf": "2099-01-05 10:00:00",
         "prev_trade_open": "2099-01-05 10:03:24",
         "prev_trade_close": "2099-01-05 10:45:32",
         "prev_trade_close_tf": "2099-01-05 10:00:00"}]}

    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with a Trade opening in the same bar that the prior
    # Trade closed in, after running past the original opening bar
    next_bar_name = f"{bt}_next_bar_fail"
    cleanup_dhstore_storage(next_bar_name)
    ts_fail = TradeSeries(start_dt="2099-01-05 00:00:00",
                          end_dt="2099-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=next_bar_name,
                          params_str="",
                          ts_id=next_bar_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 10:03:24", direction="long",
        close_dt="2099-01-05 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=next_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 11:53:18", direction="long",
        close_dt="2099-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=next_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm storage integrity check against sample bt_id fails
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {'trade_overlaps': [
        {'issue_type': 'Trade timeframe bar overlap',
         'ts_id': next_bar_name,
         'timeframe': 'e1h',
         'trade_open': '2099-01-05 11:53:18',
         'trade_open_tf': '2099-01-05 11:00:00',
         'prev_trade_open': '2099-01-05 10:03:24',
         'prev_trade_close': '2099-01-05 11:45:32',
         'prev_trade_close_tf': '2099-01-05 11:00:00'}]}
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Add a multiday Trade and confirm Trades storage integrity check fails
    # Store TradeSeries with a Trade that spans multiple days
    multiday_name = f"{bt}_multiday_fail"
    cleanup_dhstore_storage(multiday_name)
    ts_fail = TradeSeries(start_dt="2099-01-06 00:00:00",
                          end_dt="2099-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=multiday_name,
                          params_str="",
                          ts_id=multiday_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-07 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=multiday_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-07 11:53:18", direction="long",
        close_dt="2099-01-07 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=multiday_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm Trade storage integrity check against sample bt_id fails
    r = review_trades(bt_id=bt, check_integrity=True, orphan_ok=[bt])
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == ["1 invalid multiday trades found"]
    # Confirm Trade multi_ok list passes otherwise invalid Trades
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      multi_ok=[bt],
                      orphan_ok=[bt])
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Add an unclosed Trade and confirm it is flagged as unclosed_trade
    unclosed_name = f"{bt}_unclosed_fail"
    cleanup_dhstore_storage(unclosed_name)
    ts_fail = TradeSeries(start_dt="2099-01-06 00:00:00",
                          end_dt="2099-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=unclosed_name,
                          params_str="",
                          ts_id=unclosed_name,
                          bt_id=bt)
    t_unclosed = Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-06 10:45:32", timeframe="e1h",
        trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=unclosed_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id)
    t_unclosed.close_dt = None
    t_unclosed.close_date = None
    t_unclosed.close_time = None
    t_unclosed.exit_price = None
    t_unclosed.profitable = None
    t_unclosed.is_open = True
    ts_fail.add_trade(t_unclosed)
    store_tradeseries([ts_fail], include_trades=True)
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      list_issues=True,
                      orphan_ok=[bt])
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["unclosed_trade_errors"] == 1
    assert r["integrity"]["invalid_multiday_trades"] == 0
    assert r["integrity"]["issues"] == [
        "1 unclosed_trade errors found"
    ]
    assert r["integrity"]["unclosed_trades"][0]["issue_type"] == (
        "unclosed_trade"
    )
    ts_fail.delete_from_storage(include_trades=True)

    # Confirm orphaned_test_objects catches all supported object types
    orphan_backtest_name = "TEST_orphan_backtest"
    orphan_ts_name = "TEST_orphan_tradeseries"
    orphan_trade_name = "TEST_orphan_trade"
    cleanup_dhstore_storage(orphan_backtest_name,
                            orphan_ts_name,
                            orphan_trade_name)
    bt_orphan = Backtest(start_dt="2099-01-01 00:00:00",
                         end_dt="2099-01-02 00:00:00",
                         timeframe="e1h",
                         trading_hours="eth",
                         symbol="ES",
                         name=orphan_backtest_name,
                         parameters={},
                         bt_id=bt,
                         prefer_stored=False,
                         autoload_charts=False)
    ts_orphan = TradeSeries(start_dt="2099-01-01 00:00:00",
                            end_dt="2099-01-01 23:59:59",
                            timeframe="e1h",
                            trading_hours="eth",
                            symbol="ES",
                            name=orphan_ts_name,
                            params_str="",
                            ts_id=f"{bt}_TEST_TS",
                            bt_id=bt)
    ts_orphan.add_trade(Trade(
        open_dt="2099-01-01 10:03:24", direction="long",
        close_dt="2099-01-01 10:45:32", timeframe="e1h",
        trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=orphan_trade_name,
        ts_id=ts_orphan.ts_id, bt_id=ts_orphan.bt_id))
    bt_orphan.tradeseries = [ts_orphan]
    store_backtests([bt_orphan],
                    include_tradeseries=True,
                    include_trades=True)
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      list_issues=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["orphaned_test_objects_errors"] >= 3
    issue_types = {
        x["object_type"] for x in r["integrity"]["orphaned_test_objects"]
    }
    assert {"backtest", "tradeseries", "trade"}.issubset(issue_types)
    clear_storage_by_name(orphan_backtest_name)
    clear_storage_by_name(orphan_ts_name)
    clear_storage_by_name(orphan_trade_name)

    # Confirm duplicate trades fail during Trade backtests_integrity check
    duplicate_name = f"{bt}_duplicate_fail"
    duplicate_trade_name = f"{bt}_duplicate_trade"
    cleanup_dhstore_storage(duplicate_name, duplicate_trade_name)
    ts_fail = TradeSeries(start_dt="2099-01-06 00:00:00",
                          end_dt="2099-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=duplicate_name,
                          params_str="",
                          ts_id=duplicate_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-06 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=duplicate_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-06 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=duplicate_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    # While we stored 2 trades, the second should have overwritten the first
    # by design.  Therefor we only see the second Trade in storage
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    assert stored[0].open_dt == "2099-01-06 10:03:24"
    assert stored[0].close_dt == "2099-01-06 12:32:15"
    # Store the trades
    store_trades([ts_fail.trades[0]])
    stored = get_trades_by_field(field="bt_id", value=bt)
    # Now we should still have only 1 trade, but it's the first not the second
    # this time due to another overwrite
    assert len(stored) == 1
    assert stored[0].open_dt == "2099-01-06 10:03:24"
    assert stored[0].close_dt == "2099-01-06 11:45:32"
    # Change the name attribute to allow it to store itself without replacing
    ts_fail.trades[0].name = duplicate_trade_name
    store_trades([ts_fail.trades[0]])
    # Confirm integrity check now fails due to duplicate trade
    r = review_trades(bt_id=bt, check_integrity=True, orphan_ok=[bt])
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == ["1 duplicate trades found"]
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
