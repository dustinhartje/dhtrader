import site
site.addsitedir('modulepaths')
import dhstore as dhs
from dhtrades import TradeSeries, Trade
# TODO Go through dhstore.py every function/class and write out comments
#      here for things that need testing
# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO Check that list against below tests that were transfered from my
#      original manual testing hacks
# TODO write any remaining tests needed here or in class specific files
# TODO tests needed
# test_basics() review and make sure anything it was doing previously is
#     covered, copying out any useful code before recreating
# list_collections() Ensure it lists a few that should always exist like
#     candles, and probably check that it's length isn't over like 50?
#     -- probably combine with drop_collection() using a dummy test collection
# drop_collection() test carefully by creating a DELETEME collection then
#     dropping it and confirming it's no longer in list
#     --should this even exist in dhstore?  It's kind of mongo specific
#       if anything maybe it should be drop_table or similarly generic
#       and if I keep it shoudl I also have list_tables?  maybe call it stores?
#       buckets?  thingboxes?
# get_symbol_by_ticker() yeah better test that this spits out the right
#     thing with the right type and all that
# combined tests for store_, list_*, review_* get_, delete_ each object type
#     No need to combine linked types in tests, just do each type indivdidually
#     Linked tests should be done at class levels
#     --Trades
#     --TradeSeries
#     --Backtests
#     --Candles
#     --Indicators
#     --IndicatorDataPoints
#     --Events
# mock failures for remaining integrity checks to ensure they will fail
#     if there is ever a real problem.  see test_TradeSeries_integrity_check()
#     for an example below
#   --review_candles(check_integrity) - see if I can safely insert and delete
#     individual candles, and if so I can insert improper candles and
#     potentially remove to check for gaps.  I would want to create a test
#     copy of the candles collection to do this on though, don't muck with
#     the real candles.  I can probably just copy a few days worth of
#     candles into a test collection on the fly right?  should go fast...
#   --review_trades(check_integrity) checks for duplicate trades, should also
#     be able to run this against a (potential) copy of the collection or
#     just build a mockup collection on the fly with a small number of
#     Trades, since dupe check is pretty basic


def clear_storage_by_bt_id(bt_id):
    dhs.delete_backtests(symbol="ES", field="bt_id", value=bt_id,
                         include_tradeseries=True, include_trades=True)
    r = dhs.get_backtests_by_field(field="bt_id", value=bt_id)
    assert len(r) == 0
    r = dhs.get_tradeseries_by_field(field="bt_id", value=bt_id)
    assert len(r) == 0
    r = dhs.get_trades_by_field(field="bt_id", value=bt_id)
    assert len(r) == 0


def test_TradeSeries_integrity_check():
    """Constructs objects that should fail storage integrity check requirements
    to ensure checks will fail as expected if problems arise in storage"""

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
    ts_good.store(store_trades=True)
    stored = dhs.get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = dhs.get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm storage integrity check against test bt_id passes
    r = dhs.review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None
    # Clean from storage and verify gone
    ts_good.delete_from_storage(include_trades=True)
    stored = dhs.get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = dhs.get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with multiple Trades opening in the same bar
    ts_good = TradeSeries(start_dt="2025-01-05 00:00:00",
                          end_dt="2025-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_same_bar_fail",
                          params_str="",
                          ts_id=f"{bt}_same_bar_fail",
                          bt_id=bt)
    ts_good.add_trade(Trade(
        open_dt="2025-01-05 10:03:24", direction="long",
        close_dt="2025-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.add_trade(Trade(
        open_dt="2025-01-05 10:23:25", direction="long",
        close_dt="2025-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.store(store_trades=True)
    stored = dhs.get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = dhs.get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm storage integrity check fails test bt_id fails
    r = dhs.review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {'trade_overlaps': [
        {'issue_type': 'Trade timeframe bar overlap',
         'ts_id': 'DELETEME_TRADESERIES_INTEGRITY_TEST_same_bar_fail',
         'timeframe': 'e1h',
         'trade_open': '2025-01-05 10:23:25',
         'trade_open_tf': '2025-01-05 10:00:00',
         'prev_trade_open': '2025-01-05 10:03:24',
         'prev_trade_close': '2025-01-05 10:45:32',
         'prev_trade_close_tf': '2025-01-05 10:00:00'}]}
    # Clean from storage and verify gone
    ts_good.delete_from_storage(include_trades=True)
    stored = dhs.get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = dhs.get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with a Trade opening in the same bar that the prior
    # Trade closed in, after running past the original opening bar
    ts_good = TradeSeries(start_dt="2025-01-05 00:00:00",
                          end_dt="2025-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=f"{bt}_next_bar_fail",
                          params_str="",
                          ts_id=f"{bt}_next_bar_fail",
                          bt_id=bt)
    ts_good.add_trade(Trade(
        open_dt="2025-01-05 10:03:24", direction="long",
        close_dt="2025-01-05 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.add_trade(Trade(
        open_dt="2025-01-05 11:53:18", direction="long",
        close_dt="2025-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.store(store_trades=True)
    stored = dhs.get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = dhs.get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm storage integrity check against test bt_id fails
    r = dhs.review_tradeseries(bt_id=bt, check_integrity=True)
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
    ts_good.delete_from_storage(include_trades=True)
    stored = dhs.get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = dhs.get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Cleanup storage by bt_id in case anything was left behind somehow
    clear_storage_by_bt_id(bt)
