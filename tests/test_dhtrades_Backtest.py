import datetime
import site
import pytest
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhtrades as dht
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str
import dhstore as dhs

# NOTE No tests are needed on the base Backtest() class for .calculate() or
#      .incorporate_parameters() methods as these are only implemented when
#      building subclasses
# TODO loop through all passable attributes and ensure assertions cover
#      whether they are passed, not passed, or passed as different obj types
#      where applicable (i.e. symbol as str or Symbol)
#      TODO I may not have fully covered this on other objects so duplicate
#           this TODO for all others as well
# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO confirm no other TODOs remain in this file before clearing this one

# TODO Tests needed (some of these have already been written partially/fully
# Backtest review __init__ and make sure I've covered all attributes with type
#          and value test, as well as any calculations or scenarios where wrong
#          things might get passed in or various flags might change behavior
#          -- perhaps this should have it's own test to be clear and found
#             easily for future updates?  even if all it does is call create_*
# Backtest __eq__ and __ne__ pass and fail scenarios
# Backtest __str__ and __repr__ return strings successfully
# Backtest to.json and to_clean_dict  return correct types and mock values
# BacktestIndicatorTag (backtesting repo) - needs basically everything in
#     this file plus specific tests for calculate, subs_*, parameters, and
#     anything else it does different


def create_trade(open_dt="2025-01-02 12:00:00",
                 direction="long",
                 timeframe="5m",
                 trading_hours="rth",
                 entry_price=5000,
                 stop_ticks=20,
                 stop_target=4995,
                 prof_ticks=20,
                 prof_target=5005,
                 name="DELETEME"
                 ):
    return dht.Trade(open_dt=open_dt,
                     direction=direction,
                     timeframe=timeframe,
                     trading_hours=trading_hours,
                     entry_price=entry_price,
                     stop_ticks=stop_ticks,
                     stop_target=stop_target,
                     prof_ticks=prof_ticks,
                     prof_target=prof_target,
                     name=name,
                     )


def create_tradeseries(start_dt="2025-01-01 00:00:00",
                       end_dt="2025-02-01 00:00:00",
                       timeframe="5m",
                       trading_hours="rth",
                       symbol="ES",
                       name="DELETEME",
                       params_str="a1_b2_c3_p0",
                       ts_id=None,
                       bt_id=None,
                       trades=None,
                       ):
    return dht.TradeSeries(start_dt=start_dt,
                           end_dt=end_dt,
                           timeframe=timeframe,
                           trading_hours=trading_hours,
                           symbol=symbol,
                           name=name,
                           params_str=params_str,
                           ts_id=ts_id,
                           bt_id=bt_id,
                           trades=trades,
                           )


def create_backtest(start_dt="2025-01-01 00:00:00",
                    end_dt="2025-02-01 00:00:00",
                    timeframe="e1h",
                    trading_hours="eth",
                    symbol="ES",
                    name="DELETEME",
                    parameters={"a": 1, "b": "two"},
                    bt_id=None,
                    class_name="BacktestTestDeleteme",
                    chart_tf=None,
                    chart_1m=None,
                    autoload_charts=False,
                    tradeseries=None,
                    ):
    r = dht.Backtest(start_dt=start_dt,
                     end_dt=end_dt,
                     timeframe=timeframe,
                     trading_hours=trading_hours,
                     symbol=symbol,
                     name=name,
                     parameters=parameters,
                     bt_id=bt_id,
                     class_name=class_name,
                     chart_tf=chart_tf,
                     chart_1m=chart_1m,
                     autoload_charts=autoload_charts,
                     tradeseries=tradeseries,
                     )
    assert isinstance(r.start_dt, str)
    assert r.start_dt == start_dt
    assert isinstance(r.end_dt, str)
    assert r.end_dt == end_dt
    # start_dt and end_dt should be convertible to datetime objects
    assert isinstance(dt_as_dt(r.start_dt), datetime.datetime)
    assert isinstance(dt_as_dt(r.end_dt), datetime.datetime)
    assert isinstance(r.timeframe, str)
    assert r.timeframe == timeframe
    assert isinstance(r.name, str)
    assert r.name == name
    assert isinstance(r.parameters, dict)
    assert r.parameters == parameters
    if bt_id is None:
        assert isinstance(r.bt_id, str)
        assert name in r.bt_id
    else:
        assert r.bt_id == bt_id
    assert isinstance(r.class_name, str)
    assert r.class_name == class_name
    if chart_tf is None and autoload_charts is False:
        assert r.chart_tf is None
    else:
        assert isinstance(r.chart_tf, dhc.Chart)
    if chart_1m is None and autoload_charts is False:
        assert r.chart_1m is None
    else:
        assert isinstance(r.chart_1m, dhc.Chart)

    # Calculated and adjusted attributes
    # symbol should get converted to a Symbol object
    assert isinstance(r.symbol, dhc.Symbol)
    # If symbol was passed as a string, it should now be the ticker
    if isinstance(symbol, str):
        assert r.symbol.ticker == symbol
    assert isinstance(r.tradeseries, list)
    return r


def clear_storage_by_name(name: str):
    """Delete all Backtests, TradeSeries, and Trades with the given name from
    central storage"""
    dhs.delete_backtests(symbol="ES", field="name", value=name)
    s_bt = dhs.get_backtests_by_field(field="name", value=name)
    assert len(s_bt) == 0
    dhs.delete_tradeseries(symbol="ES", field="name", value=name)
    s_ts = dhs.get_tradeseries_by_field(field="name", value=name)
    assert len(s_ts) == 0
    dhs.delete_trades(symbol="ES", field="name", value=name)
    s_tr = dhs.get_trades_by_field(field="name", value=name)
    assert len(s_tr) == 0


def test_Backtest_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    bt = create_backtest()
    assert isinstance(bt, dht.Backtest)
    ts = create_tradeseries()
    assert isinstance(ts, dht.TradeSeries)
    tr = create_trade()
    assert isinstance(tr, dht.Trade)
    assert len(bt.pretty().splitlines()) == 21
    ts.add_trade(tr)
    bt.update_tradeseries(ts)
    # With TradeSeries and Trdes shown
    assert len(bt.pretty(suppress_tradeseries=False,
                         suppress_trades=False).splitlines()) == 59


def test_Backtest_load_charts():
    bt = create_backtest(start_dt="2025-01-01 00:00:00",
                         end_dt="2025-01-07 17:30:00",
                         timeframe="e1h",
                         trading_hours="eth")
    # Confirm charts have not been populated
    assert bt.chart_1m is None
    assert bt.chart_tf is None
    # Load and confirm number of candles as expected in both chart types
    bt.load_charts()
    assert len(bt.chart_1m.c_candles) == 5520
    assert len(bt.chart_tf.c_candles) == 92
    # Confirm start_dt and end_dt were updated based on 1m candle availability
    assert bt.start_dt == "2025-01-01 18:00:00"
    assert bt.end_dt == "2025-01-07 16:59:00"


def test_Backtest_restrict_dates():
    test_name = "DELETEME-RESTRICTTest"
    bt = create_backtest(name=test_name,
                         start_dt="2025-01-01 18:00:00",
                         end_dt="2025-01-31 16:59:00",
                         timeframe="e1h",
                         trading_hours="eth",
                         autoload_charts=True,
                         )
    ts1 = create_tradeseries(name=test_name,
                             start_dt="2025-01-01 18:00:00",
                             end_dt="2025-01-31 16:59:00",
                             timeframe="e1h",
                             trading_hours="eth",
                             params_str="a1_b2_c3_p0",
                             )
    ts1.add_trade(create_trade(open_dt="2025-01-02 12:00:00",
                               timeframe="e1h",
                               trading_hours="eth",
                               name=test_name
                               ))
    ts1.add_trade(create_trade(open_dt="2025-01-12 12:00:00",
                               timeframe="e1h",
                               trading_hours="eth",
                               name=test_name
                               ))
    ts1.add_trade(create_trade(open_dt="2025-01-20 12:00:00",
                               timeframe="e1h",
                               trading_hours="eth",
                               name=test_name
                               ))
    bt.update_tradeseries(ts1)
    assert bt.start_dt == "2025-01-01 18:00:00"
    assert bt.end_dt == "2025-01-31 16:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 3
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-02 12:00:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-12 12:00:00"
    assert bt.tradeseries[0].trades[2].open_dt == "2025-01-20 12:00:00"
    assert bt.chart_tf.earliest_candle == "2025-01-01 18:00:00"
    assert bt.chart_tf.latest_candle == "2025-01-31 16:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-01 18:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-31 16:59:00"
    # Confirm trying to expand dates raises an Exception
    with pytest.raises(ValueError):
        bt.restrict_dates(new_start_dt="2024-12-15 00:00:00",
                          new_end_dt="2025-01-31 16:59:00",
                          update_storage=False,
                          )
    with pytest.raises(ValueError):
        bt.restrict_dates(new_start_dt="2025-01-01 18:00:00",
                          new_end_dt="2025-02-12 16:59:00",
                          update_storage=False,
                          )
    with pytest.raises(ValueError):
        bt.restrict_dates(new_start_dt="2024-12-15 00:00:00",
                          new_end_dt="2025-02-12 16:59:00",
                          update_storage=False,
                          )
    # Make sure nothing actually changed
    assert bt.start_dt == "2025-01-01 18:00:00"
    assert bt.end_dt == "2025-01-31 16:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 3
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-02 12:00:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-12 12:00:00"
    assert bt.tradeseries[0].trades[2].open_dt == "2025-01-20 12:00:00"
    assert bt.chart_tf.earliest_candle == "2025-01-01 18:00:00"
    assert bt.chart_tf.latest_candle == "2025-01-31 16:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-01 18:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-31 16:59:00"
    # TODO check into why BacktestIndTag runs __init__ multiple times,
    #      does this happen with Backtest also?  this is slowing things
    #      way down I'm sure...
    # TODO Run restrict_dates using the original dates
    # TODO Confirm nothing actually changed again
    # TODO Revisit code and review what would happen if interrupted.  I would
    #      like it to always adjust dates on both backtest and tradeseries
    #      even if they already match and also kill any out of bounds trades
    #      rather than exit if the dates already match.  Write a test for this
    #      scenario also (will need to manually adjust the tradeseries
    #      and add a trade out of bounds to simulate a failed run then run
    #      again with the same/original dates

    # Confirm restricting start date a little works with no trade change
    bt.restrict_dates(new_start_dt="2025-01-01 22:00:00",
                      new_end_dt="2025-01-31 16:59:00",
                      update_storage=False,
                      )
    assert bt.start_dt == "2025-01-01 22:00:00"
    assert bt.end_dt == "2025-01-31 16:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 3
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-02 12:00:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-12 12:00:00"
    assert bt.tradeseries[0].trades[2].open_dt == "2025-01-20 12:00:00"
    assert bt.chart_tf.earliest_candle == "2025-01-01 22:00:00"
    assert bt.chart_tf.latest_candle == "2025-01-31 16:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-01 22:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-31 16:59:00"
    # Confirm restring start date a lot eliminates the first trade
    bt.restrict_dates(new_start_dt="2025-01-03 12:00:00",
                      new_end_dt="2025-01-31 16:59:00",
                      update_storage=False,
                      )
    assert bt.start_dt == "2025-01-03 12:00:00"
    assert bt.end_dt == "2025-01-31 16:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 2
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-12 12:00:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-20 12:00:00"
    assert bt.chart_tf.earliest_candle == "2025-01-03 12:00:00"
    assert bt.chart_tf.latest_candle == "2025-01-31 16:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-03 12:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-31 16:59:00"

    # Confirm restricting end date a little works with no trade change
    bt.restrict_dates(new_start_dt="2025-01-03 12:00:00",
                      new_end_dt="2025-01-31 14:59:00",
                      update_storage=False,
                      )
    assert bt.start_dt == "2025-01-03 12:00:00"
    assert bt.end_dt == "2025-01-31 14:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 2
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-12 12:00:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-20 12:00:00"
    assert bt.chart_tf.earliest_candle == "2025-01-03 12:00:00"
    assert bt.chart_tf.latest_candle == "2025-01-31 14:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-03 12:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-31 14:59:00"
    # Confirm restring start date a lot eliminates the last trade
    bt.restrict_dates(new_start_dt="2025-01-03 12:00:00",
                      new_end_dt="2025-01-18 14:59:00",
                      update_storage=False,
                      )
    assert bt.start_dt == "2025-01-03 12:00:00"
    assert bt.end_dt == "2025-01-18 14:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 1
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-12 12:00:00"
    assert bt.chart_tf.earliest_candle == "2025-01-03 12:00:00"
    # 2025-01-18 was a Saturday so latest candles are on Fri the 17th at close
    assert bt.chart_tf.latest_candle == "2025-01-17 16:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-03 12:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-17 16:59:00"
    # Confirm restricting both dates works and eliminates the final trade
    bt.restrict_dates(new_start_dt="2025-01-08 12:00:00",
                      new_end_dt="2025-01-10 14:59:00",
                      update_storage=False,
                      )
    assert bt.start_dt == "2025-01-08 12:00:00"
    assert bt.end_dt == "2025-01-10 14:59:00"
    assert len(bt.tradeseries) == 1
    assert len(bt.tradeseries[0].trades) == 0
    assert bt.chart_tf.earliest_candle == "2025-01-08 12:00:00"
    # 2025-01-18 was a saturday so latest candles are on Fri the 17th at close
    assert bt.chart_tf.latest_candle == "2025-01-10 14:00:00"
    assert bt.chart_1m.earliest_candle == "2025-01-08 12:00:00"
    assert bt.chart_1m.latest_candle == "2025-01-10 14:59:00"

    # ###############################################################
    # Reset, store, and repeat updates with storage verification this time

    # Clear and confirm storage has no objects with this name currently
    clear_storage_by_name(name=test_name)
    # Create a Backtest with 1 TradeSeries and 3 Trades
    bt = None
    bt = create_backtest(name=test_name,
                         start_dt="2025-01-01 18:00:00",
                         end_dt="2025-01-31 16:59:00",
                         timeframe="e1h",
                         trading_hours="eth",
                         autoload_charts=True,
                         )
    ts1 = create_tradeseries(name=test_name,
                             start_dt="2025-01-01 18:00:00",
                             end_dt="2025-01-31 16:59:00",
                             timeframe="e1h",
                             trading_hours="eth",
                             params_str="a1_b2_c3_p0",
                             )
    ts1.add_trade(create_trade(open_dt="2025-01-02 12:00:00",
                               timeframe="e1h",
                               trading_hours="eth",
                               name=test_name
                               ))
    ts1.add_trade(create_trade(open_dt="2025-01-12 12:00:00",
                               timeframe="e1h",
                               trading_hours="eth",
                               name=test_name
                               ))
    ts1.add_trade(create_trade(open_dt="2025-01-20 12:00:00",
                               timeframe="e1h",
                               trading_hours="eth",
                               name=test_name
                               ))
    bt.update_tradeseries(ts1)
    bt.store()
    bt_load = dhs.get_backtests_by_field(field="bt_id",
                                         value=bt.bt_id)[0]
    assert bt_load["start_dt"] == "2025-01-01 18:00:00"
    assert bt_load["end_dt"] == "2025-01-31 16:59:00"
    ts_load = dhs.get_tradeseries_by_field(field="bt_id",
                                           value=bt.bt_id,
                                           include_trades=True)

    assert len(ts_load) == 1
    ts = ts_load[0]
    assert ts.start_dt == "2025-01-01 18:00:00"
    assert ts.end_dt == "2025-01-31 16:59:00"
    assert len(ts.trades) == 3
    assert ts.trades[0].open_dt == "2025-01-02 12:00:00"
    assert ts.trades[1].open_dt == "2025-01-12 12:00:00"
    assert ts.trades[2].open_dt == "2025-01-20 12:00:00"
    # Update only start_dt, eliminating the first trade
    bt.restrict_dates(new_start_dt="2025-01-03 12:00:00",
                      new_end_dt="2025-01-31 16:59:00",
                      update_storage=True,
                      )
    bt_load = dhs.get_backtests_by_field(field="bt_id",
                                         value=bt.bt_id)[0]
    assert bt_load["start_dt"] == "2025-01-03 12:00:00"
    assert bt_load["end_dt"] == "2025-01-31 16:59:00"
    ts_load = dhs.get_tradeseries_by_field(field="bt_id",
                                           value=bt.bt_id,
                                           include_trades=True)
    assert len(ts_load) == 1
    ts = ts_load[0]
    assert ts.start_dt == "2025-01-03 12:00:00"
    assert ts.end_dt == "2025-01-31 16:59:00"
    assert len(bt.tradeseries[0].trades) == 2
    assert ts.trades[0].open_dt == "2025-01-12 12:00:00"
    assert ts.trades[1].open_dt == "2025-01-20 12:00:00"
    # Update only end_dt, eliminating the last
    bt.restrict_dates(new_start_dt="2025-01-03 12:00:00",
                      new_end_dt="2025-01-18 14:59:00",
                      update_storage=True,
                      )
    bt_load = dhs.get_backtests_by_field(field="bt_id",
                                         value=bt.bt_id)[0]
    assert bt_load["start_dt"] == "2025-01-03 12:00:00"
    assert bt_load["end_dt"] == "2025-01-18 14:59:00"
    ts_load = dhs.get_tradeseries_by_field(field="bt_id",
                                           value=bt.bt_id,
                                           include_trades=True)
    assert len(ts_load) == 1
    ts = ts_load[0]
    assert ts.start_dt == "2025-01-03 12:00:00"
    assert ts.end_dt == "2025-01-18 14:59:00"
    assert len(bt.tradeseries[0].trades) == 1
    assert ts.trades[0].open_dt == "2025-01-12 12:00:00"
    # Reduce both, eliminating final trade
    bt.restrict_dates(new_start_dt="2025-01-08 12:00:00",
                      new_end_dt="2025-01-10 14:59:00",
                      update_storage=True,
                      )
    bt_load = dhs.get_backtests_by_field(field="bt_id",
                                         value=bt.bt_id)[0]
    assert bt_load["start_dt"] == "2025-01-08 12:00:00"
    assert bt_load["end_dt"] == "2025-01-10 14:59:00"
    ts_load = dhs.get_tradeseries_by_field(field="bt_id",
                                           value=bt.bt_id,
                                           include_trades=True)
    assert len(ts_load) == 1
    ts = ts_load[0]
    assert ts.start_dt == "2025-01-08 12:00:00"
    assert ts.end_dt == "2025-01-10 14:59:00"
    assert len(bt.tradeseries[0].trades) == 0
    trades = dhs.get_trades_by_field(field="bt_id",
                                     value=bt.bt_id)
    assert len(trades) == 0

    # Clear and confirm storage has no objects with this name currently
    clear_storage_by_name(name=test_name)


def test_Backtest_add_and_remove_tradeseries_and_trades():
    test_name = "DELETEME-ARTSTest"
    ts1_name = "".join([test_name, "1"])
    ts1_ts_id = "".join([test_name, "1_a1_b2_c3_p0"])
    ts2_name = "".join([test_name, "2"])
    ts2_ts_id = "".join([test_name, "2_a1_b2_c3_p0"])
    bt = create_backtest(name=test_name)
    assert isinstance(bt, dht.Backtest)
    assert len(bt.tradeseries) == 0
    ts1 = create_tradeseries(name=ts1_name,
                             start_dt="2025-01-05 10:00:00",
                             end_dt="2025-01-05 14:00:00",
                             )
    assert isinstance(ts1, dht.TradeSeries)
    assert len(ts1.trades) == 0
    # TradeSeries should not have a bt_id yet
    assert ts1.bt_id is None
    # Create and add 2 Trades
    for dt in ["2025-01-05 12:00:00", "2025-01-05 13:00:00"]:
        tr = create_trade(open_dt=dt, name=test_name)
        assert isinstance(tr, dht.Trade)
        # Trade should not have a ts_id or bt_id yet
        assert tr.ts_id is None
        assert tr.bt_id is None
        ts1.add_trade(tr)
        # After adding to TradeSeries, Trade should now have TradeSeries ts_id
        assert tr.ts_id == ts1.ts_id
    assert len(ts1.trades) == 2
    assert isinstance(ts1.trades[0], dht.Trade)
    assert isinstance(ts1.trades[1], dht.Trade)
    # Add the TradeSeries to the Backtest
    bt.update_tradeseries(ts1)
    assert len(bt.tradeseries) == 1
    assert isinstance(bt.tradeseries[0], dht.TradeSeries)
    # TradeSeries and Trade should now have Backtest's bt_id
    assert ts1.bt_id == bt.bt_id
    for tr in ts1.trades:
        assert tr.bt_id == bt.bt_id
    # Create and add a second TradeSeries
    ts2 = create_tradeseries(name=ts2_name,
                             start_dt="2025-01-06 13:00:00",
                             end_dt="2025-01-06 16:00:00",
                             )
    for dt in ["2025-01-06 14:00:00", "2025-01-06 15:00:00"]:
        tr = create_trade(open_dt=dt, name=test_name)
        ts2.add_trade(tr)
    bt.update_tradeseries(ts2)
    assert isinstance(bt.tradeseries[1], dht.TradeSeries)
    # Confirm both TradeSeries and Trades attached as expected
    assert len(bt.tradeseries) == 2
    assert bt.tradeseries[0].ts_id == ts1_ts_id
    assert bt.tradeseries[0].start_dt == "2025-01-05 10:00:00"
    assert bt.tradeseries[0].end_dt == "2025-01-05 14:00:00"
    assert bt.tradeseries[1].ts_id == ts2.ts_id
    assert bt.tradeseries[1].start_dt == "2025-01-06 13:00:00"
    assert bt.tradeseries[1].end_dt == "2025-01-06 16:00:00"
    assert len(bt.tradeseries[0].trades) == 2
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-05 12:00:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-05 13:00:00"
    assert len(bt.tradeseries[1].trades) == 2
    assert bt.tradeseries[1].trades[0].open_dt == "2025-01-06 14:00:00"
    assert bt.tradeseries[1].trades[1].open_dt == "2025-01-06 15:00:00"
    # Clear and confirm storage has no objects with these name currently
    # in case previous tests failed and left orphans
    dhs.delete_backtests(symbol="ES", field="name", value=test_name)
    s_bt = dhs.get_backtests_by_field(field="name", value=test_name)
    assert len(s_bt) == 0
    dhs.delete_tradeseries(symbol="ES", field="name", value=ts1_name)
    s_ts = dhs.get_tradeseries_by_field(field="name", value=ts1_name)
    assert len(s_ts) == 0
    dhs.delete_tradeseries(symbol="ES", field="name", value=ts2_name)
    s_ts = dhs.get_tradeseries_by_field(field="name", value=ts2_name)
    assert len(s_ts) == 0
    dhs.delete_trades(symbol="ES", field="name", value=test_name)
    s_tr = dhs.get_trades_by_field(field="name", value=test_name)
    assert len(s_tr) == 0
    # Store the backtest
    bt.store(store_tradeseries=True, store_trades=True)

    # Modify and replace the 1st tradeseries, including storage update
    ts1.start_dt = "2025-01-05 08:30:00"
    ts1.end_dt = "2025-01-05 16:30:00"
    ts1.trades[0].open_dt = "2025-01-05 11:30:00"
    ts1.trades[1].open_dt = "2025-01-05 12:30:00"
    bt.update_tradeseries(ts1)
    # Confirm previous tradeseries no longer in storage since it was modified
    # but not yet stored again.  Only the second (unmodified) TradeSeries
    # should be retrievable at this stage.
    s_ts = dhs.get_tradeseries_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_ts) == 1
    assert s_ts[0].name == ts2_name
    # Confirm backtest updated and no dupes
    assert len(bt.tradeseries) == 2
    assert bt.tradeseries[0].ts_id == ts1_ts_id
    assert bt.tradeseries[0].start_dt == "2025-01-05 08:30:00"
    assert bt.tradeseries[0].end_dt == "2025-01-05 16:30:00"
    assert len(bt.tradeseries[0].trades) == 2
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-05 11:30:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-05 12:30:00"
    assert bt.tradeseries[1].ts_id == ts2_ts_id
    assert bt.tradeseries[1].start_dt == "2025-01-06 13:00:00"
    assert bt.tradeseries[1].end_dt == "2025-01-06 16:00:00"
    assert len(bt.tradeseries[1].trades) == 2
    assert bt.tradeseries[1].trades[0].open_dt == "2025-01-06 14:00:00"
    assert bt.tradeseries[1].trades[1].open_dt == "2025-01-06 15:00:00"
    # Store the backtest again, which should replace itself, it's TradeSeries,
    # and their Trades without duplication occurring
    bt.store(store_tradeseries=True, store_trades=True)
    # Get backtests with this name from storage
    s_bt = dhs.get_backtests_by_field(field="name", value=test_name)
    # Confirm exactly 1 backtest found in storage with this name
    assert len(s_bt) == 1
    # Get TradeSeries by bt_id from storage
    s_ts = dhs.get_tradeseries_by_field(field="bt_id", value=bt.bt_id)
    # Confirm exactly 2 TradeSeries in storage
    assert len(s_ts) == 2
    # Get TradeSeries by name from storage
    s_ts1 = dhs.get_tradeseries_by_field(field="name", value=ts1_name)
    s_ts2 = dhs.get_tradeseries_by_field(field="name", value=ts2_name)
    # Confirm exactly 1 of each tradeseries by name
    assert len(s_ts1) == 1
    assert len(s_ts2) == 1
    # Confirm expected start_dt and end_dt for each TradeSeries
    # ts1 should have been modified in storage after update
    assert s_ts1[0].start_dt == "2025-01-05 08:30:00"
    assert s_ts1[0].end_dt == "2025-01-05 16:30:00"
    # ts2 should retain the original values in storage as it was not updated
    assert s_ts2[0].start_dt == "2025-01-06 13:00:00"
    assert s_ts2[0].end_dt == "2025-01-06 16:00:00"
    # Confirm exactly 4 trades in storage by name and bt_id
    s_tr = dhs.get_trades_by_field(field="name", value=test_name)
    assert len(s_tr) == 4
    s_tr = dhs.get_trades_by_field(field="bt_id", value=test_name)
    assert len(s_tr) == 4
    # Confirm exactly 2 Trades in storage by each TradeSeries ts_id
    s_tr1 = dhs.get_trades_by_field(field="ts_id", value=ts1_ts_id)
    assert len(s_tr1) == 2
    s_tr2 = dhs.get_trades_by_field(field="ts_id", value=ts2_ts_id)
    assert len(s_tr2) == 2
    # Confirm Trade open_dt values as expected in storage, with ts1 updated
    # and ts2 retaining original unmodified values
    assert s_tr1[0].open_dt == "2025-01-05 11:30:00"
    assert s_tr1[1].open_dt == "2025-01-05 12:30:00"
    assert s_tr2[0].open_dt == "2025-01-06 14:00:00"
    assert s_tr2[1].open_dt == "2025-01-06 15:00:00"

    # Modify and replace the 2nd tradeseries, without storage update
    ts2.start_dt = "2025-01-06 09:30:00"
    ts2.end_dt = "2025-01-06 18:30:00"
    ts2.trades[0].open_dt = "2025-01-06 14:30:00"
    ts2.trades[1].open_dt = "2025-01-06 15:30:00"
    bt.update_tradeseries(ts2, clear_storage=False)
    # Confirm backtest updated and no duplication happened in TradeSeries or
    # Trades
    # This also confirms that .sort_tradeseries() is working as the update
    # would have added *ARTS1* on the end of the list during the update, then
    # .update_tradeseries() runs .sort_tradeseries() which sorts them
    # alphabetically by ts_id
    assert len(bt.tradeseries) == 2
    assert bt.tradeseries[0].ts_id == ts1_ts_id
    assert bt.tradeseries[0].start_dt == "2025-01-05 08:30:00"
    assert bt.tradeseries[0].end_dt == "2025-01-05 16:30:00"
    assert bt.tradeseries[1].ts_id == ts2_ts_id
    assert bt.tradeseries[1].start_dt == "2025-01-06 09:30:00"
    assert bt.tradeseries[1].end_dt == "2025-01-06 18:30:00"
    assert len(bt.tradeseries[0].trades) == 2
    assert bt.tradeseries[0].trades[0].open_dt == "2025-01-05 11:30:00"
    assert bt.tradeseries[0].trades[1].open_dt == "2025-01-05 12:30:00"
    assert len(bt.tradeseries[1].trades) == 2
    assert bt.tradeseries[1].trades[0].open_dt == "2025-01-06 14:30:00"
    assert bt.tradeseries[1].trades[1].open_dt == "2025-01-06 15:30:00"
    # Confirm storage not updated, including Trades, and no dupes
    # All of the below should remain the same as before we modified TS #2
    # Get TradeSeries by bt_id from storage
    s_ts = dhs.get_tradeseries_by_field(field="bt_id", value=bt.bt_id)
    # Confirm exactly 2 TradeSeries in storage
    assert len(s_ts) == 2
    # Get TradeSeries by name from storage
    s_ts1 = dhs.get_tradeseries_by_field(field="name", value=ts1_name)
    s_ts2 = dhs.get_tradeseries_by_field(field="name", value=ts2_name)
    # Confirm exactly 1 of each tradeseries by name
    assert len(s_ts1) == 1
    assert len(s_ts2) == 1
    # Confirm expected start_dt and end_dt for each TradeSeries
    # ts1 should have been modified in storage after update
    assert s_ts1[0].start_dt == "2025-01-05 08:30:00"
    assert s_ts1[0].end_dt == "2025-01-05 16:30:00"
    # ts2 should retain the original values in storage as it was not updated
    assert s_ts2[0].start_dt == "2025-01-06 13:00:00"
    assert s_ts2[0].end_dt == "2025-01-06 16:00:00"
    # Confirm exactly 4 trades in storage by name and bt_id
    s_tr = dhs.get_trades_by_field(field="name", value=test_name)
    assert len(s_tr) == 4
    s_tr = dhs.get_trades_by_field(field="bt_id", value=test_name)
    assert len(s_tr) == 4
    # Confirm exactly 2 Trades in storage by each TradeSeries ts_id
    s_tr1 = dhs.get_trades_by_field(field="ts_id", value=ts1_ts_id)
    assert len(s_tr1) == 2
    s_tr2 = dhs.get_trades_by_field(field="ts_id", value=ts2_ts_id)
    assert len(s_tr2) == 2
    # Confirm Trade open_dt values as expected in storage, with ts1 updated
    # and ts2 retaining original unmodified values
    assert s_tr1[0].open_dt == "2025-01-05 11:30:00"
    assert s_tr1[1].open_dt == "2025-01-05 12:30:00"
    assert s_tr2[0].open_dt == "2025-01-06 14:00:00"
    assert s_tr2[1].open_dt == "2025-01-06 15:00:00"

    # Confirm remove_tradeseries() works directly on backtest, removing
    # TradeSeries and Trades from storage by default
    bt.remove_tradeseries(ts_id=ts2.ts_id)
    assert len(bt.tradeseries) == 1
    assert bt.tradeseries[0].ts_id == ts1.ts_id
    s_ts2 = dhs.get_tradeseries_by_field(field="name", value=ts2_name)
    assert len(s_ts2) == 0
    s_tr2 = dhs.get_trades_by_field(field="ts_id", value=ts2_ts_id)
    assert len(s_tr2) == 0

    # Delete everything from storage to cleanup
    dhs.delete_backtests(symbol="ES", field="name", value=test_name)
    s_bt = dhs.get_backtests_by_field(field="name", value=test_name)
    assert len(s_bt) == 0
    dhs.delete_tradeseries(symbol="ES", field="name", value=ts1_name)
    s_ts = dhs.get_tradeseries_by_field(field="name", value=ts1_name)
    assert len(s_ts) == 0
    dhs.delete_tradeseries(symbol="ES", field="name", value=ts2_name)
    s_ts = dhs.get_tradeseries_by_field(field="name", value=ts2_name)
    assert len(s_ts) == 0
    dhs.delete_trades(symbol="ES", field="name", value=test_name)
    s_tr = dhs.get_trades_by_field(field="name", value=test_name)
    assert len(s_tr) == 0


def test_Backtest_store_retrieve_load_tradeseries_and_delete():
    test_name = "DELETEME-STORELOADTest"
    # Create and link Backtest, TradeSeries, and Trade objects
    tr = create_trade()
    ts = create_tradeseries()
    ts.add_trade(tr)
    bt = create_backtest()
    bt.update_tradeseries(ts)

    # Clear and confirm storage has no objects with this name currently
    clear_storage_by_name(name=test_name)

    # Store using Backtest.store() method and confirm result looks ok via _ids
    r_bt = bt.store(store_tradeseries=True,
                    store_trades=True,
                    )
    assert len(r_bt["backtest"]) == 1
    assert r_bt["backtest"][0]["bt_id"] == bt.bt_id
    assert len(r_bt["tradeseries"]) == 1
    assert len(r_bt["tradeseries"][0]["tradeseries"]) == 1
    r_ts = r_bt["tradeseries"][0]["tradeseries"][0]
    assert r_ts["bt_id"] == bt.bt_id
    assert r_ts["ts_id"] == ts.ts_id
    # Yeah, there's a lot of nested returns going on here, I should
    # look into making this cleaner some day
    r_tr = r_bt["tradeseries"][0]["trades"][0][0]
    assert r_tr["bt_id"] == bt.bt_id
    assert r_tr["ts_id"] == ts.ts_id

    # Confirm all objects individually retrievable from storage via ts_id/bt_id
    s_bt = dhs.get_backtests_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_bt) == 1
    assert isinstance(s_bt[0], dict)
    s_ts = dhs.get_tradeseries_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_ts) == 1
    assert isinstance(s_ts[0], dht.TradeSeries)
    s_tr = dhs.get_trades_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_tr) == 1
    assert isinstance(s_tr[0], dht.Trade)

    # Attempt to rebuild Backtest from storage using .load_tradeseries() to
    # add TradeSeries and Trades
    s = s_bt[0]
    r_bt = create_backtest(start_dt=s["start_dt"],
                           end_dt=s["end_dt"],
                           timeframe=s["timeframe"],
                           trading_hours=s["trading_hours"],
                           symbol=s["symbol"],
                           name=s["name"],
                           bt_id=s["bt_id"],
                           class_name=s["class_name"],
                           parameters=s["parameters"],
                           )
    r_bt.load_tradeseries()
    # Confirm the rebuilt Backtest matches the original we created
    assert r_bt == bt

    # Delete test objects from storage using Backtest.delete_from_storage()
    r_bt.delete_from_storage(include_tradeseries=True, include_trades=True)

    # Confirm storage has no objects remaining by name, ts_id, or bt_id
    s_bt = dhs.get_backtests_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_bt) == 0
    s_ts = dhs.get_tradeseries_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_ts) == 0
    s_tr = dhs.get_trades_by_field(field="bt_id", value=bt.bt_id)
    assert len(s_tr) == 0

    s_bt = dhs.get_backtests_by_field(field="ts_id", value=ts.ts_id)
    assert len(s_bt) == 0
    s_ts = dhs.get_tradeseries_by_field(field="ts_id", value=ts.ts_id)
    assert len(s_ts) == 0
    s_tr = dhs.get_trades_by_field(field="ts_id", value=ts.ts_id)
    assert len(s_tr) == 0
