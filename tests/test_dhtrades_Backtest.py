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
    if chart_tf is None:
        assert r.chart_tf is None
    else:
        assert isinstance(r.chart_tf, dhc.Chart)
    if chart_1m is None:
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
    bt.add_tradeseries(ts)
    # With TradeSeries and Trdes shown
    assert len(bt.pretty(suppress_tradeseries=False,
                         suppress_trades=False).splitlines()) == 59


def test_Backtest_load_charts():
    bt = create_backtest(start_dt="2025-01-01 00:00:00",
                         end_dt="2025-01-08 00:00:00",
                         timeframe="e1h",
                         trading_hours="eth")
    # Confirm charts have not been populated
    assert bt.chart_1m is None
    assert bt.chart_tf is None
    # Load and confirm number of candles as expected in both chart types
    bt.load_charts()
    assert len(bt.chart_1m.c_candles) == 5881
    assert len(bt.chart_tf.c_candles) == 99


def test_Backtest_add_tradeseries_and_trades():
    bt = create_backtest()
    assert isinstance(bt, dht.Backtest)
    assert len(bt.tradeseries) == 0
    ts = create_tradeseries()
    assert isinstance(ts, dht.TradeSeries)
    assert len(ts.trades) == 0
    # TradeSeries should not have a bt_id yet
    assert ts.bt_id is None
    tr = create_trade()
    assert isinstance(tr, dht.Trade)
    # Trade should not have a ts_id or bt_id yet
    assert tr.ts_id is None
    assert tr.bt_id is None
    ts.add_trade(tr)
    assert len(ts.trades) == 1
    assert isinstance(ts.trades[0], dht.Trade)
    # After adding to TradeSeries, Trade should now have TradeSerie's ts_id
    assert tr.ts_id == ts.ts_id
    bt.add_tradeseries(ts)
    assert len(bt.tradeseries) == 1
    assert isinstance(bt.tradeseries[0], dht.TradeSeries)
    # TradeSeries and Trade should now have Backtest's bt_id
    assert ts.bt_id == bt.bt_id
    assert tr.bt_id == bt.bt_id


def test_Backtest_store_retrieve_load_tradeseries_and_delete():
    # Create and link Backtest, TradeSeries, and Trade objects
    tr = create_trade()
    ts = create_tradeseries()
    ts.add_trade(tr)
    bt = create_backtest()
    bt.add_tradeseries(ts)

    # Clear and confirm storage has no objects with this name currently
    dhs.delete_backtests(symbol="ES", field="name", value="DELETEME-TEST")
    s_bt = dhs.get_backtests_by_field(field="name", value="DELETEME-TEST")
    assert len(s_bt) == 0
    dhs.delete_tradeseries(symbol="ES", field="name", value="DELETEME-TEST")
    s_ts = dhs.get_tradeseries_by_field(field="name", value="DELETEME-TEST")
    assert len(s_ts) == 0
    dhs.delete_trades(symbol="ES", field="name", value="DELETEME-TEST")
    s_tr = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(s_tr) == 0

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
