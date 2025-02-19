import datetime
import site
import pytest
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhtrades as dht
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str
import dhstore as dhs

# TODO Go through dhtrades.py every function/class and write out comments
#      here for things that need testing
# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO Check that list against below tests that were transfered from my
#      original manual testing hacks
# TODO write any remaining tests needed here or in class specific files
# TODO confirm no other TODOs remain in this file before clearing this line


def create_trade(open_dt="2025-01-02 12:00:00",
                 close_dt=None,
                 direction="long",
                 timeframe="5m",
                 trading_hours="rth",
                 entry_price=5000,
                 stop_ticks=20,
                 stop_target=4995,
                 prof_ticks=20,
                 prof_target=5005,
                 open_drawdown=1000,
                 name="DELETEME"
                 ):
    return dht.Trade(open_dt=open_dt,
                     close_dt=close_dt,
                     direction=direction,
                     timeframe=timeframe,
                     trading_hours=trading_hours,
                     entry_price=entry_price,
                     stop_ticks=stop_ticks,
                     stop_target=stop_target,
                     prof_ticks=prof_ticks,
                     prof_target=prof_target,
                     open_drawdown=open_drawdown,
                     name=name,
                     )


def create_tradeseries(start_dt="2025-01-01 00:00:00",
                       end_dt="2025-02-01 00:00:00",
                       timeframe="5m",
                       symbol="ES",
                       name="DELETEME",
                       params_str="a1_b2_c3_p0",
                       ts_id=None,
                       bt_id=None,
                       trades=None,
                       ):
    r = dht.TradeSeries(start_dt=start_dt,
                        end_dt=end_dt,
                        timeframe=timeframe,
                        symbol=symbol,
                        name=name,
                        params_str=params_str,
                        ts_id=ts_id,
                        bt_id=bt_id,
                        trades=trades,
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
    assert isinstance(r.params_str, str)
    assert r.params_str == params_str
    if bt_id is None:
        assert r.bt_id is None
    else:
        assert isinstance(r.bt_id, str)
        assert r.bt_id == bt_id
    # Calculated and adjusted attributes
    # symbol should get converted to a Symbol object
    assert isinstance(r.symbol, dhc.Symbol)
    # If symbol was passed as a string, it should now be the ticker
    if isinstance(symbol, str):
        assert r.symbol.ticker == symbol
    assert isinstance(r.ts_id, (str))
    # If ts_id was not passed it should get calculated with name and params_str
    if ts_id is None:
        assert name in r.ts_id
        assert params_str in r.ts_id
    else:
        assert r.ts_id == ts_id
    assert isinstance(r.trades, list)
    return r


def test_TradeSeries_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    ts = create_tradeseries()
    test_trade = create_trade()
    assert isinstance(ts, dht.TradeSeries)
    assert len(ts.pretty().splitlines()) == 13
    ts.add_trade(test_trade)
    # With trades shown
    assert len(ts.pretty(suppress_trades=False).splitlines()) == 42


def test_TradeSeries_add_sort_and_get_trades():
    ts = create_tradeseries()
    assert len(ts.trades) == 0
    # Create and add 2 trades out of order (later trade goes in first)
    trade1 = create_trade(open_dt="2025-01-05 12:00:00",
                          close_dt="2025-01-05 13:00:00")
    ts.add_trade(trade1)
    assert len(ts.trades) == 1
    trade2 = create_trade(open_dt="2025-01-04 09:35:00",
                          close_dt="2025-01-04 09:40:00")
    ts.add_trade(trade2)
    assert len(ts.trades) == 2
    # Confirm trades are out of order
    assert ts.trades[0] == trade1
    assert ts.trades[1] == trade2
    # Sort and verify they are now in chronological order
    ts.sort_trades()
    assert ts.trades[0] == trade2
    assert ts.trades[1] == trade1
    # Confirm we can retrieve a trade by open_dt
    assert ts.get_trade_by_open_dt("2025-01-04 09:35:00") == trade2
    # Confirm we cannot retrieve a trade by open_dt that does not exist
    assert ts.get_trade_by_open_dt("2025-01-04 08:35:00") is None


def test_TradeSeries_store_retrieve_and_delete():
    # Create a TradeSeries with 2 Trade objects to test with
    ts = create_tradeseries(name="DELETEME-TEST")
    ts.add_trade(create_trade(open_dt="2025-01-05 12:00:00",
                              close_dt="2025-01-05 13:00:00"))
    ts.add_trade(create_trade(open_dt="2025-01-06 09:35:00",
                              close_dt="2025-01-06 09:40:00"))
    # Clear and confirm storage has no objects with this name currently
    dhs.delete_tradeseries(symbol="ES", field="name", value="DELETEME-TEST")
    s_ts = dhs.get_tradeseries_by_field(field="name", value="DELETEME-TEST")
    assert len(s_ts) == 0
    dhs.delete_trades(symbol="ES", field="name", value="DELETEME-TEST")
    s_tr = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(s_tr) == 0
    # Store and check the result looks successful by matching ts_id on each
    r = ts.store(store_trades=True)
    assert len(r["tradeseries"]) == 1
    r_ts = r["tradeseries"][0]
    assert r_ts["ts_id"] == ts.ts_id
    r_tr = r["trades"]
    assert len(r_tr) == 2
    assert r_tr[0][0]["ts_id"] == ts.ts_id
    assert r_tr[1][0]["ts_id"] == ts.ts_id
    # Confirm we can retrieve the TradeSeries and both Trades by ts_id
    r_ts = dhs.get_tradeseries_by_field(field="ts_id", value=ts.ts_id)
    assert len(r_ts) == 1
    assert isinstance(r_ts[0], dht.TradeSeries)
    assert r_ts[0].ts_id == ts.ts_id
    r_tr = dhs.get_trades_by_field(field="ts_id", value=ts.ts_id)
    assert len(r_tr) == 2
    assert isinstance(r_tr[0], dht.Trade)
    assert r_tr[0].ts_id == ts.ts_id
    assert isinstance(r_tr[1], dht.Trade)
    assert r_tr[1].ts_id == ts.ts_id
    # Delete objects by ts_id
    dhs.delete_tradeseries(symbol="ES", field="ts_id", value=ts.ts_id)
    dhs.delete_trades(symbol="ES", field="ts_id", value=ts.ts_id)
    # Confirm storage has no objects with this name or ts_id at end of test
    s_ts = dhs.get_tradeseries_by_field(field="name", value="DELETEME-TEST")
    assert len(s_ts) == 0
    s_tr = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(s_tr) == 0
    s_ts = dhs.get_tradeseries_by_field(field="ts_id", value=ts.ts_id)
    assert len(s_ts) == 0
    s_tr = dhs.get_trades_by_field(field="ts_id", value=ts.ts_id)
    assert len(s_tr) == 0
