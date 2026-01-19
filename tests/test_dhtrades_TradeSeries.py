import datetime
import pytest
import json
import site
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhtrades as dht
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str
import dhstore as dhs
from testdata.testdata import Rebuilder

# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO confirm no other TODOs remain in this file before clearing this one


# TODO Tests needed (some of these have already been written partially/fully
# TradeSeries review __init__ and make sure I've covered all attributes with
#             type and value test, as well as any calculations or scenarios
#             where wrong things might get passed in or various flags might
#             change behavior
#             -- perhaps this should have it's own test to be clear and found
#                easily for future updates?  even if all it does is call
#                create_*
# TradeSeries __eq__ and __ne__ pass and fail scenarios
# TradeSeries __str__ and __repr__ return strings successfully
# TradeSeries to.json and to_clean_dict  return correct types and mock values
# TradeSeries .update_bt_id() success and fail scenarios, make sure trades
#             also get updates
# TradeSeries delete_from_storage() needs coverage, I've only used dhstore
#             functions in tests below


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
                     name=name,
                     )


def add_1m_candle(trade, dt, c_open, c_high, c_low, c_close):
    """Creates a dhcharts.Candle representing a 1 minute candle occurring
    during an open trade.  This is used to test against actual observed live
    trade results, simulating each significant candle in the trade."""
    trade.candle_update(dhc.Candle(c_datetime=dt,
                                   c_timeframe="1m",
                                   c_open=c_open,
                                   c_high=c_high,
                                   c_low=c_low,
                                   c_close=c_close,
                                   c_volume=100,
                                   c_symbol="ES",
                                   ))


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
    r = dht.TradeSeries(start_dt=start_dt,
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
    assert isinstance(r.start_dt, str)
    assert r.start_dt == start_dt
    assert isinstance(r.end_dt, str)
    assert r.end_dt == end_dt
    # start_dt and end_dt should be convertible to datetime objects
    assert isinstance(dt_as_dt(r.start_dt), datetime.datetime)
    assert isinstance(dt_as_dt(r.end_dt), datetime.datetime)
    assert isinstance(r.timeframe, str)
    assert r.timeframe == timeframe
    assert isinstance(r.trading_hours, str)
    assert r.trading_hours == trading_hours
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
    assert len(ts.pretty().splitlines()) == 15
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


def test_TradeSeries_balance_impact_and_stats():
    # Test a TradeSeries with winning and losing trades that does not liquidate
    ts = create_tradeseries()
    # Trade closes 25pts/$1250 in profit with rr=4
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5025,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5200, 5000, 5000)
    ts.add_trade(t)
    # Trade closes -50pts/-$2500 in loss with rr=1
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=5050,
                     )
    add_1m_candle(t, "2025-01-02 13:31:00", 5000, 5000, 4500, 5000)
    ts.add_trade(t)
    r = ts.balance_impact(balance_open=3000,
                          contracts=1,
                          contract_value=50,
                          contract_fee=3.10)
    # Loses -$1250 before fees and -$6.20 in fees per contract (1)
    assert r["balance_open"] == 3000
    assert r["balance_close"] == 1743.80
    assert r["balance_high"] == 4246.90
    assert r["balance_low"] == 1743.80
    assert r["liquidated"] is False
    s = ts.stats()
    assert s["gl_sequence"] == "gL"
    assert s["profitable_trades"] == 1
    assert s["avg_gain_per_con"] == 1250
    assert s["losing_trades"] == 1
    assert s["avg_loss_per_con"] == 2500
    assert s["total_trades"] == 2
    assert s["success_percent"] == 50
    assert s["duration_sec_p20"] == 60
    assert s["duration_sec_median"] == 60
    assert s["duration_sec_p80"] == 60
    assert s["setup_risk_reward"] == 2
    assert s["effective_risk_reward"] == 2
    assert s["min_risk_reward"] == 1
    assert s["max_risk_reward"] == 4
    assert s["trading_days"] == 1
    assert s["total_days"] == 32
    assert s["total_weeks"] == 4.57
    assert s["trades_per_week"] == 0.44
    assert s["trades_per_day"] == 0.06
    assert s["trades_per_trading_day"] == 2
    assert s["trade_ticks"] == [{'stop': 200, 'prof': 200, 'offset': 0},
                                {'stop': 400, 'prof': 100, 'offset': 0}]

    # Test a TradeSeries with all winning trades that does not liquidate
    ts = create_tradeseries()
    # Trade closes 25pts/$1250 in profit with rr=4
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5025,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5200, 4970, 5000)
    ts.add_trade(t)
    # Trade closes 50pts/$2500 in profit with rr=1
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=5050,
                     )
    add_1m_candle(t, "2025-01-02 13:31:00", 5000, 5100, 4970, 5000)
    ts.add_trade(t)
    # Trade closes 82pts/$4100 in profit with rr=0.61
    t = create_trade(open_dt="2025-01-02 14:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=5082,
                     )
    add_1m_candle(t, "2025-01-02 14:31:00", 5000, 5100, 5000, 5000)
    ts.add_trade(t)
    # Gains $7850 before fees and -$9.30 in fees per contract (4)
    # Works out to $31400 gain with -$37.20 in fees
    r = ts.balance_impact(balance_open=50000,
                          contracts=4,
                          contract_value=50,
                          contract_fee=3.10)
    assert r["balance_open"] == 50000
    assert r["balance_close"] == 81362.80
    assert r["balance_high"] == 81362.80
    assert r["balance_low"] == 43987.60
    assert r["liquidated"] is False
    s = ts.stats()
    assert s["gl_sequence"] == "ggg"
    assert s["profitable_trades"] == 3
    assert s["losing_trades"] == 0
    assert s["total_trades"] == 3
    assert s["success_percent"] == 100
    assert s["duration_sec_p20"] == 60
    assert s["duration_sec_median"] == 60
    assert s["duration_sec_p80"] == 60
    assert s["setup_risk_reward"] == 1.27
    assert s["avg_gain_per_con"] == 2616.67
    assert s["avg_loss_per_con"] is None
    assert s["effective_risk_reward"] is None
    assert s["trading_days"] == 1
    assert s["total_days"] == 32
    assert s["total_weeks"] == 4.57
    assert s["trades_per_week"] == 0.66
    assert s["trades_per_day"] == 0.09
    assert s["trades_per_trading_day"] == 3.0
    assert s["trade_ticks"] == [{'stop': 200, 'prof': 200, 'offset': 0},
                                {'stop': 200, 'prof': 328, 'offset': 0},
                                {'stop': 400, 'prof': 100, 'offset': 0}]

    # Test a TradeSeries with all losing trades that does liquidate
    # after setting two sequential new highs
    ts = create_tradeseries()
    # Trade closes -8pts/-$400 in loss with rr=0.008
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4992, prof_target=6000,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5010, 4900, 5000)
    ts.add_trade(t)
    # Trade closes -20pts/-$1000 in loss with rr=0.02
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4980, prof_target=6000,
                     )
    add_1m_candle(t, "2025-01-02 13:35:00", 5000, 5100, 4900, 5000)
    ts.add_trade(t)
    # Trade closes -25pts/-$1250 in loss with rr=0.0125
    t = create_trade(open_dt="2025-01-02 14:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4975, prof_target=6000,
                     )
    add_1m_candle(t, "2025-01-02 15:51:00", 5000, 5000, 4900, 5000)
    ts.add_trade(t)
    # Loses -$2650 before fees and -$9.30 in fees per contract (2)
    # Works out to -$5300 loss with -$18.60 in fees
    r = ts.balance_impact(balance_open=5000,
                          contracts=2,
                          contract_value=50,
                          contract_fee=3.10)
    assert r["balance_open"] == 5000
    assert r["balance_close"] == -318.60
    assert r["balance_high"] == 14187.60
    assert r["balance_low"] == -318.60
    assert r["liquidated"] is True
    s = ts.stats()
    assert s["gl_sequence"] == "LLL"
    assert s["profitable_trades"] == 0
    assert s["avg_gain_per_con"] is None
    assert s["losing_trades"] == 3
    assert s["avg_loss_per_con"] == 883.33
    assert s["total_trades"] == 3
    assert s["success_percent"] == 0
    assert s["duration_sec_p20"] == 156
    assert s["duration_sec_median"] == 300
    assert s["duration_sec_p80"] == 3036
    assert s["setup_risk_reward"] == 0.02
    assert s["effective_risk_reward"] is None
    assert s["trading_days"] == 1
    assert s["total_days"] == 32
    assert s["total_weeks"] == 4.57
    assert s["trades_per_week"] == 0.66
    assert s["trades_per_day"] == 0.09
    assert s["trades_per_trading_day"] == 3.0
    assert s["trade_ticks"] == [{'stop': 32, 'prof': 4000, 'offset': 0},
                                {'stop': 80, 'prof': 4000, 'offset': 0},
                                {'stop': 100, 'prof': 4000, 'offset': 0}]

    # Change a few things up just to cover bases a bit more
    # NOTE - Changes are not consistent with all other Trade attributes
    ts.end_dt = "2025-01-08 17:00:00"
    ts.trades[2].open_dt = "2025-01-07 12:31:00"
    ts.trades[2].close_dt = "2025-01-07 14:30:00"
    ts.trades[0].stop_ticks = 1000
    ts.trades[1].stop_ticks = 1000
    ts.trades[2].stop_ticks = 1000
    ts.trades[1].profitable = True
    ts.trades[1].exit_price = 5080
    s = ts.stats()
    assert s["gl_sequence"] == "LgL"
    assert s["profitable_trades"] == 1
    assert s["losing_trades"] == 2
    assert s["total_trades"] == 3
    assert s["success_percent"] == 33.33
    assert s["duration_sec_p20"] == 156
    assert s["duration_sec_median"] == 300
    assert s["duration_sec_p80"] == 4404
    assert s["setup_risk_reward"] == 0.25
    assert s["avg_gain_per_con"] == 4000
    assert s["avg_loss_per_con"] == 825
    assert s["effective_risk_reward"] == 0.21
    assert s["trading_days"] == 2
    assert s["total_days"] == 8
    assert s["total_weeks"] == 1.14
    assert s["trades_per_week"] == 2.63
    assert s["trades_per_day"] == 0.38
    assert s["trades_per_trading_day"] == 1.5
    expected_trade_ticks = [{'stop': 1000, 'prof': 4000, 'offset': 0}]
    assert s["trade_ticks"] == expected_trade_ticks
    # Confirm weekly_stats() also
    print(ts.weekly_stats())
    assert ts.weekly_stats() == {'2024-12-29': {'total_trades': 2,
                                                'profitable_trades': 1,
                                                'losing_trades': 1,
                                                'gl_in_ticks': 288,
                                                'success_rate': 50.0},
                                 '2025-01-05': {'total_trades': 1,
                                                'profitable_trades': 0,
                                                'losing_trades': 1,
                                                'gl_in_ticks': -100,
                                                'success_rate': 0.0}
                                 }


def test_TradeSeries_non_target_closes_stats_and_effective_risk_reward_calc():
    """Confirm effective_risk_reward calculations when trades close at prices
       other than their expected targets.  This would typically happen when
       auto closing a trade that is still open at the end of the day."""
    ts = create_tradeseries()
    # Long trade closing at partial profit +23pt at end of day
    t = create_trade(open_dt="2025-01-02 12:30:00", direction="long",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5100,
                     )
    t.close(price=5023, dt="2025-01-02 15:55:00")
    ts.add_trade(t)
    # Short trade closing at partial profit +91.75pt at end of day
    t = create_trade(open_dt="2025-01-03 12:30:00", direction="short",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=5100, prof_target=4900,
                     )
    t.close(price=4908.25, dt="2025-01-03 15:55:00")
    ts.add_trade(t)
    # Long trade closing at partial loss -4.5pt at end of day
    t = create_trade(open_dt="2025-01-04 12:30:00", direction="long",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5050,
                     )
    t.close(price=4995.5, dt="2025-01-04 15:55:00")
    ts.add_trade(t)
    # Short trade closing at partial loss -45pt at end of day
    t = create_trade(open_dt="2025-01-05 12:30:00", direction="short",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=5100, prof_target=4950,
                     )
    t.close(price=5045, dt="2025-01-05 15:55:00")
    ts.add_trade(t)
    # Long trade closing at profit target +50pt
    t = create_trade(open_dt="2025-01-06 12:30:00", direction="long",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5050,
                     )
    t.close(price=5050, dt="2025-01-06 12:55:00")
    ts.add_trade(t)
    # Short trade closing at stop target -75pt
    t = create_trade(open_dt="2025-01-07 12:30:00", direction="short",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=5075, prof_target=4950,
                     )
    t.close(price=5075, dt="2025-01-07 12:55:00")
    ts.add_trade(t)
    # Long trade closing exactly break even at end of day
    t = create_trade(open_dt="2025-01-08 12:30:00", direction="long",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5050,
                     )
    t.close(price=5000, dt="2025-01-08 15:55:00")
    ts.add_trade(t)

    s = ts.stats()
    assert s["gl_sequence"] == "ggLLgLL"
    assert s["profitable_trades"] == 3
    assert s["avg_gain_per_con"] == 2745.83
    assert s["losing_trades"] == 4
    assert s["avg_loss_per_con"] == 1556.25
    assert s["total_trades"] == 7
    assert s["success_percent"] == 42.86
    assert s["setup_risk_reward"] == 1.5
    assert s["effective_risk_reward"] == 0.57
    assert s["trading_days"] == 7
    assert s["total_days"] == 32
    assert s["total_weeks"] == 4.57
    assert s["trades_per_week"] == 1.53
    assert s["trades_per_day"] == 0.22
    assert s["trades_per_trading_day"] == 1
    assert s["trade_ticks"] == [{'stop': 300, 'prof': 200, 'offset': 0},
                                {'stop': 400, 'prof': 200, 'offset': 0},
                                {'stop': 400, 'prof': 400, 'offset': 0}]


def test_TradeSeries_drawdown_impact():
    # Test a TradeSeries with winning and losing trades that does not liquidate
    ts = create_tradeseries()
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5025,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5200, 5000, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=5050,
                     )
    add_1m_candle(t, "2025-01-02 13:31:00", 5000, 5000, 4500, 5000)
    ts.add_trade(t)
    r = ts.drawdown_impact(drawdown_open=3000,
                           drawdown_limit=6500,
                           contracts=1,
                           contract_value=50,
                           contract_fee=3.10)
    assert r["drawdown_open"] == 3000
    assert r["drawdown_close"] == 1743.80
    assert r["drawdown_high"] == 4250
    assert r["drawdown_low"] == 1746.9
    assert r["liquidated"] is False
    # Test a TradeSeries with all winning trades that does not liquidate
    ts = create_tradeseries()
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4900, prof_target=5005,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5200, 4995, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=5003,
                     )
    add_1m_candle(t, "2025-01-02 13:31:00", 5000, 5100, 5000, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 14:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=5007,
                     )
    add_1m_candle(t, "2025-01-02 14:31:00", 5000, 5100, 5000, 5000)
    ts.add_trade(t)
    r = ts.drawdown_impact(drawdown_open=5000,
                           drawdown_limit=6500,
                           contracts=4,
                           contract_value=50,
                           contract_fee=3.10)
    assert r["drawdown_open"] == 5000
    assert r["drawdown_close"] == 6487.6
    assert r["drawdown_high"] == 7887.60
    assert r["drawdown_low"] == 4000.0
    assert r["liquidated"] is False
    # Test a TradeSeries with all losing trades that does liquidate
    # after setting two sequential new highs
    ts = create_tradeseries()
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4992, prof_target=6000,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5002, 4900, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4980, prof_target=6000,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5020, 4900, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 14:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4950, prof_target=6000,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5000, 4900, 5000)
    ts.add_trade(t)
    r = ts.drawdown_impact(drawdown_open=5000,
                           drawdown_limit=6500,
                           contracts=2,
                           contract_value=50,
                           contract_fee=3.10)
    assert r["drawdown_open"] == 5000
    assert r["drawdown_close"] == -2818.60
    assert r["drawdown_high"] == 6193.8
    assert r["drawdown_low"] == -2812.4
    assert r["liquidated"] is True
    # TradeSeries pushes 1 trade past drawdown_limit triggering trail effect
    ts = create_tradeseries()
    t = create_trade(open_dt="2025-01-02 12:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4000, prof_target=5005,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5100, 5000, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 13:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4000, prof_target=5005,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5100, 5000, 5000)
    ts.add_trade(t)
    t = create_trade(open_dt="2025-01-02 14:30:00",
                     stop_ticks=None, prof_ticks=None,
                     stop_target=4000, prof_target=5005,
                     )
    add_1m_candle(t, "2025-01-02 12:31:00", 5000, 5100, 5000, 5000)
    ts.add_trade(t)
    r = ts.drawdown_impact(drawdown_open=5800,
                           drawdown_limit=6500,
                           contracts=2,
                           contract_value=50,
                           contract_fee=3.10)
    assert r["drawdown_open"] == 5800
    assert r["drawdown_close"] == 6493.8
    assert r["drawdown_high"] == 6993.80
    assert r["drawdown_low"] == 5800
    assert r["liquidated"] is False


@pytest.mark.storage
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


@pytest.mark.historical
def test_TradeSeries_historical():
    """Rebuild  TradeSeries from historical extracted data to compare methods
    output to expected results manually calculated outside of dhtrader

    Tests methods:
        TradeSeries.stats()
        TradeSeries.balance_impact()
        TradeSeries.drawdown_impact()
    """
    # SET1 SHORT TRADES NO REFINING ######################################
    # Rebuild testdata/set1 short TradeSeries
    ts = Rebuilder().rebuild_tradeseries(
        in_file="testdata/set1/set1_tradeseries.json",
        ts_ids="BacktestEMAReject-eth_e1h_9_s80-p160-o40",
        bt_ids=None,
        start_dt=None,
        end_dt=None,
        trades_file="testdata/set1/set1_trades.json"
        )[0]

    # .stats() method
    # Get expected stats() results for comparison
    ef = "testdata/set1/expected/set1_ts_shorts_full_stats.json"
    with open(ef, "r") as f:
        expected_stats = json.load(f)
    expected_stats["trade_ticks"] = expected_stats["trade_ticks"]
    # Get actual stats() results from method
    actual_stats = ts.stats()
    # Compare expected to actual results
    assert actual_stats == expected_stats

    # .balance_impact() method
    ef = "testdata/set1/expected/set1_ts_shorts_full_balanceimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    actual_results = ts.balance_impact(balance_open=100000,
                                       contracts=2,
                                       contract_value=50,
                                       contract_fee=3.04,
                                       include_first_min=True)
    assert actual_results == expected_results

    # .drawdown_impact() method
    ef = "testdata/set1/expected/set1_ts_shorts_full_drawdownimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    actual_results = ts.drawdown_impact(drawdown_open=6000,
                                        drawdown_limit=6500,
                                        contracts=2,
                                        contract_value=50,
                                        contract_fee=3.04,
                                        include_first_min=True)
    assert actual_results == expected_results

    # SET1 LONG TRADES NO REFINING ######################################
    # Rebuild testdata/set1 long TradeSeries
    ts = Rebuilder().rebuild_tradeseries(
        in_file="testdata/set1/set1_tradeseries.json",
        ts_ids="BacktestEMABounce-eth_e1h_9_s80-p160-o0",
        bt_ids=None,
        start_dt=None,
        end_dt=None,
        trades_file="testdata/set1/set1_trades.json"
        )[0]

    # .stats() method
    # Get expected stats() results for comparison
    ef = "testdata/set1/expected/set1_ts_longs_full_stats.json"
    with open(ef, "r") as f:
        expected_stats = json.load(f)
    # Get actual stats() results from method
    actual_stats = ts.stats()
    # Compare expected to actual results
    assert actual_stats == expected_stats

    # .balance_impact() method
    ef = "testdata/set1/expected/set1_ts_longs_full_balanceimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    actual_results = ts.balance_impact(balance_open=100000,
                                       contracts=2,
                                       contract_value=50,
                                       contract_fee=3.04,
                                       include_first_min=True)
    assert actual_results == expected_results

    # .drawdown_impact() method
    ef = "testdata/set1/expected/set1_ts_longs_full_drawdownimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    actual_results = ts.drawdown_impact(drawdown_open=6000,
                                        drawdown_limit=6500,
                                        contracts=2,
                                        contract_value=50,
                                        contract_fee=3.04,
                                        include_first_min=True)
    assert actual_results == expected_results
