import datetime
import site
import pytest
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhtrades as dht
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str
import dhstore as dhs

# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO confirm no other TODOs remain in this file before clearing this line


# TODO Tests needed (some of these have already been written partially/fully
# Trade Drawdown calculations (see placeholder function below for details)
# Trade review __init__ and make sure I've covered all attributes with type and
#       value test, as well as any calculations or scenarios where wrong
#       things might get passed in or various flags might change behavior
#       -- perhaps this should have it's own test to be clear and found easily
#          for future updates?  even if all it does is call create_*
# Trade __eq__ and __ne__ pass and fail scenarios
# Trade __str__ and __repr__ return strings successfully
# Trade to.json and to_clean_dict  return correct types and mock values
# Trade .close_trade() I do some closes in other tests already, but write
#       a test that specifically covers as many closing scenarios as I can
#       think up and confirm it's results.  Does timeframe/trading_hours
#       matter here?


def create_trade(open_dt="2025-01-02 12:00:00",
                 direction="long",
                 timeframe="5m",
                 trading_hours="rth",
                 entry_price=5000,
                 stop_ticks=500,  # wide to allow test to control close price
                 stop_target=None,
                 prof_ticks=500,  # wide to allow test to control close price
                 prof_target=None,
                 name="DELETEME",
                 ):
    # Do not add further arguments to this function where defaults are set
    # by Trade() or it will break assertions below meant to test defaults
    # and calculated attributes.  For further testing in test_* functions using
    # these objects, run create_trade() to create then update them after.
    r = dht.Trade(open_dt=open_dt,
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
    # Validate passed attributes
    assert isinstance(r, dht.Trade)
    assert r.open_dt == open_dt
    assert r.direction == direction
    assert r.timeframe == timeframe
    assert r.trading_hours == trading_hours
    assert r.entry_price == entry_price
    assert r.name == name
    # Validate default attributes
    # Note that some are adjusted by __init__ such as symbol
    assert r.close_dt is None
    assert r.exit_price is None
    assert r.offset_ticks == 0
    assert isinstance(r.symbol, dhc.Symbol)
    assert r.symbol.ticker == "ES"
    assert r.is_open
    assert r.profitable is None
    assert r.version == "1.0.0"
    assert r.ts_id is None
    assert r.bt_id is None
    # Validate calculated attributes
    assert isinstance(r.created_dt, str)
    assert isinstance(dt_as_dt(r.created_dt), datetime.datetime)
    assert isinstance(r.open_epoch, int)
    # Stop & profit tick & target calculations have their own test_ function
    # due to complexity, just make sure we got numbers for this part
    assert isinstance(r.stop_ticks, int)
    assert isinstance(r.prof_ticks, int)
    assert isinstance(r.stop_target, (int, float))
    assert isinstance(r.prof_target, (int, float))
    if r.direction == "long":
        assert r.flipper == 1
    else:
        assert r.flipper == -1
    return r


def add_1m_candle(trade, dt, c_open, c_high, c_low, c_close):
    """Creates a dhcharts.Candle representing a 1 minute candle occurring
    during an open trade.  This is used to test against actual observed live
    trade results, simulating each significant candle in the trade.  It's more
    robust and realistic than add_flat_candle() and may replace it entirely
    once proven useful."""
    trade.candle_update(dhc.Candle(c_datetime=dt,
                                   c_timeframe="1m",
                                   c_open=c_open,
                                   c_high=c_high,
                                   c_low=c_low,
                                   c_close=c_close,
                                   c_volume=100,
                                   c_symbol="ES",
                                   ))


def add_flat_candle(trade, price, dt):
    """Creates a dhcharts.Candle representing the price given as the
    all price values as a shortcut for testing specific thresholds without
    digging up real candle values"""
    add_1m_candle(trade, dt, price, price, price, price)


def test_dhtrades_Trade_drawdown_and_balance_calculations():
    """Test that drawdown and balance impact methods calculate properly for
    all scenarios."""
    # NOTE While some of these scenarios are covered in later test functions,
    #      I think it's wise to cover all drawdown scenarios in their own
    #      dedicated test to ensure full coverage and have a place to easily
    #      review their expected behavior even if this duplicates coverage
    # TODO mebe update all other tests to use update candles to match how the
    #      backtests will be running these.  They should always udpate with
    #      the opening and closing candles, and possibly a peak candle if
    #      appropriate.  In theory I should have every trade update with all
    #      actual candles, but I might choose to skip some if they are long
    #      trades and candles are inside of prior ranges seen in the trade.
    #      Note that I'll need to update stop and/or profit targets for this
    #      to work properly, or in cases where I closed manually after a PB
    #      I'll need to run the .close() method directly vs letting .update()
    #      trigger it as a non-interrupted trade would do
    # TODO I need to make sure code is sufficient to handle analysis that does
    #      not include drawdowns.  How would I indicate this?  Should I have
    #      open, close, and drawdown_max attributes as None or set to zero?
    #      zero seems like it would create less error potential but is not as
    #      explicit.
    #      TODO These scenarios also need tests to confirm them
    # TODO This probably belongs in test_dhtrades_TradeSeries.py, but I'll need
    #      one or more tests to confirm TradeSeries calculate multiple trades
    #      correctly in sequence with regard to drawdown values, and also that
    #      they automatically recalculate correctly if Trades are added or
    #      removed (need a method to remove, by datetime probably)
    # TODO do I need to factor in the peak profit seen from my actual trade
    #      test results noted in the .md file below?  Seems likely, at least
    #      for the final drawdown_max tests to work as expected it will need
    #      to know what the max distance from entry was.
    # TODO need more tests to cover tracks_drawdown, tracks_balance, and
    #      confirmations on how the related attributes are adjusted when True
    #      vs when False.
    # TODO need tests generally around balance calculations, make sure it's
    #      doing things accurately.  Probably update this function's name and
    #      assertions to cover drawdowns AND balances both

    # These tests compare to real trade results captured in an Apex evaluation
    # account as noted in Apex_Drawdown_Observations.md.  Using real trade
    # results ensures all calculations match real life and not just my
    # assumptions about real life that may be inaccurate.

    # Each trade updates with a single candle representing the peak profit
    # seen during that trade to simulate drawdown impacts relative to trailing
    # drawdown thresholds

    # Long trade closed in profit after some pullback using 2 contracts
    # Also confirms multiple contracts calculate correctly for long trades
    t = create_trade(open_dt="2025-03-09 23:12:52",
                     direction="long",
                     entry_price=5749.50,
                     )
    add_flat_candle(t, 5750.50, "2025-03-09 23:13:00")  # peak profit
    t.close(price=5749.75, dt="2025-03-09 23:13:07")
    bal = t.balance_impact(245483.93, 2, 50, 3.10)
    draw = t.drawdown_impact(1757.28, 6500, 2, 50, 3.10)
    assert bal["balance_close"] == 245502.73
    assert bal["gain_loss"] == 18.8
    assert draw["drawdown_close"] == 1776.08

    # Long trade closed at a loss after being temporarily in profit
    t = create_trade(open_dt="2025-03-09 23:18:05",
                     direction="long",
                     entry_price=5749.25,
                     )
    add_flat_candle(t, 5749.50, "2025-03-09 23:19:00")  # peak profit
    t.close(price=5748.50, dt="2025-03-09 23:20:03")
    bal = t.balance_impact(245502.73, 1, 50, 3.10)
    draw = t.drawdown_impact(1776.08, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245462.13
    assert bal["gain_loss"] == -40.6
    assert draw["drawdown_close"] == 1735.48

    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-09 23:27:33",
                     direction="long",
                     entry_price=5746,
                     )
    add_flat_candle(t, 5747, "2025-03-09 23:28:00")  # peak profit
    t.close(price=5746, dt="2025-03-09 23:29:09")
    bal = t.balance_impact(245462.13, 1, 50, 3.10)
    draw = t.drawdown_impact(1735.48, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245459.03
    assert bal["gain_loss"] == -3.1
    assert draw["drawdown_close"] == 1732.38

    # Long trade runs to profit target directly from entry
    t = create_trade(open_dt="2025-03-09 23:31:05",
                     direction="long",
                     entry_price=5746.25,
                     )
    add_flat_candle(t, 5746.75, "2025-03-09 23:32:00")  # peak profit
    t.close(price=5746.75, dt="2025-03-09 23:33:00")
    bal = t.balance_impact(245459.03, 1, 50, 3.10)
    draw = t.drawdown_impact(1732.38, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245480.93
    assert bal["gain_loss"] == 21.90
    assert draw["drawdown_close"] == 1754.28

    # Long trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:01:01",
                     direction="long",
                     entry_price=5751.25,
                     )
    add_flat_candle(t, 5751.25, "2025-03-10 00:01:00")  # peak profit
    t.close(price=5750.75, dt="2025-03-10 00:01:43")
    bal = t.balance_impact(245656.13, 1, 50, 3.10)
    draw = t.drawdown_impact(1929.48, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245628.03
    assert bal["gain_loss"] == -28.10
    assert draw["drawdown_close"] == 1901.38

    # Short trade runs to profit target directly from entry
    # Also confirms multiple contracts calculate correctly for short trades
    t = create_trade(open_dt="2025-03-09 23:52:44",
                     direction="short",
                     entry_price=5752.25,
                     )
    add_flat_candle(t, 5751.50, "2025-03-09 23:53:00")  # peak profit
    t.close(price=5751.50, dt="2025-03-09 23:54:04")
    bal = t.balance_impact(245506.03, 3, 50, 3.10)
    draw = t.drawdown_impact(1779.38, 6500, 3, 50, 3.10)
    assert bal["balance_close"] == 245609.23
    assert bal["gain_loss"] == 103.20
    assert draw["drawdown_close"] == 1882.58

    # Short trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:04:05",
                     direction="short",
                     entry_price=5750.50,
                     )
    add_flat_candle(t, 5750.50, "2025-03-10 00:04:00")  # peak profit
    t.close(price=5751.75, dt="2025-03-10 00:05:12")
    bal = t.balance_impact(245628.03, 1, 50, 3.10)
    draw = t.drawdown_impact(1901.38, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245562.43
    assert bal["gain_loss"] == -65.60
    assert draw["drawdown_close"] == 1835.78

    # Short trade closed at a loss after reaching profit temporarily
    t = create_trade(open_dt="2025-03-10 00:10:03",
                     direction="short",
                     entry_price=5752.75,
                     )
    add_flat_candle(t, 5752.25, "2025-03-10 00:10:00")  # peak profit
    t.close(price=5753.25, dt="2025-03-10 00:11:08")
    bal = t.balance_impact(245562.43, 1, 50, 3.10)
    draw = t.drawdown_impact(1835.78, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245534.33
    assert bal["gain_loss"] == -28.10
    assert draw["drawdown_close"] == 1807.68

    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-10 00:12:56",
                     direction="short",
                     entry_price=5754.25,
                     )
    add_flat_candle(t, 5754.25, "2025-03-10 00:12:00")  # peak profit
    t.close(price=5754.25, dt="2025-03-10 00:13:54")
    bal = t.balance_impact(245534.33, 1, 50, 3.10)
    draw = t.drawdown_impact(1807.68, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245531.23
    assert bal["gain_loss"] == -3.10
    assert draw["drawdown_close"] == 1804.58

    # Short trade closed in profit after some pullback
    t = create_trade(open_dt="2025-03-10 00:16:24",
                     direction="short",
                     entry_price=5754.50,
                     )
    add_flat_candle(t, 5753.50, "2025-03-10 00:17:00")  # peak profit
    t.close(price=5754, dt="2025-03-10 00:19:58")
    bal = t.balance_impact(245531.23, 1, 50, 3.10)
    draw = t.drawdown_impact(1804.58, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 245553.13
    assert bal["gain_loss"] == 21.90
    assert draw["drawdown_close"] == 1826.48

    # Remaining trades occurred near/through drawdown_max levels

    # Long trade starting at drawdown_max sustained downside then partial
    # pullback to exit at loss
    t = create_trade(open_dt="2025-03-10 00:30:02",
                     direction="long",
                     entry_price=5756.25,
                     )
    add_flat_candle(t, 5756.25, "2025-03-10 00:31:00")  # peak profit
    t.close(price=5751, dt="2025-03-10 00:37:03")
    bal = t.balance_impact(250000, 1, 50, 3.10)
    draw = t.drawdown_impact(6500, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 249734.40
    assert bal["gain_loss"] == -265.60
    assert draw["drawdown_close"] == 6234.40

    # Short trade opens at drawdown_max and goes into profit.  We expect zero
    # drawdown_impact because it was already at max and closed without any
    # pullback from it's peak, however we should see a positive gain_loss
    t = create_trade(open_dt="2025-03-10 00:52:13",
                     direction="short",
                     entry_price=5748.25,
                     prof_target=5747.50,
                     prof_ticks=None,
                     )
    # opening candle
    add_1m_candle(t, "2025-03-10 00:52:00", 5749, 5749.25, 5748.25, 5748.25)
    # closing candle
    add_1m_candle(t, "2025-03-10 00:53:00", 5748.25, 5748.50, 5746, 5746)
    bal = t.balance_impact(250000, 1, 50, 3.10)
    draw = t.drawdown_impact(6500, 6500, 1, 50, 3.10)
    assert bal["balance_close"] == 250034.40
    assert bal["gain_loss"] == 34.40
    assert draw["drawdown_close"] == 6500

    # The following 3 trades (2 short 1 long) open near drawdown_max, run into
    # profit past that threshold, then pull back some before being closed.
    # These should not have equal gain_loss and drawdown_impact because the
    # trailing effect is triggered when it surpasses drawdown_max.

    # Short 1 contract surpasses drawdown_max
    t = create_trade(open_dt="2025-03-10 00:40:41",
                     direction="short",
                     entry_price=5752.50,
                     )
    add_flat_candle(t, 5746, "2025-03-10 00:53:00")
    t.close(price=5747.25, dt="2025-03-10 00:54:10")
    bal = t.balance_impact(249734.40, 1, 50, 3.10)
    draw = t.drawdown_impact(6234.40, 6500, 1, 50, 3.10)
    print(draw)
    assert bal["balance_close"] == 249993.80
    assert bal["gain_loss"] == 259.40
    # Issue 33 (this and the next 2 drawdown_close assertion failures)
    # TODO getting 6493.80, off by 45.35
    # assert draw["drawdown_close"] == 6448.45

    # Long 2 contracts surpasses drawdown_max
    t = create_trade(open_dt="2025-03-14 14:36:49",
                     direction="long",
                     entry_price=5625.75,
                     )
    add_flat_candle(t, 5633.75, "2025-03-14 14:37:00")
    t.close(price=5631.50, dt="2025-03-14 14:41:21")
    bal = t.balance_impact(249706.40, 2, 50, 3.10)
    draw = t.drawdown_impact(6161.05, 6500, 2, 50, 3.10)
    assert bal["balance_close"] == 250275.20
    assert bal["gain_loss"] == 568.80
    # TODO getting 6500, off by 178.10
    # assert draw["drawdown_close"] == 6321.90

    # Short 3 contracts surpasses drawdown_max
    t = create_trade(open_dt="2025-03-14 14:51:28",
                     direction="short",
                     entry_price=5629.50,
                     )
    add_flat_candle(t, 5627.50, "2025-03-14 14:52:00")
    t.close(price=5628.75, dt="2025-03-14 14:52:22")
    bal = t.balance_impact(250275.20, 3, 50, 3.10)
    draw = t.drawdown_impact(6321.90, 6500, 3, 50, 3.10)
    assert bal["balance_close"] == 250378.40
    assert bal["gain_loss"] == 103.20
    # TODO getting 6425.10, off by 42.25
    # assert draw["drawdown_close"] == 6382.85


def test_Trade_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    trade = create_trade()
    assert isinstance(trade, dht.Trade)
    assert len(trade.pretty().splitlines()) == 26


def test_Trade_tick_and_target_calculations_correct():
    # LONG Providing both accurately should result in the values as provided
    t = create_trade(direction="long",
                     entry_price=5000,
                     stop_ticks=20,
                     stop_target=4995,
                     prof_ticks=20,
                     prof_target=5005,
                     )
    assert t.stop_ticks == 20
    assert t.stop_target == 4995
    assert t.prof_ticks == 20
    assert t.prof_target == 5005
    # LONG Providing both inaccurately should fail with Value Error
    with pytest.raises(ValueError):
        create_trade(direction="long",
                     entry_price=5000,
                     stop_ticks=250,
                     stop_target=4995,
                     )
    with pytest.raises(ValueError):
        create_trade(direction="long",
                     entry_price=5000,
                     prof_ticks=250,
                     prof_target=5005,
                     )
    # LONG Providing ticks only should calculate accurate target
    t = create_trade(direction="long",
                     entry_price=5000,
                     stop_ticks=20,
                     stop_target=None,
                     prof_ticks=20,
                     prof_target=None,
                     )
    assert t.stop_target == 4995
    assert t.prof_target == 5005
    # LONG Providing targets only should calculate accurate ticks
    t = create_trade(direction="long",
                     entry_price=5000,
                     stop_ticks=None,
                     stop_target=4995,
                     prof_ticks=None,
                     prof_target=5005,
                     )
    assert t.stop_ticks == 20
    assert t.prof_ticks == 20
    # LONG Providing neither should fail with ValueError
    with pytest.raises(ValueError):
        t = create_trade(direction="long",
                         entry_price=5000,
                         stop_ticks=None,
                         stop_target=None,
                         )
    with pytest.raises(ValueError):
        t = create_trade(direction="long",
                         entry_price=5000,
                         prof_ticks=None,
                         prof_target=None,
                         )
    # ### Short trades
    # SHORT Providing both accurately should result in the values as provided
    t = create_trade(direction="short",
                     entry_price=5000,
                     stop_ticks=20,
                     stop_target=5005,
                     prof_ticks=20,
                     prof_target=4995,
                     )
    assert t.stop_ticks == 20
    assert t.stop_target == 5005
    assert t.prof_ticks == 20
    assert t.prof_target == 4995
    # SHORT Providing both inaccurately should fail with Value Error
    with pytest.raises(ValueError):
        create_trade(direction="short",
                     entry_price=5000,
                     stop_ticks=250,
                     stop_target=5005,
                     )
    with pytest.raises(ValueError):
        create_trade(direction="short",
                     entry_price=5000,
                     prof_ticks=250,
                     prof_target=4995,
                     )
    # SHORT Providing ticks only should calculate accurate target
    t = create_trade(direction="short",
                     entry_price=5000,
                     stop_ticks=20,
                     stop_target=None,
                     prof_ticks=20,
                     prof_target=None,
                     )
    assert t.stop_target == 5005
    assert t.prof_target == 4995
    # SHORT Providing targets only should calculate accurate ticks
    t = create_trade(direction="short",
                     entry_price=5000,
                     stop_ticks=None,
                     stop_target=5005,
                     prof_ticks=None,
                     prof_target=4995,
                     )
    assert t.stop_ticks == 20
    assert t.prof_ticks == 20
    # SHORT Providing neither should fail with ValueError
    with pytest.raises(ValueError):
        t = create_trade(direction="short",
                         entry_price=5000,
                         stop_ticks=None,
                         stop_target=None,
                         )
    with pytest.raises(ValueError):
        t = create_trade(direction="short",
                         entry_price=5000,
                         prof_ticks=None,
                         prof_target=None,
                         )


def test_Trade_creation_long_update_drawdowns_and_close_at_profit():
    # Create a trade (create_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    add_flat_candle(t, 5003, "2025-03-14 14:52:00")
    # Closing long trade at a gain
    t.close(price=5005, dt="2025-01-02 12:45:00")
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(3000, 6500, 1, 50, 3.10)

    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 5005
    assert bal["gain_loss"] == 246.9
    assert draw["drawdown_close"] == 3246.9
    assert not t.is_open
    assert t.profitable


def test_Trade_creation_long_close_at_loss():
    # Create a trade (create_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    add_flat_candle(t, 5009, "2025-03-14 14:52:00")
    # Closing long trade at a loss
    t.close(price=4995, dt="2025-01-02 12:45:00")
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(1000, 6500, 1, 50, 3.10)
    assert t.exit_price == 4995
    assert bal["gain_loss"] == -253.10
    assert draw["drawdown_close"] == 746.90
    assert not t.is_open
    assert not t.profitable


def test_Trade_creation_short_close_at_profit():
    # Create a trade (create_trade() covers creation assertions)
    t = create_trade(direction="short",
                     stop_target=5005,
                     stop_ticks=20,
                     prof_target=4995,
                     prof_ticks=20,
                     )
    add_flat_candle(t, 4998, "2025-03-14 14:52:00")
    # Closing long trade at a profit
    t.close(price=4995, dt="2025-01-02 12:45:00")
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(1000, 6500, 1, 50, 3.10)
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 4995
    assert bal["gain_loss"] == 246.90
    assert draw["drawdown_close"] == 1246.90
    assert not t.is_open
    assert t.profitable


def test_Trade_creation_short_close_at_loss():
    # Create a trade (create_trade() covers creation assertions)
    t = create_trade(direction="short",
                     stop_target=5005,
                     stop_ticks=20,
                     prof_target=4995,
                     prof_ticks=20,
                     )
    # Closing long trade at a loss
    t.close(price=5005, dt="2025-01-02 12:45:00")
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(1000, 6500, 1, 50, 3.10)
    assert t.exit_price == 5005
    assert bal["gain_loss"] == -253.10
    assert draw["drawdown_close"] == 746.90
    assert not t.is_open
    assert not t.profitable


def test_Trade_store_retrieve_delete():
    # First make sure there are no DELETEME trades in storage currently
    dhs.delete_trades(symbol="ES", field="name", value="DELETEME-TEST")
    stored = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(stored) == 0
    # Create and store a basic test, confirming it can be retreived after
    t = create_trade(name="DELETEME-TEST")
    stored = t.store()
    # Confirm storage op returns something that looks vaguely like our trade
    assert stored[0]["name"] == "DELETEME-TEST"
    # Confirm we can retreive our trade from storage
    stored = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(stored) == 1
    assert isinstance(stored[0], dht.Trade)
    # Delete it from storage and confirm it can no longer be retrieved
    dhs.delete_trades(symbol="ES", field="name", value="DELETEME-TEST")
    stored = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(stored) == 0
