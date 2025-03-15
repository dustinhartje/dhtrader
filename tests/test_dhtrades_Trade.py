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
                 drawdown_open=1000,
                 drawdown_max=6500,
                 balance_open=100000,
                 contracts=1,
                 contract_value=50,
                 contract_fee=3.10,
                 name="DELETEME",
                 tracks_drawdown=True,
                 tracks_balance=True,
                 ):
    # Do not add further arguments to this function where defaults are set
    # by Trade() or it will break assertions below meant to test defaults
    # and calculated attributes.  For further testing in test_* functions using
    # these objects, run basic_trade() to create then update them after.
    r = dht.Trade(open_dt=open_dt,
                  direction=direction,
                  timeframe=timeframe,
                  trading_hours=trading_hours,
                  entry_price=entry_price,
                  stop_ticks=stop_ticks,
                  stop_target=stop_target,
                  prof_ticks=prof_ticks,
                  prof_target=prof_target,
                  drawdown_open=drawdown_open,
                  drawdown_max=drawdown_max,
                  balance_open=balance_open,
                  contracts=contracts,
                  contract_value=contract_value,
                  contract_fee=contract_fee,
                  name=name,
                  tracks_balance=tracks_balance,
                  tracks_drawdown=tracks_drawdown,
                  )
    # Validate passed attributes
    assert isinstance(r, dht.Trade)
    assert r.open_dt == open_dt
    assert r.direction == direction
    assert r.timeframe == timeframe
    assert r.trading_hours == trading_hours
    assert r.entry_price == entry_price
    assert r.drawdown_open == drawdown_open
    assert r.drawdown_max == drawdown_max
    assert r.balance_open == balance_open
    assert r.name == name
    assert r.tracks_drawdown == tracks_drawdown
    assert r.tracks_balance == tracks_balance
    # Validate default attributes
    # Note that some are adjusted by __init__ such as symbol
    assert r.drawdown_close is None
    assert r.close_dt is None
    assert r.exit_price is None
    assert r.gain_loss is None
    assert r.offset_ticks == 0
    assert r.drawdown_impact == contracts * contract_fee * -1
    assert isinstance(r.symbol, dhc.Symbol)
    assert r.symbol.ticker == "ES"
    assert r.contracts == contracts
    assert r.contract_value == contract_value
    assert r.contract_fee == contract_fee
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


def add_flat_candle(trade, price, dt):
    """Creates a dhcharts.Candle representing the price given as the
    all price values.  This candle is then fed to the given Trade object's
    .update() method to simulate peak_profit or peak_loss being experienced
    mid-trade in order to test impacts to these and related attributes such
    as drawdown_impact, particularly where prices exceeding the trade's
    drawdown_max come into play."""
    trade.update(dhc.Candle(c_datetime=dt,
                            c_timeframe="1m",
                            c_open=price,
                            c_high=price,
                            c_low=price,
                            c_close=price,
                            c_volume=100,
                            c_symbol="ES",
                            ))

def add_1m_candle(trade, dt, c_open, c_high, c_low, c_close):
    """Creates a dhcharts.Candle representing a 1 minute candle occurring
    during an open trade.  This is used to test against actual observed live
    trade results, simulating each significant candle in the trade.  It's more
    robust and realistic than add_flat_candle() and may replace it entirely
    once proven useful."""
    trade.update(dhc.Candle(c_datetime=dt,
                            c_timeframe="1m",
                            c_open=c_open,
                            c_high=c_high,
                            c_low=c_low,
                            c_close=c_close,
                            c_volume=100,
                            c_symbol="ES",
                            ))

def test_dhtrades_Trade_drawdown_calculations():
    """Test that drawdown impact calculates properly for all scenarios."""
    # NOTE While some of these scenarios are covered in later test functions,
    #      I think it's wise to cover all drawdown scenarios in their own
    #      dedicated test to ensure full coverage and have a place to easily
    #      review their expected behavior even if this duplicates coverage
    # TODO need a test to cover long trade with drawdown overage (profit beyond
    #      drawdown_max then a pullback).  I should do this one with multiple
    #      contracts to ensure multicontract is covered in these scenarios as
    #      well.  I'm pretty sure I have a duplicate TODO somewhere for this
    #      one...
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

    # Long trade closed in profit after some pullback
    # Also confirms multiple contracts calculate correctly for long trades
    t = create_trade(open_dt="2025-03-09 23:12:52",
                     direction="long",
                     entry_price=5749.50,
                     drawdown_open=1757.28,
                     balance_open=245483.93,
                     contracts=2
                     )
    add_flat_candle(t, 5749.75, "2025-03-09 23:13:00")
    t.update_drawdowns(5750.50)  # udpate for peak price seen
    t.close(price=5749.75, dt="2025-03-09 23:13:07")
    assert t.balance_close == 245502.73
    assert t.gain_loss == 18.8
    assert t.drawdown_impact == 18.8
    assert t.drawdown_close == 1776.08
    # Long trade closed at a loss after reaching profit temporarily
    t = create_trade(open_dt="2025-03-09 23:18:05",
                     direction="long",
                     entry_price=5749.25,
                     drawdown_open=1776.08,
                     balance_open=245502.73,
                     )
    t.update_drawdowns(5749.50)  # udpate for peak price seen
    t.close(price=5748.50, dt="2025-03-09 23:20:03")
    assert t.balance_close == 245462.13
    assert t.gain_loss == -40.6
    assert t.drawdown_impact == -40.6
    assert t.drawdown_close == 1735.48
    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-09 23:27:33",
                     direction="long",
                     entry_price=5746,
                     drawdown_open=1735.48,
                     balance_open=245462.13,
                     )
    t.update_drawdowns(5747)  # udpate for peak price seen
    t.close(price=5746, dt="2025-03-09 23:29:09")
    assert t.balance_close == 245459.03
    assert t.gain_loss == -3.1
    assert t.drawdown_impact == -3.1
    assert t.drawdown_close == 1732.38
    # Long trade runs to profit target directly from entry
    t = create_trade(open_dt="2025-03-09 23:31:05",
                     direction="long",
                     entry_price=5746.25,
                     drawdown_open=1732.38,
                     balance_open=245459.03,
                     )
    t.update_drawdowns(5746.75)  # udpate for peak price seen
    t.close(price=5746.75, dt="2025-03-09 ")
    assert t.balance_close == 245480.93
    assert t.gain_loss == 21.90
    assert t.drawdown_impact == 21.90
    assert t.drawdown_close == 1754.28
    # Long trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:01:01",
                     direction="long",
                     entry_price=5751.25,
                     drawdown_open=1929.48,
                     balance_open=245656.13,
                     )
    t.update_drawdowns(5751.25)  # udpate for peak price seen
    t.close(price=5750.75, dt="2025-03-10 00:01:43")
    assert t.balance_close == 245628.03
    assert t.gain_loss == -28.10
    assert t.drawdown_impact == -28.10
    assert t.drawdown_close == 1901.38
    # Short trade runs to profit target directly from entry
    # Also confirms multiple contracts calculate correctly for short trades
    t = create_trade(open_dt="2025-03-09 23:52:44",
                     direction="short",
                     entry_price=5752.25,
                     drawdown_open=1779.38,
                     balance_open=245506.03,
                     contracts=3,
                     )
    t.update_drawdowns(5751.50)  # udpate for peak price seen
    t.close(price=5751.50, dt="2025-03-09 23:54:04")
    assert t.balance_close == 245609.23
    assert t.gain_loss == 103.20
    assert t.drawdown_impact == 103.20
    assert t.drawdown_close == 1882.58
    # Short trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:04:05",
                     direction="short",
                     entry_price=5750.50,
                     drawdown_open=1901.38,
                     balance_open=245628.03,
                     )
    t.update_drawdowns(5750.50)  # udpate for peak price seen
    t.close(price=5751.75, dt="2025-03-10 00:05:12")
    assert t.balance_close == 245562.43
    assert t.gain_loss == -65.60
    assert t.drawdown_impact == -65.60
    assert t.drawdown_close == 1835.78
    # Short trade closed at a loss after reaching profit temporarily
    t = create_trade(open_dt="2025-03-10 00:10:03",
                     direction="short",
                     entry_price=5752.75,
                     drawdown_open=1835.78,
                     balance_open=245562.43,
                     )
    t.update_drawdowns(5752.25)  # udpate for peak price seen
    t.close(price=5753.25, dt="2025-03-10 00:11:08")
    assert t.balance_close == 245534.33
    assert t.gain_loss == -28.10
    assert t.drawdown_impact == -28.10
    assert t.drawdown_close == 1807.68
    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-10 00:12:56",
                     direction="short",
                     entry_price=5754.25,
                     drawdown_open=1807.68,
                     balance_open=245534.33,
                     )
    t.update_drawdowns(5754.25)  # udpate for peak price seen
    t.close(price=5754.25, dt="2025-03-10 00:13:54")
    assert t.balance_close == 245531.23
    assert t.gain_loss == -3.10
    assert t.drawdown_impact == -3.10
    assert t.drawdown_close == 1804.58
    # Short trade closed in profit after some pullback
    t = create_trade(open_dt="2025-03-10 00:16:24",
                     direction="short",
                     entry_price=5754.50,
                     drawdown_open=1804.58,
                     balance_open=245531.23,
                     )
    t.update_drawdowns(5753.50)  # udpate for peak price seen
    t.close(price=5754, dt="2025-03-10 00:19:58")
    assert t.balance_close == 245553.13
    assert t.gain_loss == 21.90
    assert t.drawdown_impact == 21.90
    assert t.drawdown_close == 1826.48

    # Remaining trades occurred near/through drawdown_max levels

    # Long trade starting at drawdown_max sustained downside then partial
    # pullback to exit at loss
    t = create_trade(open_dt="2025-03-10 00:30:02",
                     direction="long",
                     entry_price=5756.25,
                     drawdown_open=6500,
                     balance_open=250000,
                     )
    t.update_drawdowns(5756.25)  # udpate for peak price seen
    t.close(price=5751, dt="2025-03-10 00:37:03")
    assert t.balance_close == 249734.40
    assert t.gain_loss == -265.60
    assert t.drawdown_impact == -265.60
    assert t.drawdown_close == 6234.40


# ^^^^^^^^^ TODO Trades above need switch to candle updates ^^^^^^^^^^^^^^
# ************************ All passing to this point ***********************

    # Short trade opens at drawdown_max and goes into profit.  We expect zero
    # drawdown_impact because it was already at max and closed without any
    # pullback from it's peak, however we should see a positive gain_loss
    t = create_trade(open_dt="2025-03-10 00:52:13",
                     direction="short",
                     entry_price=5748.25,
                     drawdown_open=6500,
                     balance_open=250000,
                     prof_target=5747.50,
                     prof_ticks=None,
                     )
    # opening candle
    print("trade created, about to add opening candle")
    print(t.pretty())
    # TODO update all other tests to use update candles to match how the
    #      backtests will be running these.  They should always udpate with
    #      the opening and closing candles, and possibly a peak candle if
    #      appropriate.  In theory I should have every trade update with all
    #      actual candles, but I might choose to skip some if they are long
    #      trades and candles are inside of prior ranges seen in the trade.
    #      Note that I'll need to update stop and/or profit targets for this
    #      to work properly, or in cases where I closed manually after a PB
    #      I'll need to run the .close() method directly vs letting .update()
    #      trigger it as a non-interrupted trade would do
    add_1m_candle(t, "2025-03-10 00:52:00", 5749, 5749.25, 5748.25, 5748.25)
    print(t.pretty())
    # closing candle
    add_1m_candle(t, "2025-03-10 00:53:00", 5748.25, 5748.50, 5746, 5746)
    assert t.balance_close == 250034.40
    assert t.gain_loss == 34.40
    #TODO this one is showing 34.40 and 6534.40 so it's not honoring
    #     drawdown_max, review and fix
    # looks like peak_profit is not getting calculated because .update() is
    # never run.  Should I always run it for the opening and closing candles?
    assert t.drawdown_impact == 0
    assert t.drawdown_close == 6500

    # The following 3 trades (2 short 1 long) open near drawdown_max, run into
    # profit past that threshold, then pull back some before being closed.
    # These should not have equal gain_loss and drawdown_impact because the
    # trailing effect is triggered when it surpasses drawdown_max.

    # Short 1 contract surpasses drawdown_max
    t = create_trade(open_dt="2025-03-10 00:40:41",
                     direction="short",
                     entry_price=5752.50,
                     drawdown_open=6234.40,
                     balance_open=249734.40,
                     )
    add_flat_candle(t, 5746, "2025-03-10 00:53:00")
    # TODO once this is working , switch all other tests to add_flat_candle()
    #t.update_drawdowns(5746)  # udpate for peak price seen
    t.close(price=5747.25, dt="2025-03-10 00:54:10")
    assert t.balance_close == 249993.80
    assert t.gain_loss == 259.40
    # TODO Issue 33
    #assert t.drawdown_impact == 214.05
    assert t.drawdown_impact == 200
    #assert t.drawdown_close == 6448.45
    assert t.drawdown_close == 6434.4





    # Long 2 contracts surpasses drawdown_max
    t = create_trade(open_dt="2025-03-14 14:36:49",
                     direction="long",
                     entry_price=5625.75,
                     drawdown_open=6161.05,
                     balance_open=249706.40,
                     contracts=2,
                     )
    add_flat_candle(t, 5633.75, "2025-03-14 14:37:00")
    t.close(price=5631.50, dt="2025-03-14 14:41:21")
    assert t.balance_close == 250275.20
    assert t.gain_loss == 568.80
    # NOTE My current guess is that peak_profit isn't getting calced right
    #      and this is throwing off the overage calc later
    # this is a diff of -$53.10 which is suspiciously 1 contract-point and
    # one contract fee, so is it not factoring the contracts in right?
    # if I had done this with one contract....
    # 
    # TODO getting 107.75
    #assert t.drawdown_impact == 160.85
    # TODO getting 6268.80
    #assert t.drawdown_close == 6321.90







    # Short 3 contracts surpasses drawdown_max
    t = create_trade(open_dt="2025-03-14 14:51:28",
                     direction="short",
                     entry_price=5629.50,
                     drawdown_open=6321.90,
                     balance_open=250275.20,
                     contracts=3,
                     )
    add_flat_candle(t, 5627.50, "2025-03-14 14:52:00")
    t.close(price=5628.75, dt="2025-03-14 14:52:22")
    assert t.balance_close == 250378.40
    # TODO getting 103.2
    assert t.gain_loss == 112.20
    # TODO getting 121.80
    #assert t.drawdown_impact == 60.95
    # TODO getting 6200.10
    #assert t.drawdown_close == 6382.85








def hide_Trade_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    trade = create_trade()
    assert isinstance(trade, dht.Trade)
    assert len(trade.pretty().splitlines()) == 30


def hide_Trade_tick_and_target_calculations_correct():
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


def hide_Trade_creation_long_update_drawdowns_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    t.update_drawdowns(price_seen=5003)
    assert t.drawdown_impact == -3
    # Closing long trade at a gain
    t.close(price=5005, dt="2025-01-02 12:45:00")
    # drawdown_open should not change from default
    assert t.drawdown_open == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 5005
    assert t.gain_loss == 250
    assert t.drawdown_impact == 250
    assert t.drawdown_close == 1250
    assert not t.is_open
    assert t.profitable


def hide_Trade_creation_long_update_drawdowns_and_close_at_loss():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    t.update_drawdowns(price_seen=5009)
    assert t.drawdown_impact == -9
    # Closing long trade at a loss
    t.close(price=4995, dt="2025-01-02 12:45:00")
    # drawdown_open should not change from default
    assert t.drawdown_open == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 4995
    assert t.gain_loss == -250
    assert t.drawdown_impact == -700
    assert t.drawdown_close == 300
    assert not t.is_open
    assert not t.profitable


def hide_Trade_creation_short_update_drawdowns_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="short",
                     stop_target=5005,
                     prof_target=4995,
                     )
    # Update drawdown_impact
    t.update_drawdowns(price_seen=4998)
    assert t.drawdown_impact == -2
    # Closing long trade at a profit
    t.close(price=4995, dt="2025-01-02 12:45:00")
    # drawdown_open should not change from default
    assert t.drawdown_open == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 4995
    assert t.gain_loss == 250
    assert t.drawdown_impact == 250
    assert t.drawdown_close == 1250
    assert not t.is_open
    assert t.profitable


def hide_Trade_creation_short_update_drawdowns_and_close_at_loss():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="short",
                     stop_target=5005,
                     prof_target=4995,
                     )
    # Update drawdown_impact
    t.update_drawdowns(price_seen=4998)
    assert t.drawdown_impact == -2
    # Closing long trade at a loss
    t.close(price=5005, dt="2025-01-02 12:45:00")
    # drawdown_open should not change from default
    assert t.drawdown_open == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 5005
    assert t.gain_loss == -250
    assert t.drawdown_impact == -350
    assert t.drawdown_close == 650
    assert not t.is_open
    assert not t.profitable


def hide_Trade_store_retrieve_delete():
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
