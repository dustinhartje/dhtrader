from copy import copy
import datetime
import json
import site
import pytest
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhtrades as dht
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str
import dhstore as dhs
from datetime import timedelta as td
from testdata.testdata import Rebuilder

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
# TODO confirm no other TODOs remain in this file before clearing this line


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
                  close_dt=None,
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


def test_Trade_confirm_observed_results():
    """Confirm the results of a number of live trades match the values that
    were seen "in the wild".  All of the trades in this test were performed
    in an Apex trading account with drawdown and balance before/after recorded
    in Apex_Live_Trade_Observations.md to use as assertion targets directly and
    to use in directly calculating additional assertions such as gain_loss and
    drawdown_trailing_increase.

    Actual candle data is used to update the trades in most cases with many
    assertions being based on this data such as high_price and low_price.

    Some assertions could not be observed so they are assumed based on my best
    understanding of the math involved.  This includes balance_low,
    balance_high, drawdown_low, and drawdown_high

    These trades cover all combinations and edge cases I could think of and
    check all potential outputs.  Collectively they should cover most potential
    regression issues though I'll still create targetted mock values tests as
    well to be on the safe side.

    Note that stop and profit targets are seldom used to close these trades as
    they mostly were closed manually to catch specific scenarios, as such the
    drawdown_low/max and balance_low/max might have been different if I were
    using targets.  Other tests will cover target based scenarios."""

    # These tests compare to real trade results captured in an Apex evaluation
    # account as noted in Apex_Drawdown_Observations.md.  Using real trade
    # results ensures all calculations match real life and not just my
    # assumptions about real life that may be inaccurate.

    # Long trade closed in profit after some pullback using 2 contracts
    # Also confirms multiple contracts calculate correctly for long trades
    t = create_trade(open_dt="2025-03-09 23:12:52",
                     direction="long",
                     entry_price=5749.50,
                     )
    add_1m_candle(t, "2025-03-09 23:12:00", 5753.25, 5753.25, 5748.50, 5749.75)
    add_1m_candle(t, "2025-03-09 23:13:00", 5750, 5750.75, 5747.75, 5748.25)
    t.close(price=5749.75, dt="2025-03-09 23:13:07")
    assert t.high_price == 5753.25
    assert t.low_price == 5747.75
    assert t.exit_price == 5749.75
    assert t.parent_bar_secs() == 172
    bal = t.balance_impact(245483.93, 2, 50, 3.10)
    assert bal["balance_open"] == 245483.93
    assert bal["balance_close"] == 245502.73
    assert bal["balance_low"] == 245302.73
    assert bal["balance_high"] == 245852.73
    assert bal["gain_loss"] == 18.8
    draw = t.drawdown_impact(1757.28, 6500, 2, 50, 3.10)
    assert draw["drawdown_open"] == 1757.28
    assert draw["drawdown_close"] == 1776.08
    assert draw["drawdown_low"] == 1582.28
    assert draw["drawdown_high"] == 2132.28
    assert draw["drawdown_trail_increase"] == 0

    # Long trade closed at a loss after being temporarily in profit
    t = create_trade(open_dt="2025-03-09 23:18:05",
                     direction="long",
                     entry_price=5749.25,
                     )
    add_1m_candle(t, "2025-03-09 23:18:00", 5749, 5749.50, 5748.75, 5749.50)
    add_1m_candle(t, "2025-03-09 23:19:00", 5749.50, 5749.50, 5748.75, 5748.75)
    add_1m_candle(t, "2025-03-09 23:20:00", 5748.75, 5748.75, 5747, 5747.50)
    t.close(price=5748.50, dt="2025-03-09 23:20:03")
    assert t.high_price == 5749.50
    assert t.low_price == 5747
    assert t.exit_price == 5748.50
    assert t.parent_bar_secs() == 185
    bal = t.balance_impact(245502.73, 1, 50, 3.10)
    assert bal["balance_open"] == 245502.73
    assert bal["balance_close"] == 245462.13
    assert bal["balance_low"] == 245387.13
    assert bal["balance_high"] == 245512.13
    assert bal["gain_loss"] == -40.6
    draw = t.drawdown_impact(1776.08, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1776.08
    assert draw["drawdown_close"] == 1735.48
    assert draw["drawdown_low"] == 1663.58
    assert draw["drawdown_high"] == 1788.58
    assert draw["drawdown_trail_increase"] == 0

    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-09 23:27:33",
                     direction="long",
                     entry_price=5746,
                     )
    add_1m_candle(t, "2025-03-09 23:27:00", 5746.25, 5746.25, 5745.75, 5746)
    add_1m_candle(t, "2025-03-09 23:28:00", 5746.25, 5747, 5745.50, 5745.50)
    add_1m_candle(t, "2025-03-09 23:29:00", 5745.75, 5746, 5745, 5745)
    t.close(price=5746, dt="2025-03-09 23:29:09")
    assert t.high_price == 5747
    assert t.low_price == 5745
    assert t.exit_price == 5746
    assert t.parent_bar_secs() == 153
    bal = t.balance_impact(245462.13, 1, 50, 3.10)
    assert bal["balance_open"] == 245462.13
    assert bal["balance_close"] == 245459.03
    assert bal["balance_low"] == 245409.03
    assert bal["balance_high"] == 245509.03
    assert bal["gain_loss"] == -3.1
    draw = t.drawdown_impact(1735.48, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1735.48
    assert draw["drawdown_close"] == 1732.38
    assert draw["drawdown_low"] == 1685.48
    assert draw["drawdown_high"] == 1785.48
    assert draw["drawdown_trail_increase"] == 0

    # Long trade runs to profit target directly from entry
    t = create_trade(open_dt="2025-03-09 23:31:05",
                     direction="long",
                     entry_price=5746.25,
                     )
    add_1m_candle(t, "2025-03-09 23:31:00", 5745.75, 5747, 5745.75, 5746.75)
    add_1m_candle(t, "2025-03-09 23:32:00", 5746.75, 5747.75, 5746.75, 5747.25)
    add_1m_candle(t, "2025-03-09 23:33:00", 5747, 5747.75, 5746.50, 5747.75)
    t.close(price=5746.75, dt="2025-03-09 23:33:00")
    assert t.high_price == 5747.75
    assert t.low_price == 5745.75
    assert t.exit_price == 5746.75
    assert t.parent_bar_secs() == 65
    bal = t.balance_impact(245459.03, 1, 50, 3.10)
    assert bal["balance_open"] == 245459.03
    assert bal["balance_close"] == 245480.93
    assert bal["balance_low"] == 245430.93
    assert bal["balance_high"] == 245530.93
    assert bal["gain_loss"] == 21.90
    draw = t.drawdown_impact(1732.38, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1732.38
    assert draw["drawdown_close"] == 1754.28
    assert draw["drawdown_low"] == 1707.38
    assert draw["drawdown_high"] == 1807.38
    assert draw["drawdown_trail_increase"] == 0

    # Long trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:01:01",
                     direction="long",
                     entry_price=5751.25,
                     )
    add_1m_candle(t, "2025-03-10 00:01:00", 5751.25, 5751.25, 5750.50, 5750.50)
    t.close(price=5750.75, dt="2025-03-10 00:01:43")
    assert t.high_price == 5751.25
    assert t.low_price == 5750.50
    assert t.exit_price == 5750.75
    assert t.parent_bar_secs() == 61
    bal = t.balance_impact(245656.13, 1, 50, 3.10)
    assert bal["balance_open"] == 245656.13
    assert bal["balance_close"] == 245628.03
    assert bal["balance_low"] == 245615.53
    assert bal["balance_high"] == 245653.03
    assert bal["gain_loss"] == -28.10
    draw = t.drawdown_impact(1929.48, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1929.48
    assert draw["drawdown_close"] == 1901.38
    assert draw["drawdown_low"] == 1891.98
    assert draw["drawdown_high"] == 1929.48
    assert draw["drawdown_trail_increase"] == 0

    # Short trade runs to profit target directly from entry
    # Also confirms multiple contracts calculate correctly for short trades
    t = create_trade(open_dt="2025-03-09 23:52:44",
                     direction="short",
                     entry_price=5752.25,
                     )
    add_1m_candle(t, "2025-03-09 23:52:00", 5752.50, 5752.50, 5752, 5752)
    add_1m_candle(t, "2025-03-09 23:53:00", 5752, 5752, 5751.50, 5751.50)
    add_1m_candle(t, "2025-03-09 23:54:00", 5751.50, 5752.25, 5751.50, 5752)
    t.close(price=5751.50, dt="2025-03-09 23:54:04")
    assert t.high_price == 5752.50
    assert t.low_price == 5751.50
    assert t.exit_price == 5751.50
    assert t.parent_bar_secs() == 164
    bal = t.balance_impact(245506.03, 3, 50, 3.10)
    assert bal["balance_open"] == 245506.03
    assert bal["balance_close"] == 245609.23
    assert bal["balance_low"] == 245459.23
    assert bal["balance_high"] == 245609.23
    assert bal["gain_loss"] == 103.20
    draw = t.drawdown_impact(1779.38, 6500, 3, 50, 3.10)
    assert draw["drawdown_open"] == 1779.38
    assert draw["drawdown_close"] == 1882.58
    assert draw["drawdown_low"] == 1741.88
    assert draw["drawdown_high"] == 1891.88
    assert draw["drawdown_trail_increase"] == 0

    # Short trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:04:05",
                     direction="short",
                     entry_price=5750.50,
                     )
    # TV has this candle low at 5750.75 but I entered at 5750.50 (confirmed)
    # I suspect NinjaTrader used the prior candle close due to low volume since
    # it was a simulated trade while no real trade happened at that price.
    # Adjusting candle in this test to match my simulated entry
    add_1m_candle(t, "2025-03-10 00:04:00", 5750.75, 5751.25, 5750.50, 5751.25)
    add_1m_candle(t, "2025-03-10 00:05:00", 5751.50, 5751.75, 5751.25, 5751.75)
    t.close(price=5751.75, dt="2025-03-10 00:05:12")
    assert t.high_price == 5751.75
    assert t.low_price == 5750.50
    assert t.exit_price == 5751.75
    assert t.parent_bar_secs() == 245
    bal = t.balance_impact(245628.03, 1, 50, 3.10)
    assert bal["balance_open"] == 245628.03
    assert bal["balance_close"] == 245562.43
    assert bal["balance_low"] == 245562.43
    assert bal["balance_high"] == 245624.93
    assert bal["gain_loss"] == -65.60
    draw = t.drawdown_impact(1901.38, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1901.38
    assert draw["drawdown_close"] == 1835.78
    assert draw["drawdown_low"] == 1838.88
    assert draw["drawdown_high"] == 1901.38
    assert draw["drawdown_trail_increase"] == 0

    # Short trade closed at a loss after reaching profit temporarily
    t = create_trade(open_dt="2025-03-10 00:10:03",
                     direction="short",
                     entry_price=5752.75,
                     )
    add_1m_candle(t, "2025-03-10 00:10:00", 5752.75, 5753, 5752.25, 5753)
    add_1m_candle(t, "2025-03-10 00:11:00", 5753, 5753.75, 5753, 5753.50)
    t.close(price=5753.25, dt="2025-03-10 00:11:08")
    assert t.high_price == 5753.75
    assert t.low_price == 5752.25
    assert t.exit_price == 5753.25
    assert t.parent_bar_secs() == 3
    bal = t.balance_impact(245562.43, 1, 50, 3.10)
    assert bal["balance_open"] == 245562.43
    assert bal["balance_close"] == 245534.33
    assert bal["balance_low"] == 245509.33
    assert bal["balance_high"] == 245584.33
    assert bal["gain_loss"] == -28.10
    draw = t.drawdown_impact(1835.78, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1835.78
    assert draw["drawdown_close"] == 1807.68
    assert draw["drawdown_low"] == 1785.78
    assert draw["drawdown_high"] == 1860.78
    assert draw["drawdown_trail_increase"] == 0

    # Short trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-10 00:12:56",
                     direction="short",
                     entry_price=5754.25,
                     )
    add_1m_candle(t, "2025-03-10 00:12:00", 5754, 5754.50, 5754, 5754.25)
    add_1m_candle(t, "2025-03-10 00:13:00", 5754.50, 5754.75, 5754, 5754)
    t.close(price=5754.25, dt="2025-03-10 00:13:54")
    assert t.high_price == 5754.75
    assert t.low_price == 5754
    assert t.exit_price == 5754.25
    assert t.parent_bar_secs() == 176
    bal = t.balance_impact(245534.33, 1, 50, 3.10)
    assert bal["balance_open"] == 245534.33
    assert bal["balance_close"] == 245531.23
    assert bal["balance_low"] == 245506.23
    assert bal["balance_high"] == 245543.73
    assert bal["gain_loss"] == -3.10
    draw = t.drawdown_impact(1807.68, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1807.68
    assert draw["drawdown_close"] == 1804.58
    assert draw["drawdown_low"] == 1782.68
    assert draw["drawdown_high"] == 1820.18
    assert draw["drawdown_trail_increase"] == 0

    # Short trade closed in profit after some pullback
    t = create_trade(open_dt="2025-03-10 00:16:24",
                     direction="short",
                     entry_price=5754.50,
                     )
    add_1m_candle(t, "2025-03-10 00:16:00", 5754.75, 5754.75, 5754.50, 5754.75)
    add_1m_candle(t, "2025-03-10 00:17:00", 5754.50, 5755, 5754.25, 5754.75)
    add_1m_candle(t, "2025-03-10 00:18:00", 5754.75, 5754.75, 5754, 5754.25)
    add_1m_candle(t, "2025-03-10 00:19:00", 5754.25, 5754.75, 5753.50, 5754)
    t.close(price=5754, dt="2025-03-10 00:19:58")
    assert t.high_price == 5755
    assert t.low_price == 5753.50
    assert t.exit_price == 5754
    assert t.parent_bar_secs() == 84
    bal = t.balance_impact(245531.23, 1, 50, 3.10)
    assert bal["balance_open"] == 245531.23
    assert bal["balance_close"] == 245553.13
    assert bal["balance_low"] == 245503.13
    assert bal["balance_high"] == 245578.13
    assert bal["gain_loss"] == 21.90
    draw = t.drawdown_impact(1804.58, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 1804.58
    assert draw["drawdown_close"] == 1826.48
    assert draw["drawdown_low"] == 1779.58
    assert draw["drawdown_high"] == 1854.58
    assert draw["drawdown_trail_increase"] == 0

    # Remaining trades occurred near/through drawdown_limit levels

    # Long trade starting at drawdown_limit sustained downside then partial
    # pullback to exit at loss
    t = create_trade(open_dt="2025-03-10 00:30:02",
                     direction="long",
                     entry_price=5756.25,
                     )
    add_1m_candle(t, "2025-03-10 00:30:00", 5756, 5756.25, 5755, 5755.25)
    add_1m_candle(t, "2025-03-10 00:31:00", 5755.50, 5755.50, 5755, 5755)
    add_1m_candle(t, "2025-03-10 00:32:00", 5755, 5755, 5754.25, 5754.25)
    add_1m_candle(t, "2025-03-10 00:33:00", 5754.50, 5754.75, 5753.75, 5753.75)
    add_1m_candle(t, "2025-03-10 00:34:00", 5753.75, 5754.25, 5753.75, 5754.25)
    add_1m_candle(t, "2025-03-10 00:35:00", 5754.25, 5754.75, 5752.75, 5753.25)
    add_1m_candle(t, "2025-03-10 00:36:00", 5753.25, 5753.25, 5750.50, 5751.50)
    add_1m_candle(t, "2025-03-10 00:37:00", 5751.25, 5751.50, 5750.75, 5751.50)
    t.close(price=5751, dt="2025-03-10 00:37:03")
    assert t.high_price == 5756.25
    assert t.low_price == 5750.50
    assert t.exit_price == 5751
    assert t.parent_bar_secs() == 2
    bal = t.balance_impact(250000, 1, 50, 3.10)
    assert bal["balance_open"] == 250000
    assert bal["balance_close"] == 249734.40
    assert bal["balance_low"] == 249709.40
    assert bal["balance_high"] == 249996.90
    assert bal["gain_loss"] == -265.60
    draw = t.drawdown_impact(6500, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 6500
    assert draw["drawdown_close"] == 6234.40
    assert draw["drawdown_low"] == 6212.5
    assert draw["drawdown_high"] == 6500.0
    assert draw["drawdown_trail_increase"] == 0

    # Short trade opens at drawdown_limit and goes into profit.  We expect zero
    # drawdown_impact because it was already at max and closed without any
    # pullback from it's peak, however we should see a positive gain_loss
    t = create_trade(open_dt="2025-03-10 00:52:13",
                     direction="short",
                     entry_price=5748.25,
                     prof_target=5747.50,
                     prof_ticks=None,
                     )
    add_1m_candle(t, "2025-03-10 00:52:00", 5749, 5749.25, 5748.25, 5748.25)
    add_1m_candle(t, "2025-03-10 00:53:00", 5748.25, 5748.50, 5746, 5746)
    # No .close() is needed as this candle closes via prof_target
    assert t.high_price == 5749.25
    assert t.low_price == 5747.50
    assert t.exit_price == 5747.50
    assert t.parent_bar_secs() == 133
    bal = t.balance_impact(250000, 1, 50, 3.10)
    assert bal["balance_open"] == 250000
    assert bal["balance_close"] == 250034.40
    assert bal["balance_low"] == 249946.90
    assert bal["balance_high"] == 250034.40
    assert bal["gain_loss"] == 34.40
    draw = t.drawdown_impact(6500, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 6500
    assert draw["drawdown_close"] == 6496.9
    assert draw["drawdown_low"] == 6450.0
    assert draw["drawdown_high"] == 6537.5
    assert draw["drawdown_trail_increase"] == 37.5

    # The following 3 trades open near drawdown_limit, run into
    # profit past that threshold, then pull back some before being closed.
    # These should not have equal balance vs drawdown impacts because the
    # trailing effect is triggered when it surpasses drawdown_limit.

    # Short 1 contract surpasses drawdown_limit
    t = create_trade(open_dt="2025-03-10 00:40:41",
                     direction="short",
                     entry_price=5752.50,
                     )
    add_1m_candle(t, "2025-03-10 00:40:00", 5751.75, 5752.75, 5751.75, 5752.50)
    add_1m_candle(t, "2025-03-10 00:41:00", 5752.50, 5752.50, 5751.50, 5751.50)
    add_1m_candle(t, "2025-03-10 00:42:00", 5751.50, 5751.75, 5749.25, 5750)
    add_1m_candle(t, "2025-03-10 00:43:00", 5750, 5750.50, 5749.75, 5750.50)
    add_1m_candle(t, "2025-03-10 00:44:00", 5750.50, 5750.50, 5750.25, 5750.25)
    add_1m_candle(t, "2025-03-10 00:45:00", 5750, 5057.75, 5749.75, 5750.75)
    add_1m_candle(t, "2025-03-10 00:46:00", 5750.75, 5751.25, 5750.50, 5750.50)
    add_1m_candle(t, "2025-03-10 00:47:00", 5750.50, 5751, 5749.75, 5750.50)
    add_1m_candle(t, "2025-03-10 00:48:00", 5750.50, 5751, 5750.25, 5750.75)
    add_1m_candle(t, "2025-03-10 00:49:00", 5750.75, 5750.75, 5750.25, 5750.25)
    add_1m_candle(t, "2025-03-10 00:50:00", 5750.25, 5751, 5750.25, 5750.25)
    add_1m_candle(t, "2025-03-10 00:51:00", 5750.25, 5750.50, 5748.75, 5749.25)
    add_1m_candle(t, "2025-03-10 00:52:00", 5749, 5749.25, 5748.25, 5748.25)
    add_1m_candle(t, "2025-03-10 00:53:00", 5748.25, 5748.50, 5746, 5746)
    add_1m_candle(t, "2025-03-10 00:54:00", 5746.25, 5748, 5746.25, 5748)
    t.close(price=5747.25, dt="2025-03-10 00:54:10")
    assert t.high_price == 5752.75
    assert t.low_price == 5746
    assert t.exit_price == 5747.25
    assert t.parent_bar_secs() == 41
    bal = t.balance_impact(249734.40, 1, 50, 3.10)
    assert bal["balance_open"] == 249734.40
    assert bal["balance_close"] == 249993.80
    assert bal["balance_low"] == 249718.80
    assert bal["balance_high"] == 250056.30
    assert bal["gain_loss"] == 259.40
    draw = t.drawdown_impact(6234.40, 6500, 1, 50, 3.10)
    assert draw["drawdown_open"] == 6234.40
    # Github Issue 33 (multiple lines in this file)
    # calculating as 6493.80, observed 6448.45, off by 45.35
    # assert draw["drawdown_close"] == 6448.45
    assert draw["drawdown_low"] == 6221.9
    assert draw["drawdown_high"] == 6559.4
    # Github Issue 33 (multiple lines in this file)
    # calculating as 59.40, observed 45.35, off by 14.05
    # assert draw["drawdown_trail_increase"] == 45.35

    # Long 2 contracts surpasses drawdown_limit
    t = create_trade(open_dt="2025-03-14 14:36:49",
                     direction="long",
                     entry_price=5625.75,
                     )
    add_1m_candle(t, "2025-03-14 14:36:00", 5625, 5627, 5623.75, 5625)
    add_1m_candle(t, "2025-03-14 14:37:00", 5625, 5627.75, 5625, 5626.75)
    add_1m_candle(t, "2025-03-14 14:38:00", 5626.75, 5631.25, 5626.75, 5630.50)
    add_1m_candle(t, "2025-03-14 14:39:00", 5630.75, 5632.75, 5629.75, 5632.50)
    add_1m_candle(t, "2025-03-14 14:40:00", 5632.75, 5633.75, 5632, 5632.25)
    add_1m_candle(t, "2025-03-14 14:41:00", 5632.50, 5632.75, 5626.75, 5629)
    t.close(price=5631.50, dt="2025-03-14 14:41:21")
    assert t.high_price == 5633.75
    assert t.low_price == 5623.75
    assert t.exit_price == 5631.50
    assert t.parent_bar_secs() == 109
    bal = t.balance_impact(249706.40, 2, 50, 3.10)
    assert bal["balance_open"] == 249706.40
    assert bal["balance_close"] == 250275.20
    assert bal["balance_low"] == 249500.20
    assert bal["balance_high"] == 250500.20
    assert bal["gain_loss"] == 568.80
    draw = t.drawdown_impact(6161.05, 6500, 2, 50, 3.10)
    assert draw["drawdown_open"] == 6161.05
    # Github Issue 33 (multiple lines in this file)
    # calculating as 6500, observed 6321.90, off by 178.10
    # assert draw["drawdown_close"] == 6321.90
    assert draw["drawdown_low"] == 5961.05
    assert draw["drawdown_high"] == 6961.05
    # Github Issue 33 (multiple lines in this file)
    # calculating as 461.05, observed 407.95, off by 53.10
    # assert draw["drawdown_trail_increase"] == 407.95

    # Short 3 contracts surpasses drawdown_limit
    t = create_trade(open_dt="2025-03-14 14:51:28",
                     direction="short",
                     entry_price=5629.50,
                     )
    add_1m_candle(t, "2025-03-14 14:51:00", 5633.25, 5633.25, 5628, 5628.75)
    add_1m_candle(t, "2025-03-14 14:52:00", 5629, 5630.75, 5627.50, 5630.25)
    t.close(price=5628.75, dt="2025-03-14 14:52:22")
    assert t.high_price == 5633.25
    assert t.low_price == 5627.50
    assert t.exit_price == 5628.75
    assert t.parent_bar_secs() == 88
    bal = t.balance_impact(250275.20, 3, 50, 3.10)
    assert bal["balance_open"] == 250275.20
    assert bal["balance_close"] == 250378.40
    assert bal["balance_low"] == 249703.40
    assert bal["balance_high"] == 250565.90
    assert bal["gain_loss"] == 103.20
    draw = t.drawdown_impact(6321.90, 6500, 3, 50, 3.10)
    assert draw["drawdown_open"] == 6321.90
    # Github Issue 33 (multiple lines in this file)
    # calculating as 6425.10, observed 6382.85, off by 42.25
    # assert draw["drawdown_close"] == 6382.85
    assert draw["drawdown_low"] == 5759.4
    assert draw["drawdown_high"] == 6621.9
    # Github Issue 33 (multiple lines in this file)
    # calculating as 121.90, observed 51.25, off by 70.65
    # assert draw["drawdown_trail_increase"] == 51.25


def test_Trade_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    trade = create_trade()
    assert isinstance(trade, dht.Trade)
    assert len(trade.pretty().splitlines()) == 32


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


def test_Trade_gain_loss():
    """Confirms Trade.gain_loss() method math working as expected"""
    # Closing long trade at a gain
    t = create_trade()
    t.close(price=5005, dt="2025-01-02 12:45:00")
    assert t.gain_loss(contracts=1) == 250
    # Closing long trade at a loss
    t = create_trade()
    t.close(price=4800, dt="2025-01-02 12:45:00")
    assert t.gain_loss(contracts=2) == -20000
    # Closing short trade at a gain
    t = create_trade(direction="short")
    t.close(price=4950, dt="2025-01-02 12:45:00")
    assert t.gain_loss(contracts=3) == 7500
    # Closing short trade at a loss
    t = create_trade(direction="short")
    t.close(price=5025, dt="2025-01-02 12:45:00")
    assert t.gain_loss(contracts=5) == -6250


def test_Trade_duration():
    """Confirms Trade.duration method math working as expected"""
    t = create_trade(open_dt="2025-01-02 12:45:00")
    t.close(price=5005, dt="2025-01-02 12:45:00")
    assert t.duration() == 0
    t.close(price=5005, dt="2025-01-02 12:46:00")
    assert t.duration() == 60
    t.close(price=5005, dt="2025-01-03 12:46:00")
    assert t.duration() == 86460
    t.close(price=5005, dt="2025-01-02 12:44:00")
    assert t.duration() == -60


def test_Trade_creation_long_close_at_profit():
    # Create a trade (create_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    add_1m_candle(t, "2025-03-14 14:52:00", 5003, 5003, 5003, 5003)
    # Closing long trade at a gain
    t.close(price=5005, dt="2025-01-02 12:45:00")
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(3000, 6500, 1, 50, 3.10)

    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 5005
    assert bal["gain_loss"] == 246.9
    assert t.gain_loss(contracts=1) == 250
    assert draw["drawdown_close"] == 3246.9
    assert not t.is_open
    assert t.profitable


def test_Trade_creation_long_close_at_loss():
    # Create a trade (create_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    add_1m_candle(t, "2025-03-14 14:52:00", 5009, 5009, 5009, 5009)
    # Closing long trade at a loss
    t.close(price=4995, dt="2025-01-02 12:45:00")
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(1000, 6500, 1, 50, 3.10)
    assert t.exit_price == 4995
    assert bal["gain_loss"] == -253.10
    assert t.gain_loss(contracts=1) == -250
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
    add_1m_candle(t, "2025-03-14 14:52:00", 4998, 4998, 4998, 4998)
    # Closing long trade at a profit
    t.close(price=4995, dt="2025-01-02 12:45:00")
    bal = t.balance_impact(100000, 1, 50, 3.10)
    draw = t.drawdown_impact(1000, 6500, 1, 50, 3.10)
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 4995
    assert bal["gain_loss"] == 246.90
    assert t.gain_loss(contracts=1) == 250
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
    assert t.gain_loss(contracts=1) == -250
    assert draw["drawdown_close"] == 746.90
    assert not t.is_open
    assert not t.profitable


def test_Trade_candle_update_returns_correct_values():
    # Should not return closed until some target is met (500 ticks default)
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5001, c_low=4999, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="long")
    assert t.candle_update(c)["closed"] is False
    t = create_trade(direction="short")
    assert t.candle_update(c)["closed"] is False
    # Should close at stop target 4875 long / 5125 short exactly
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=4875, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="long")
    assert t.candle_update(c)["closed"] is True
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5125, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="short")
    assert t.candle_update(c)["closed"] is True
    # Should not close at profit target 5125 long / 4875 short exactly
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5125, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="long")
    assert t.candle_update(c)["closed"] is False
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=4875, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="short")
    assert t.candle_update(c)["closed"] is False
    # Should close one tick past profit targets
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5125.25, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="long")
    assert t.candle_update(c)["closed"] is True
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=4874.75, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t = create_trade(direction="short")
    assert t.candle_update(c)["closed"] is True


def test_Trade_candle_update_closes_trades_correctly():
    # Check close status and related attribs/methods for all target scenarios
    # Long trade should not close with no target hit
    t = create_trade(direction="long")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.is_open is True
    assert t.profitable is None
    assert t.close_dt is None
    assert t.exit_price is None
    assert t.drawdown_impact(1000, 3000, 1, 50, 3.10) is None
    assert t.balance_impact(1000, 1, 50, 3.10) is None
    # Long trade closes at prof_target when surpassed
    t = create_trade(direction="long")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5200, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.is_open is False
    assert t.profitable is True
    assert t.close_dt == "2025-01-02 12:01:00"
    assert t.exit_price == 5125
    assert t.drawdown_impact(1000, 3000, 1, 50, 3.10) is not None
    assert t.balance_impact(1000, 1, 50, 3.10) is not None
    # Long trade closes at stop_target when surpassed
    t = create_trade(direction="long")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=4800, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.is_open is False
    assert t.profitable is False
    assert t.close_dt == "2025-01-02 12:01:00"
    assert t.exit_price == 4875
    assert t.drawdown_impact(1000, 3000, 1, 50, 3.10) is not None
    assert t.balance_impact(1000, 1, 50, 3.10) is not None
    # Short trade should not close with no target hit
    t = create_trade(direction="short")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.is_open is True
    assert t.profitable is None
    assert t.close_dt is None
    assert t.exit_price is None
    assert t.drawdown_impact(1000, 3000, 1, 50, 3.10) is None
    assert t.balance_impact(1000, 1, 50, 3.10) is None
    # Short trade closes at prof_target when surpassed
    t = create_trade(direction="short")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5000, c_low=4800, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.is_open is False
    assert t.profitable is True
    assert t.close_dt == "2025-01-02 12:01:00"
    assert t.exit_price == 4875
    assert t.drawdown_impact(1000, 3000, 1, 50, 3.10) is not None
    assert t.balance_impact(1000, 1, 50, 3.10) is not None
    # Short trade closes at stop_target when surpassed
    t = create_trade(direction="short")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5200, c_low=5000, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.is_open is False
    assert t.profitable is False
    assert t.close_dt == "2025-01-02 12:01:00"
    assert t.exit_price == 5125
    assert t.drawdown_impact(1000, 3000, 1, 50, 3.10) is not None
    assert t.balance_impact(1000, 1, 50, 3.10) is not None
    # Long trade does not close during entry minute when 1m bar closes under
    # the profit target.  It should wait for the next bar to confirm profit.
    t = create_trade(direction="long", stop_ticks=20, prof_ticks=20)
    c = dhc.Candle(c_datetime="2025-01-02 12:00:00", c_timeframe="1m",
                   c_open=5000, c_high=5050, c_low=4999, c_close=5000,
                   c_volume=100, c_symbol="ES")
    status = t.candle_update(c)
    assert not status["closed"]
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5050, c_low=4999, c_close=5050,
                   c_volume=100, c_symbol="ES")
    status = t.candle_update(c)
    assert status["closed"]
    # Long trade does close during entry minute when 1m bar closes over the
    # profit target
    t = create_trade(direction="long", stop_ticks=20, prof_ticks=20)
    c = dhc.Candle(c_datetime="2025-01-02 12:00:00", c_timeframe="1m",
                   c_open=5000, c_high=5050, c_low=4999, c_close=5050,
                   c_volume=100, c_symbol="ES")
    status = t.candle_update(c)
    assert status["closed"]
    # Short trade does not close during entry minute when 1m bar closes over
    # the profit target.  It should wait for the next bar to confirm profit.
    t = create_trade(direction="short", stop_ticks=20, prof_ticks=20)
    c = dhc.Candle(c_datetime="2025-01-02 12:00:00", c_timeframe="1m",
                   c_open=5000, c_high=5001, c_low=4950, c_close=5000,
                   c_volume=100, c_symbol="ES")
    status = t.candle_update(c)
    assert not status["closed"]
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5001, c_low=4950, c_close=5050,
                   c_volume=100, c_symbol="ES")
    status = t.candle_update(c)
    assert status["closed"]
    # Short trade does close during entry minute when 1m bar closes under the
    # profit target
    t = create_trade(direction="short", stop_ticks=20, prof_ticks=20)
    c = dhc.Candle(c_datetime="2025-01-02 12:00:00", c_timeframe="1m",
                   c_open=5000, c_high=5001, c_low=4950, c_close=4950,
                   c_volume=100, c_symbol="ES")
    status = t.candle_update(c)
    assert status["closed"]


def test_Trade_sets_high_low_exit_prices_correctly():
    # Long trade sets candle high and low if exit targets are not hit
    t = create_trade(direction="long")
    assert t.high_price == 5000
    assert t.low_price == 5000
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5100, c_low=4900, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.high_price == 5100
    assert t.low_price == 4900
    assert t.exit_price is None
    assert t.is_open
    # Long trade sets correctly at profit target when surpassed
    t = create_trade(direction="long")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5200, c_low=4900, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.high_price == 5125
    assert t.low_price == 4900
    assert t.exit_price == 5125
    assert not t.is_open
    # Long trade sets correctly at stop target when surpassed
    t = create_trade(direction="long")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5100, c_low=4800, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.high_price == 5100
    assert t.low_price == 4875
    assert t.exit_price == 4875
    assert not t.is_open
    # Short trade sets candle high and low if exit targets are not hit
    t = create_trade(direction="short")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5100, c_low=4900, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.high_price == 5100
    assert t.low_price == 4900
    assert t.exit_price is None
    assert t.is_open
    # Short trade sets correctly at profit target when surpassed
    t = create_trade(direction="short")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5100, c_low=4800, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.high_price == 5100
    assert t.low_price == 4875
    assert t.exit_price == 4875
    assert not t.is_open
    # Short trade sets correctly at stop target when surpassed
    t = create_trade(direction="short")
    c = dhc.Candle(c_datetime="2025-01-02 12:01:00", c_timeframe="1m",
                   c_open=5000, c_high=5200, c_low=4900, c_close=5000,
                   c_volume=100, c_symbol="ES")
    t.candle_update(c)
    assert t.high_price == 5125
    assert t.low_price == 4900
    assert t.exit_price == 5125
    assert not t.is_open


def test_Trade_parent_bar_secs():
    """Test method parent_bar_secs() returns accurate on several timeframes"""
    t = create_trade(open_dt="2025-01-02 11:52:44", timeframe="1m")
    assert t.parent_bar_secs() == 44
    t = create_trade(open_dt="2025-01-02 11:52:44", timeframe="5m")
    assert t.parent_bar_secs() == 164
    t = create_trade(open_dt="2025-01-02 11:52:44", timeframe="15m")
    assert t.parent_bar_secs() == 464
    t = create_trade(open_dt="2025-01-02 11:52:44", timeframe="e1h")
    assert t.parent_bar_secs() == 3164
    t = create_trade(open_dt="2025-01-02 11:52:44", timeframe="r1h")
    assert t.parent_bar_secs() == 1364


def test_Trade_closed_intraday():
    """Confirms .closed_intraday() method returns correct values for all known
    scenarios"""
    # RTH trade closes same day before rth close
    t = create_trade(open_dt="2025-01-05 12:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-05 12:05:00", price=5000)
    assert t.closed_intraday() is True
    # RTH trade closes same day after close
    t = create_trade(open_dt="2025-01-06 12:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-06 20:00:00", price=5000)
    assert t.closed_intraday() is False
    # RTH trade closes next day early before rth open
    t = create_trade(open_dt="2025-01-06 12:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-07 04:00:00", price=5000)
    assert t.closed_intraday() is False
    # RTH trade closes next day after rth open
    t = create_trade(open_dt="2025-01-06 12:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-07 11:00:00", price=5000)
    assert t.closed_intraday() is False
    # RTH trade opens midday, closes 1 second before market close
    t = create_trade(open_dt="2025-01-06 14:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-06 16:59:59", price=5000)
    assert t.closed_intraday() is False
    # RTH trade opens midday, closes at exact open of next day
    t = create_trade(open_dt="2025-01-06 14:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-07 09:30:00", price=5000)
    assert t.closed_intraday() is False
    # RTH trade closes several days later during market hours
    t = create_trade(open_dt="2025-01-06 12:00:00",
                     trading_hours="rth")
    t.close(dt="2025-01-09 14:00:00", price=5000)
    assert t.closed_intraday() is False
    # ETH trade opens early before rth open, closes midday before close
    t = create_trade(open_dt="2025-01-06 04:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-06 14:00:00", price=5000)
    assert t.closed_intraday() is True
    # ETH trade opens early before rth open, closes same day after close
    t = create_trade(open_dt="2025-01-06 04:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-06 19:00:00", price=5000)
    assert t.closed_intraday() is False
    # ETH trade opens early before rth open, closes in last minute of day
    t = create_trade(open_dt="2025-01-06 04:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-06 16:59:00", price=5000)
    assert t.closed_intraday() is True
    # ETH trade opens early before rth open, closes at exact open of next day
    t = create_trade(open_dt="2025-01-06 04:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-06 18:00:00", price=5000)
    assert t.closed_intraday() is False
    # ETH trade opens late after eth open, closes same day before midnight
    t = create_trade(open_dt="2025-01-06 19:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-06 21:30:00", price=5000)
    assert t.closed_intraday() is True
    # ETH trade opens late after eth open, closes next morning before rth open
    t = create_trade(open_dt="2025-01-06 19:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-07 04:00:00", price=5000)
    assert t.closed_intraday() is True
    # ETH trade opens late after eth open, closes next day after rth open
    t = create_trade(open_dt="2025-01-06 19:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-07 14:00:00", price=5000)
    assert t.closed_intraday() is True
    # ETH trade opens late after eth open, closes next day after next open
    t = create_trade(open_dt="2025-01-06 19:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-07 20:00:00", price=5000)
    assert t.closed_intraday() is False
    # ETH trade closes several days later
    t = create_trade(open_dt="2025-01-06 12:00:00",
                     trading_hours="eth")
    t.close(dt="2025-01-09 15:25:00", price=5000)
    assert t.closed_intraday() is False


@pytest.mark.storage
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


@pytest.mark.historical
def test_Trade_historical():
    """Rebuild lists of Trades from historical extracted data and compare
    methods output to expected results manually calculated outside of dhtrader

    Tests methods:
        Trade.balance_impact()
        Trade.drawdown_impact()
    """
    # SET1 SHORT TRADES NO REFINING ######################################
    # Rebuild trades list from historical extracted data file
    tf = "testdata/set1/set1_trades.json"
    ts_id = "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    trades = Rebuilder().rebuild_trades(in_file=tf,
                                        ts_id=ts_id)

    # Trade.balance_impact()
    ef = "testdata/set1/expected/set1_trades_shorts_full_balanceimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    for i, t in enumerate(trades):
        actual_results = t.balance_impact(balance_open=100000,
                                          contracts=2,
                                          contract_value=50,
                                          contract_fee=3.04)
        expected = copy(expected_results[i])
        # remove "open_dt" which is included for debugging reference
        if "open_dt" in expected:
            expected.pop("open_dt")
        assert actual_results == expected

    # Trade.drawdown_impact()
    ef = "testdata/set1/expected/set1_trades_shorts_full_drawdownimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    for i, t in enumerate(trades):
        actual_results = t.drawdown_impact(drawdown_open=6000,
                                           drawdown_limit=6500,
                                           contracts=2,
                                           contract_value=50,
                                           contract_fee=3.04)
        expected = copy(expected_results[i])
        # remove "open_dt" and "liquidated" keys from expected as they vary
        if "open_dt" in expected:
            expected.pop("open_dt")
        assert actual_results == expected

    # SET1 LONG TRADES NO REFINING ######################################
    # Rebuild trades list from historical extracted data file
    tf = "testdata/set1/set1_trades.json"
    ts_id = "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    trades = Rebuilder().rebuild_trades(in_file=tf,
                                        ts_id=ts_id)

    # Trade.balance_impact()
    ef = "testdata/set1/expected/set1_trades_longs_full_balanceimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    for i, t in enumerate(trades):
        actual_results = t.balance_impact(balance_open=100000,
                                          contracts=2,
                                          contract_value=50,
                                          contract_fee=3.04)
        expected = copy(expected_results[i])
        # remove "open_dt" which is included for debugging reference
        if "open_dt" in expected:
            expected.pop("open_dt")
        assert actual_results == expected

    # Trade.drawdown_impact()
    ef = "testdata/set1/expected/set1_trades_longs_full_drawdownimpact.json"
    with open(ef, "r") as f:
        expected_results = json.load(f)
    for i, t in enumerate(trades):
        actual_results = t.drawdown_impact(drawdown_open=6000,
                                           drawdown_limit=6500,
                                           contracts=2,
                                           contract_value=50,
                                           contract_fee=3.04)
        expected = copy(expected_results[i])
        # remove "open_dt" and "liquidated" keys from expected as they vary
        if "open_dt" in expected:
            expected.pop("open_dt")
        assert actual_results == expected
