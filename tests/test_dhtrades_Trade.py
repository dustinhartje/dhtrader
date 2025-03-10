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
                 stop_ticks=20,
                 stop_target=None,
                 prof_ticks=20,
                 prof_target=None,
                 drawdown_open=1000,
                 drawdown_max=6500,
                 contracts=1,
                 contract_value=50,
                 contract_fee=3.10,
                 name="DELETEME"
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
                  contracts=contracts,
                  contract_value=contract_value,
                  contract_fee=contract_fee,
                  name=name,
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
    assert r.name == name
    # Validate default attributes
    # Note that some are adjusted by __init__ such as symbol
    assert r.drawdown_close is None
    assert r.close_dt is None
    assert r.exit_price is None
    assert r.gain_loss is None
    assert r.offset_ticks == 0
    assert r.drawdown_impact == 0
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


def test_dhtrades_Trade_drawdown_calculations():
    """Test that drawdown impact calculates properly for all scenarios."""
    # NOTE While some of these scenarios are covered in later test functions,
    #      I think it's wise to cover all drawdown scenarios in their own
    #      dedicated test to ensure full coverage and have a place to easily
    #      review their expected behavior even if this duplicates coverage
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

    # These tests compare to real trade results captured in an Apex evaluation
    # account as noted in Apex_Drawdown_Observations.md.  Using real trade
    # results ensures all calculations match real life and not just my
    # assumptions about real life that may be inaccurate.

    # Long trade closed in profit after some pullback
    # Also confirms multiple contracts calculate correctly for long trades
    t = create_trade(open_dt="2025-03-09 23:12:52",
                     direction="long",
                     entry_price=5749.50,
                     drawdown_open=1757.28,
                     contracts=2
                     )
    t.update_drawdown(5750.50)  # udpate for peak price seen
    t.close(price=5749.75, close_dt="2025-03-09 23:13:07")
    assert t.gain_loss == 18.8
    assert t.drawdown_impact == 18.8
    assert t.drawdown_close == 1776.08
    # Long trade closed at a loss after reaching profit temporarily
    t = create_trade(open_dt="2025-03-09 23:18:05",
                     direction="long",
                     entry_price=5749.25,
                     drawdown_open=1776.08,
                     )
    t.update_drawdown(5749.50)  # udpate for peak price seen
    t.close(price=5748.50, close_dt="2025-03-09 23:20:03")
    assert t.gain_loss == -40.6
    assert t.drawdown_impact == -40.6
    assert t.drawdown_close == 1735.48
    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-09 23:27:33",
                     direction="long",
                     entry_price=5746,
                     drawdown_open=1735.48,
                     )
    t.update_drawdown(5747)  # udpate for peak price seen
    t.close(price=5746, close_dt="2025-03-09 23:29:09")
    assert t.gain_loss == -3.1
    assert t.drawdown_impact == -3.1
    assert t.drawdown_close == 1732.38
    # Long trade runs to profit target directly from entry
    t = create_trade(open_dt="2025-03-09 23:31:05",
                     direction="long",
                     entry_price=5746.25,
                     drawdown_open=1732.38,
                     )
    t.update_drawdown(5746.75)  # udpate for peak price seen
    t.close(price=5746.75, close_dt="2025-03-09 ")
    assert t.gain_loss == 21.90
    assert t.drawdown_impact == 21.90
    assert t.drawdown_close == 1754.28
    # Long trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:01:01",
                     direction="long",
                     entry_price=5751.25,
                     drawdown_open=1929.48,
                     )
    t.update_drawdown(5751.25)  # udpate for peak price seen
    t.close(price=5750.75, close_dt="2025-03-10 00:01:43")
    assert t.gain_loss == -28.10
    assert t.drawdown_impact == -28.10
    assert t.drawdown_close == 1901.38
    # Short trade runs to profit target directly from entry
    # Also confirms multiple contracts calculate correctly for short trades
    t = create_trade(open_dt="2025-03-09 23:52:44",
                     direction="short",
                     entry_price=5752.25,
                     drawdown_open=1779.38,
                     contracts=3,
                     )
    t.update_drawdown(5751.50)  # udpate for peak price seen
    t.close(price=5751.50, close_dt="2025-03-09 23:54:04")
    assert t.gain_loss == 103.20
    assert t.drawdown_impact == 103.20
    assert t.drawdown_close == 1882.58
    # Short trade runs to stop target directly from entry, no green seen
    t = create_trade(open_dt="2025-03-10 00:04:05",
                     direction="short",
                     entry_price=5750.50,
                     drawdown_open=1901.38,
                     )
    t.update_drawdown(5750.50)  # udpate for peak price seen
    t.close(price=5751.75, close_dt="2025-03-10 00:05:12")
    assert t.gain_loss == -65.60
    assert t.drawdown_impact == -65.60
    assert t.drawdown_close == 1835.78
    # Short trade closed at a loss after reaching profit temporarily
    t = create_trade(open_dt="2025-03-10 00:10:03",
                     direction="short",
                     entry_price=5752.75,
                     drawdown_open=1835.78,
                     )
    t.update_drawdown(5752.25)  # udpate for peak price seen
    t.close(price=5753.25, close_dt="2025-03-10 00:11:08")
    assert t.gain_loss == -28.10
    assert t.drawdown_impact == -28.10
    assert t.drawdown_close == 1807.68
    # Long trade closed at break even after some downside seen
    t = create_trade(open_dt="2025-03-10 00:12:56",
                     direction="short",
                     entry_price=5754.25,
                     drawdown_open=1807.68,
                     )
    t.update_drawdown(5754.25)  # udpate for peak price seen
    t.close(price=5754.25, close_dt="2025-03-10 00:13:54")
    assert t.gain_loss == -3.10
    assert t.drawdown_impact == -3.10
    assert t.drawdown_close == 1804.58
    # Short trade closed in profit after some pullback
    t = create_trade(open_dt="2025-03-10 00:16:24",
                     direction="short",
                     entry_price=5754.50,
                     drawdown_open=1804.58,
                     )
    t.update_drawdown(5753.50)  # udpate for peak price seen
    t.close(price=5754, close_dt="2025-03-10 00;19:58")
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
                     )
    t.update_drawdown(5756.25)  # udpate for peak price seen
    t.close(price=5751, close_dt="2025-03-10 00:37:03")
    assert t.gain_loss == -265.60
    assert t.drawdown_impact == -265.60
    assert t.drawdown_close == 6234.40
    # Short trade opens near drawdown_max, runs into profit a few ticks past
    # it then falls back and gets closed at profit below drawdown_max.  While
    # trades up to this one had equal gain_loss and drawdown_impact due to
    # not interacting with drawdown_max, this trade and the next should not
    # result in these attributes being equal due to the trailing effect when
    # profit exceeds the current drawdown_max threshold on the account.
    t = create_trade(open_dt="2025-03-10 00:40:41",
                     direction="short",
                     entry_price=5752.50,
                     drawdown_open=6234.40,
                     )
    t.update_drawdown(5746)  # udpate for peak price seen
    t.close(price=5747.25, close_dt="2025-03-10 00;54:10")
    assert t.gain_loss == 259.40
    assert t.drawdown_impact == 214.05
    assert t.drawdown_close == 6448.45
    # Short trade opens at drawdown_max and goes into profit.  We expect zero
    # drawdown_impact because it was already at max and closed without any
    # pullback from it's peak, however we should see a positive gain_loss
    t = create_trade(open_dt="2025-03-10 00:52:13",
                     direction="short",
                     entry_price=5748.25,
                     drawdown_open=6500,
                     )
    t.update_drawdown(5747.50)  # udpate for peak price seen
    t.close(price=5747.50, close_dt="2025-03-10 00:53:35")
    assert t.gain_loss == 34.40
    assert t.drawdown_impact == 0
    assert t.drawdown_close == 6500


def test_Trade_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    trade = create_trade()
    assert isinstance(trade, dht.Trade)
    assert len(trade.pretty().splitlines()) == 30


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
    gt                 prof_target=5005,
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


def test_Trade_creation_long_update_drawdown_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    t.update_drawdown(price_seen=5003)
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


def test_Trade_creation_long_update_drawdown_and_close_at_loss():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    t.update_drawdown(price_seen=5009)
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


def test_Trade_creation_short_update_drawdown_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="short",
                     stop_target=5005,
                     prof_target=4995,
                     )
    # Update drawdown_impact
    t.update_drawdown(price_seen=4998)
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


def test_Trade_creation_short_update_drawdown_and_close_at_loss():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="short",
                     stop_target=5005,
                     prof_target=4995,
                     )
    # Update drawdown_impact
    t.update_drawdown(price_seen=4998)
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
