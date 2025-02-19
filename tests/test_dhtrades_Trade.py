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


def hide_dhtrades_Trade_drawdown_calculations_correct():
    """Test that drawdown impact calculates properly for all scenarios."""
    # NOTE While some of these scenarios are covered in later test functions,
    #      I think it's wise to cover all drawdown scenarios in their own
    #      dedicated test to ensure full coverage and have a place to easily
    #      review their expected behavior even if there is duplication later
    # TODO run a few trades on MES in APEX to figure out what actual
    #      results should be expected for these scenarios.
    # TODO long max profit
    # TODO long partial profit (closed after peak and pullback but still ITM)
    # TODO long close at breakeven
    # TODO long close at direct loss (never got green)
    # TODO long close at loss after some green
    # TODO short max profit
    # TODO short partial profit (closed after peak and pullback but still ITM)
    # TODO short close at breakeven
    # TODO short close at direct loss (never got green)
    # TODO short close at loss after some green
    # TODO move this test to after create_and_verify_pretty once finished


def create_trade(open_dt="2025-01-02 12:00:00",
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
                  open_drawdown=open_drawdown,
                  name=name,
                  )
    # Validate passed attributes
    assert isinstance(r, dht.Trade)
    assert r.open_dt == open_dt
    assert r.direction == direction
    assert r.timeframe == timeframe
    assert r.trading_hours == trading_hours
    assert r.entry_price == entry_price
    assert r.open_drawdown == open_drawdown
    assert r.name == name
    # Validate default attributes
    # Note that some are adjusted by __init__ such as symbol
    assert r.close_drawdown is None
    assert r.close_dt is None
    assert r.exit_price is None
    assert r.gain_loss is None
    assert r.offset_ticks == 0
    assert r.drawdown_impact == 0
    assert isinstance(r.symbol, dhc.Symbol)
    assert r.symbol.ticker == "ES"
    assert r.contracts == 1
    assert r.contract_value == 50
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


def test_Trade_creation_long_update_drawdown_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_trade(direction="long")
    # Update drawdown_impact
    t.update_drawdown(price_seen=5003)
    assert t.drawdown_impact == -3
    # Closing long trade at a gain
    t.close(price=5005, dt="2025-01-02 12:45:00")
    # open_drawdown should not change from default
    assert t.open_drawdown == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 5005
    assert t.gain_loss == 250
    assert t.drawdown_impact == 250
    assert t.close_drawdown == 1250
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
    # open_drawdown should not change from default
    assert t.open_drawdown == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 4995
    assert t.gain_loss == -250
    assert t.drawdown_impact == -700
    assert t.close_drawdown == 300
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
    # open_drawdown should not change from default
    assert t.open_drawdown == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 4995
    assert t.gain_loss == 250
    assert t.drawdown_impact == 250
    assert t.close_drawdown == 1250
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
    # open_drawdown should not change from default
    assert t.open_drawdown == 1000
    # Confirm exit price, gain_loss, and drawdown calculate correctly
    assert t.exit_price == 5005
    assert t.gain_loss == -250
    assert t.drawdown_impact == -350
    assert t.close_drawdown == 650
    assert not t.is_open
    assert not t.profitable


def test_Trade_store_retrieve_delete():
    # First make sure there are no DELETEME trades in storage currently
    dhs.delete_trades(symbol="ES", field="name", value="DELETEME-TEST")
    stored = dhs.get_trades_by_field(field="name", value="DELETEME-TEST")
    assert len(stored) == 0
    # Create and store a basic test, confirming it can be retreived after
    t = create_trade(name="DELETEME-TEST")
    print("\n-----------------------------")
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
