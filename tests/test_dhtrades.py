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



# ################# Transferred from dhtrades.py #####################


def test_dhtrades_create_and_verify_pretty_all_classes():
    # TODO consider moving these (and similar in other test_*.py base files)
    #      to class specific test files if that structure makes more sense
    #      once I have tests fleshed out a lot more.  Maybe non-class-specific
    #      test files make sense to only have for functions and not
    #      for classes/methods?

    # Check line counts of pretty output, won't change unless class changes
    # ---------------------------- TRADE ---------------------------
    out_trade = dht.Trade(open_dt="2025-01-02 12:00:00",
                          direction="long",
                          timeframe="5m",
                          trading_hours="rth",
                          entry_price=5001.50,
                          stop_target=4995,
                          prof_target=5010,
                          open_drawdown=1000,
                          name="DELETEME"
                          )
    assert isinstance(out_trade, dht.Trade)
    assert len(out_trade.pretty().splitlines()) == 30
    # -------------------------- TRADESERIES -------------------------
    out_ts = dht.TradeSeries(start_dt="2025-01-02 00:00:00",
                             end_dt="2025-01-05 17:59:00",
                             timeframe="5m",
                             symbol="ES",
                             name="DELETEME_Testing",
                             params_str="1p_2s",
                             trades=None,
                             )
    assert isinstance(out_ts, dht.TradeSeries)
    assert len(out_ts.pretty().splitlines()) == 13
    out_ts.add_trade(out_trade)
    assert len(out_ts.pretty().splitlines()) == 13
    # With trades shown
    assert len(out_ts.pretty(suppress_trades=False).splitlines()) == 42

    # --------------------------- BACKTEST --------------------------
    out_bt = dht.Backtest(start_dt="2025-01-02 12:00:00",
                          end_dt="2025-01-02 12:01:00",
                          symbol="ES",
                          timeframe="1m",
                          trading_hours="eth",
                          name="DELETEME_Testing",
                          parameters={},
                          autoload_charts=True,
                          )
    assert isinstance(out_bt, dht.Backtest)
    assert len(out_bt.pretty().splitlines()) == 17
    out_bt.add_tradeseries(out_ts)
    # With tradeseries, trades, charts, and candles shown
    assert len(out_bt.pretty(suppress_tradeseries=False,
                             suppress_trades=False,
                             suppress_charts=False,
                             suppress_chart_candles=False,
                             ).splitlines()) == 170




# ============================ TRADES ==============================

def create_basic_trade(open_dt="2025-01-02 12:00:00",
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
    result = dht.Trade(open_dt=open_dt,
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
    assert isinstance(result, dht.Trade)
    assert result.open_dt == open_dt
    assert result.direction == direction
    assert result.timeframe == timeframe
    assert result.trading_hours == trading_hours
    assert result.entry_price == entry_price
    assert result.open_drawdown == open_drawdown
    assert result.name == name
    # Validate default attributes
    # Note that some are adjusted by __init__ such as symbol
    assert result.close_drawdown is None
    assert result.close_dt is None
    assert result.exit_price is None
    assert result.gain_loss is None
    assert result.offset_ticks == 0
    assert result.drawdown_impact == 0
    assert isinstance(result.symbol, dhc.Symbol)
    assert result.symbol.ticker == "ES"
    assert result.contracts == 1
    assert result.contract_value == 50
    assert result.is_open
    assert result.profitable is None
    assert result.version == "1.0.0"
    assert result.ts_id is None
    assert result.bt_id is None
    # Validate calculated attributes
    assert isinstance(result.created_dt, str)
    assert isinstance(dt_as_dt(result.created_dt), datetime.datetime)
    assert isinstance(result.open_epoch, int)
    # Stop & profit tick & target calculations have their own test_ function
    # due to complexity, just make sure we got numbers for this part
    assert isinstance(result.stop_ticks, int)
    assert isinstance(result.prof_ticks, int)
    assert isinstance(result.stop_target, (int, float))
    assert isinstance(result.prof_target, (int, float))
    if result.direction == "long":
        assert result.flipper == 1
    else:
        assert result.flipper == -1

    return result


def test_dhtrades_Trade_tick_and_target_calculations_correct():
    # LONG Providing both accurately should result in the values as provided
    t = create_basic_trade(direction="long",
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
        create_basic_trade(direction="long",
                           entry_price=5000,
                           stop_ticks=250,
                           stop_target=4995,
                           )
    with pytest.raises(ValueError):
        create_basic_trade(direction="long",
                           entry_price=5000,
                           prof_ticks=250,
                           prof_target=5005,
                           )
    # LONG Providing ticks only should calculate accurate target
    t = create_basic_trade(direction="long",
                           entry_price=5000,
                           stop_ticks=20,
                           stop_target=None,
                           prof_ticks=20,
                           prof_target=None,
                           )
    assert t.stop_target == 4995
    assert t.prof_target == 5005
    # LONG Providing targets only should calculate accurate ticks
    t = create_basic_trade(direction="long",
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
        t = create_basic_trade(direction="long",
                               entry_price=5000,
                               stop_ticks=None,
                               stop_target=None,
                               )
    with pytest.raises(ValueError):
        t = create_basic_trade(direction="long",
                               entry_price=5000,
                               prof_ticks=None,
                               prof_target=None,
                               )
    # ### Short trades
    # SHORT Providing both accurately should result in the values as provided
    t = create_basic_trade(direction="short",
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
        create_basic_trade(direction="short",
                           entry_price=5000,
                           stop_ticks=250,
                           stop_target=5005,
                           )
    with pytest.raises(ValueError):
        create_basic_trade(direction="short",
                           entry_price=5000,
                           prof_ticks=250,
                           prof_target=4995,
                           )
    # SHORT Providing ticks only should calculate accurate target
    t = create_basic_trade(direction="short",
                           entry_price=5000,
                           stop_ticks=20,
                           stop_target=None,
                           prof_ticks=20,
                           prof_target=None,
                           )
    assert t.stop_target == 5005
    assert t.prof_target == 4995
    # SHORT Providing targets only should calculate accurate ticks
    t = create_basic_trade(direction="short",
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
        t = create_basic_trade(direction="short",
                               entry_price=5000,
                               stop_ticks=None,
                               stop_target=None,
                               )
    with pytest.raises(ValueError):
        t = create_basic_trade(direction="short",
                               entry_price=5000,
                               prof_ticks=None,
                               prof_target=None,
                               )


def test_dhtrades_Trade_creation_long_update_drawdown_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_basic_trade(direction="long")
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


def test_dhtrades_Trade_creation_long_update_drawdown_and_close_at_loss():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_basic_trade(direction="long")
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


def test_dhtrades_Trade_creation_short_update_drawdown_and_close_at_profit():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_basic_trade(direction="short",
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


def test_dhtrades_Trade_creation_short_update_drawdown_and_close_at_loss():
    # Create a trade (basic_trade() covers creation assertions)
    t = create_basic_trade(direction="short",
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




    #print("\nStoring trade")
    #print(t.store())

#    print("------------------------------------------------------------------")
#    t = Trade(open_dt="2025-01-02 12:01:00",
#              close_dt="2025-01-02 12:15:00",
#              direction="short",
#              timeframe="5m",
#              trading_hours="rth",
#              entry_price=5001.50,
#              stop_target=4995,
#              prof_target=5010,
#              open_drawdown=1000,
#              exit_price=4995,
#              name="DELETEMEToo"
#              )
#    print("Created closed short test trade with a gain.  This should have "
#          f"gain_loss == $325 and drawdown_impact == $325 (I think...)\n{t}")
#    print("\nUpdating drawdown_impact")
#    t.update_drawdown(price_seen=5009)
#    print(t)
#    print("\nStoring trade")
#    print(t.store())
#
#    print("\n\nReviewing trades in storage:")
#    print(dhs.review_trades(symbol="ES"))
#
#    print("\nDeleting name=DELETEMEToo trades first")
#    print(dhs.delete_trades(symbol="ES",
#                            field="name",
#                            value="DELETEMEToo",
#                            ))
#    print("\nReviewing trades in storage to confirm deletion:")
#    print(dhs.review_trades(symbol="ES"))
#    print("\nDeleting trades with name=DELETEME to finish cleanup")
#    print(dhs.delete_trades(symbol="ES",
#                            field="name",
#                            value="DELETEME",
#                            ))
#    print("\nReviewing trades in storage to confirm deletion:")
#    print(dhs.review_trades(symbol="ES"))




#
#    # TradeSeries
#    print("========================== TRADESERIES ===========================")
#    print("Creating a TradeSeries")
#    ts = TradeSeries(start_dt="2025-01-02 00:00:00",
#                     end_dt="2025-01-05 17:59:00",
#                     timeframe="5m",
#                     symbol="ES",
#                     name="DELETEME_Testing",
#                     params_str="1p_2s",
#                     trades=None,
#                     )
#    ts_id_to_delete = [ts.ts_id]
#    print(ts)
#    print("\nAdding two trades out of order")
#    ts.add_trade(Trade(open_dt="2025-01-03 12:00:00",
#                       close_dt="2025-01-03 12:15:00",
#                       direction="short",
#                       timeframe="5m",
#                       trading_hours="rth",
#                       entry_price=5001.50,
#                       stop_target=4995,
#                       prof_target=5010,
#                       open_drawdown=1000,
#                       exit_price=4995,
#                       name="DELETEME",
#                       ))
#    ts.add_trade(Trade(open_dt="2025-01-02 14:10:00",
#                       close_dt="2025-01-02 15:35:00",
#                       direction="short",
#                       timeframe="5m",
#                       trading_hours="rth",
#                       entry_price=5001.50,
#                       stop_target=4995,
#                       prof_target=5010,
#                       open_drawdown=1000,
#                       exit_price=4995,
#                       name="DELETEME"
#                       ))
#    print(ts.trades)
#    print("\nTesting .get_trade_by_open_dt() method returns a trade for "
#          "2025-01-02 14:10:00")
#    print(ts.get_trade_by_open_dt("2025-01-02 14:10:00"))
#    print("\nTesting .get_trade_by_open_dt() method returns a None for "
#          "2025-01-02 15:10:00")
#    print(ts.get_trade_by_open_dt("2025-01-02 15:10:00"))
#    print("\nCurrent order of trade open_dt fields")
#    for t in ts.trades:
#        print(t.open_dt)
#    print("\nrunning .sort_trades() to fix the ordering:")
#    ts.sort_trades()
#    for t in ts.trades:
#        print(t.open_dt)
#
#    print("\nStoring TradeSeries and child Trades")
#    print(ts.store(store_trades=True))
#    print("\n\nReviewing tradeseries in storage:")
#    print(dhs.review_tradeseries(symbol="ES"))
#    print("\n\nReviewing trades in storage:")
#    print(dhs.review_trades(symbol="ES"))
#
#    print("\nDeleting TradeSeries objects from mongo using ts_id")
#    for t in ts_id_to_delete:
#        print(dhs.delete_tradeseries(symbol="ES",
#                                     field="ts_id",
#                                     value=t
#                                     ))
#    print("\nDeleting Trade objects from mongo using ts_id")
#    print(dhs.delete_trades(symbol="ES",
#                            field="ts_id",
#                            value=ts.ts_id,
#                            ))
#    print("\nReviewing again to confirm deletion")
#    print("\n\nReviewing tradeseries in storage:")
#    print(dhs.review_tradeseries(symbol="ES"))
#    print("\n\nReviewing trades in storage:")
#    print(dhs.review_trades(symbol="ES"))
#




#    # Backtesters
#    print("======================== BACKTESTS================================")
#    print("Creating a Backtest object")
#    b = Backtest(start_dt="2025-01-02 00:00:00",
#                 end_dt="2025-01-04 00:00:00",
#                 symbol="ES",
#                 timeframe="e1h",
#                 trading_hours="eth",
#                 name="DELETEME_Testing",
#                 parameters={},
#                 autoload_charts=True,
#                 )
#    print(b)
#    print("\nAdding the previous test TradeSeries to this test Backtest")
#    b.add_tradeseries(ts)
#    print(b)
#    print("\nLet's make sure our Backtest has turtles all the way down, "
#          "i.e. complete set of child objects"
#          )
#    stuff = {"tradeseries": 0,
#             "trades": 0,
#             "charts": 0,
#             "tf_candles": 0,
#             "1m_candles": 0,
#             }
#    things = {"tradeseries": 1,
#              "trades": 2,
#              "charts": 2,
#              "tf_candles": 40,
#              "1m_candles": 2400,
#              }
#    if b.chart_tf is not None:
#        stuff["charts"] += 1
#        stuff["tf_candles"] += len(b.chart_tf.c_candles)
#    if b.chart_1m is not None:
#        stuff["charts"] += 1
#        stuff["1m_candles"] += len(b.chart_1m.c_candles)
#    if b.tradeseries is not None:
#        stuff["tradeseries"] += len(b.tradeseries)
#        for ts in b.tradeseries:
#            if ts.trades is not None:
#                stuff["trades"] += len(ts.trades)
#    print(f"Expected: {things}")
#    print(f"Received: {stuff}")
#    if stuff == things:
#        print("OK: They match!")
#    else:
#        print("ERROR: They don't match...")
#
#    print("------------------------------------------------------------------")
#    print("\nReviewing before storing all this junk:")
#    print("\nReviewing backtests in storage")
#    print(dhs.review_backtests(symbol="ES"))
#    print("\nReviewing tradeseries in storage:")
#    print(dhs.review_tradeseries(symbol="ES"))
#    print("\nReviewing trades in storage:")
#    print(dhs.review_trades(symbol="ES"))
#
#    print("\nStoring the backtest and it's child objects")
#    b.store(store_tradeseries=True,
#            store_trades=True,
#            )
#
#    print("\nReviewing after storing all this junk, we should see 1 Backtest, "
#          "1 TradeSeries, and 2 Trades all with 'DELETEME' in their names"
#          )
#    print("\nReviewing backtests in storage")
#    print(dhs.review_backtests(symbol="ES"))
#    print("\nReviewing tradeseries in storage:")
#    print(dhs.review_tradeseries(symbol="ES"))
#    print("\nReviewing trades in storage:")
#    print(dhs.review_trades(symbol="ES"))
#
#    print("\nAnd now we'll try to delete them all through the bt_id and ts_id "
#          "fields")
#    print(dhs.delete_backtests(symbol="ES",
#                               field="bt_id",
#                               value=b.bt_id,
#                               ))
#    for t in b.tradeseries:
#        print(dhs.delete_tradeseries(symbol="ES",
#                                     field="ts_id",
#                                     value=t.ts_id,
#                                     ))
#        print(dhs.delete_trades(symbol="ES",
#                                field="ts_id",
#                                value=t.ts_id,
#                                ))
#
#    print("\nReviewing after deletion, no 'DELETEME' objects should exist")
#    print("\nReviewing backtests in storage")
#    print(dhs.review_backtests(symbol="ES"))
#    print("\nReviewing tradeseries in storage:")
#    print(dhs.review_tradeseries(symbol="ES"))
#    print("\nReviewing trades in storage:")
#    print(dhs.review_trades(symbol="ES"))
