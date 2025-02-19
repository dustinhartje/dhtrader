import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
import dhcharts as dhc
from dhutil import dt_as_dt, dt_as_str, dow_name

# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO confirm all other TODOs have been cleared from this file
# TODO Tests needed (some of these have already been written partially/fully
# Chart __init__stuff, confirm all attributes, check types, check any
#        scenarios where incorrect values can be passed, check any flags that
#        alter attribute calculations
# Chart __str__ and __repr__ return strings successfully
# Chart to.json and to_clean_dict  return correct types and mock values
# Chart.pretty()
# Chart __eq and __ne pass and fail scenarios
# Chart create and outputs (to_json, to_clean_dict, pretty, __str__, __repr__
# Chart store, retreive, delete
# Chart sort_candles - create random datetime candles then sort and confirm
#       each is > datetime of prior in a for loop
# Chart add_candle success and as many failure scenarios as I can dream up
# Chart load_candles success and fail scenarios, including loading a large
#       number from storage for various timeframes.
#       -- Perhaps map out a number of months with expected candle counts for
#          various timeframes and loop through them all, plus ensure last 30
#          days has some decent minimum number in each timeframe
#       -- also confirm rth and eth various timeframes don't load candles
#          outside of the trading_hours closed times
# Chart review_candles success and fail scenarios
#       -- confirm storage candles from bot() to reasonably current
#          (within a week?) for all timeframes
#       -- confirm earliest and latest actually are earliest and latest
#          it looks like I don't necessarily sort them first so maybe add
#          that in the process and then check each candle to confirm it's
#          >= earliest and <= latest
#       -- do a couple of chart setups ensuring I get correct values
#          for a few times on various timeframes including spanning holidays
#          and weekends and adjacent to them


# TODO Update this to be the new create and test_creation functions
def test_dhcharts_create_and_verify_pretty_all_classes():
    # Check line counts of pretty output, won't change unless class changes
    out_candle = dhc.Candle(c_datetime="2025-01-02 12:00:00",
                            c_timeframe="1m",
                            c_open=5000,
                            c_high=5007.75,
                            c_low=4995.5,
                            c_close=5002,
                            c_volume=1501,
                            c_symbol="ES",
                            )
    assert isinstance(out_candle, dhc.Candle)
    assert len(out_candle.pretty().splitlines()) == 22
    out_chart = dhc.Chart(c_timeframe="1m",
                          c_trading_hours="rth",
                          c_symbol="ES",
                          c_start="2025-01-02 12:00:00",
                          c_end="2025-01-02 12:10:00",
                          autoload=False,
                          )
    assert isinstance(out_chart, dhc.Chart)
    out_chart.add_candle(out_candle)
    assert len(out_chart.pretty().splitlines()) == 14
    assert len(out_chart.pretty(suppress_candles=False).splitlines()) == 35
