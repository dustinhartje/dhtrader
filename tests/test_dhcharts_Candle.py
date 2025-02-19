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
# Candle __init__stuff, confirm all attributes, check types, check any
#        scenarios where incorrect values can be passed, check any flags that
#        alter attribute calculations
# Candle __str__ and __repr__ return strings successfully
# Candle to.json and to_clean_dict  return correct types and mock values
# Candle.pretty()
# Candle __eq and __ne pass and fail scenarios
# Candle create and outputs (to_json, to_clean_dict, pretty, __str__, __repr__
# Candle store, retreive, delete
# Candle contains_datetime() pass and fail scenarios
# Candle contains_price() pass and fail scenarios


# TODO Update this to be the new create and test_creation functions
def test_dhcharts_create_and_verify_pretty_all_classes():
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
