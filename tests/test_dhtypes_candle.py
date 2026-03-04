import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
from dhtypes import (
    Candle, Chart, Day, Event, Indicator, IndicatorDataPoint,
    IndicatorEMA, IndicatorSMA, Symbol)


def test_Candle_create_and_verify_pretty():
    out_candle = Candle(c_datetime="2025-01-02 12:00:00",
                        c_timeframe="1m",
                        c_open=5000,
                        c_high=5007.75,
                        c_low=4995.5,
                        c_close=5002,
                        c_volume=1501,
                        c_symbol="ES",
                        )
    assert isinstance(out_candle, Candle)
    assert len(out_candle.pretty().splitlines()) == 23
