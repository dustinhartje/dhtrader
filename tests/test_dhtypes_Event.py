import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
from dhtypes import (
    Candle, Chart, Day, Event, Indicator, IndicatorDataPoint,
    IndicatorEMA, IndicatorSMA, Symbol)


def test_Event_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    out_event = Event(start_dt="2025-01-02 12:00:00",
                      end_dt="2025-01-02 18:00:00",
                      symbol="ES",
                      category="Closed",
                      tags=["holiday"],
                      notes="Test Holiday",
                      )
    assert isinstance(out_event, Event)
    assert len(out_event.pretty().splitlines()) == 12
