import site
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str
# TODO Go through dhchars.py every function/class and write out comments
#      here for things that need testing
# TODO Check that list against below tests that were transfered from my
#      original manual testing hacks
# TODO write any remaining tests needed here or in class specific files

# Tests transferred from manual checks


def test_dhcharts_create_and_verify_pretty_all_classes():
    # Check line counts of pretty output, won't change unless class changes
    out_sym = dhc.Symbol(ticker="ES",
                         name="ES",
                         leverage_ratio=50.0,
                         tick_size=0.25,
                         )
    assert isinstance(out_sym, dhc.Symbol)
    assert len(out_sym.pretty().splitlines()) == 26
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
    out_event = dhc.Event(start_dt="2025-01-02 12:00:00",
                          end_dt="2025-01-02 18:00:00",
                          symbol="ES",
                          category="Closed",
                          tags=["holiday"],
                          notes="Test Holiday",
                          )
    assert isinstance(out_event, dhc.Event)
    assert len(out_event.pretty().splitlines()) == 12
    out_dp = dhc.IndicatorDataPoint(dt="2025-01-02 12:00:00",
                                    value=100,
                                    ind_id="ES1mTESTSMA-DELETEME9",
                                    )
    assert isinstance(out_dp, dhc.IndicatorDataPoint)
    assert len(out_dp.pretty().splitlines()) == 6

    out_ind = dhc.IndicatorSMA(name="TestSMA-DELETEME",
                               timeframe="5m",
                               trading_hours="eth",
                               symbol="ES",
                               description="yadda",
                               calc_version="yoda",
                               calc_details="yeeta",
                               start_dt="2025-01-08 09:30:00",
                               end_dt="2025-01-08 11:30:00",
                               parameters={"length": 9,
                                           "method": "close"
                                           },
                               candle_chart=out_chart,
                               )
    assert isinstance(out_ind, dhc.IndicatorSMA)
    out_ind.datapoints = [out_dp]
    assert len(out_ind.pretty().splitlines()) == 36
    assert len(out_ind.pretty(suppress_datapoints=False,
                              suppress_chart_candles=False,
                              ).splitlines()) == 62
    assert len(out_ind.get_info(pretty=True).splitlines()) == 17
