"""Tests for Indicator, IndicatorSMA, and IndicatorEMA calculation."""
import json
import pytest
from dhtrader import (
    Candle, Chart,
    delete_indicator, delete_indicators_by_name,
    get_indicator, get_indicator_datapoints,
    get_indicators_by_name, Indicator, IndicatorDataPoint,
    IndicatorEMA, IndicatorSMA, store_indicator, Symbol)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_rth_hod_creation_and_calculation():
    """Indicator creation, calculation, and RTH/HOD datapoint checks.

    Storage Usage: load_underlying_chart.
    """
    # Confirm RTH datapoints are calculated
    ind = Indicator(name="DELETEME-hod-demo",
                    description="Code testing use only",
                    timeframe="15m",
                    trading_hours="rth",
                    symbol="ES",
                    calc_version="1.0.0",
                    calc_details="super basic",
                    start_dt="2024-12-01 00:00:00",
                    end_dt="2025-01-31 00:00:00",
                    )
    ind.calculate()
    assert len(ind.datapoints) > 0
    # Datapoints should not exist on weekdays before or after rth hours
    assert ind.get_datapoint(dt="2025-01-02 03:37:00") is None
    assert ind.get_datapoint(dt="2025-01-02 16:01:00") is None
    assert ind.get_datapoint(dt="2025-01-03 08:59:00") is None
    assert ind.get_datapoint(dt="2025-01-03 19:30:00") is None

    # Datapoints should not exist on weekends at any time for RTH
    for d in ["04", "05", "11", "12", "18", "19", "25", "26"]:
        for t in ["03:30", "07:00", "09:29", "09:30", "10:50", "13:27",
                  "15:59", "16:00", "17:20", "18:01", "20:36", "23:59"]:
            assert ind.get_datapoint(dt=f"2025-01-{d} {t}:00") is None
    # Datapoints should not exist on weekdays before or after normal RTH hours
    for d in ["02", "03", "04", "05", "06",
              "09", "10", "11", "12", "13",
              "16", "17", "18", "19", "20",
              "23", "26", "27", "30", "31"]:
        for t in ["02:00", "08:30", "16:17", "18:08", "22:27", "23:59"]:
            assert ind.get_datapoint(dt=f"2024-12-{d} {t}:00") is None
    # Datapoints should exist on weekdays during normal RTH hours
    for d in ["02", "03", "04", "05", "06",
              "09", "10", "11", "12", "13",
              "16", "17", "18", "19", "20",
              "23", "26", "27", "30", "31"]:
        for t in ["09:30", "10:50", "11:08", "13:27", "14:30", "15:59"]:
            assert ind.get_datapoint(dt=f"2024-12-{d} {t}:00") is not None
    # Datapoints should not exist during holiday closures in normal RTH hours
    # Christmas Eve early close @ 1pm, Christmas Day closed all day
    for dt in ["12-24 13:21", "12-24 15:30", "12-25 09:30",
               "12-25 13:10", "12-25 15:00"]:
        assert ind.get_datapoint(dt=f"2024-{dt}:00") is None
    # Correct values should be returned during weekdays (spot checks)
        assert ind.get_datapoint(dt="2024-12-02 11:15:00").value == 6062
        assert ind.get_datapoint(dt="2024-12-12 13:00:00").value == 6087.75
        assert ind.get_datapoint(dt="2024-12-19 14:30:00").value == 6005.25
        assert ind.get_datapoint(dt="2024-12-30 12:15:00").value == 5981
        assert ind.get_datapoint(dt="2025-01-08 11:00:00").value == 5954
        assert ind.get_datapoint(dt="2025-01-15 14:15:00").value == 5993
        assert ind.get_datapoint(dt="2025-01-22 12:30:00").value == 6135.75
        assert ind.get_datapoint(dt="2025-01-28 11:45:00").value == 6087.25


def shared_assertions_Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2(i):
    """Assert ES ETH 5m EMA close l20 s2 datapoint values.

    NOTE: assertion values below need to be verified against TradingView
    or by running the corresponding calculated spotcheck test locally and
    printing each get_datapoint result for the datetimes below.
    """
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 1/5/25
    assert i.get_datapoint(dt="2025-01-05 18:00:00").value == 5986.32
    assert i.get_datapoint(dt="2025-01-05 20:34:00").value == 5985.85
    assert i.get_datapoint(dt="2025-01-05 23:59:00").value == 5986.45
    assert i.get_datapoint(dt="2025-01-05 15:45:00") is None
    # Mon 1/6/25
    assert i.get_datapoint(dt="2025-01-06 18:00:00").value == 6025.23
    assert i.get_datapoint(dt="2025-01-06 10:34:00").value == 6047.43
    assert i.get_datapoint(dt="2025-01-06 16:59:00").value == 6024.54
    assert i.get_datapoint(dt="2025-01-06 17:12:00") is None
    # Tue 1/7/25
    assert i.get_datapoint(dt="2025-01-07 18:00:00").value == 5956.17
    assert i.get_datapoint(dt="2025-01-07 12:15:00").value == 5988.62
    assert i.get_datapoint(dt="2025-01-07 16:59:00").value == 5956.48
    assert i.get_datapoint(dt="2025-01-07 17:24:00") is None


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculated_spotcheck_ES_eth_5m_EMA_close_l20_s2():
    """Test calculated spotcheck for ES ETH 5m EMA close l20 s2.

    Storage Usage: get_indicator, load_underlying_chart.
    """
    ind_calced = get_indicator(ind_id="ES_eth_5m_EMA_close_l20_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_calced.start_dt = "2024-12-29 00:00:00"
    ind_calced.end_dt = "2025-01-08 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    shared_assertions_Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2(
        ind_calced)


@pytest.mark.slow
@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_storage_spotcheck_ES_eth_5m_EMA_close_l20_s2():
    """Test storage spotcheck for ES ETH 5m EMA close l20 s2.

    Storage Usage: get_indicator, load_datapoints.
    """
    ind_stored = get_indicator(ind_id="ES_eth_5m_EMA_close_l20_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_stored.load_datapoints()
    shared_assertions_Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2(
        ind_stored)


# 5m ETH 9
def shared_assertions_Indicator_spotcheck_ES_eth_5m_EMA_close_l9_s2(i):
    """Assert ES ETH 5m EMA close l9 s2 datapoint values.

    NOTE: assertion values below need to be verified against TradingView
    or by running the corresponding calculated spotcheck test locally and
    printing each get_datapoint result for the datetimes below.
    """
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 1/5/25
    assert i.get_datapoint(dt="2025-01-05 18:00:00").value == 5986.39
    assert i.get_datapoint(dt="2025-01-05 20:34:00").value == 5985.06
    assert i.get_datapoint(dt="2025-01-05 23:59:00").value == 5987.10
    assert i.get_datapoint(dt="2025-01-05 15:45:00") is None
    # Mon 1/6/25
    assert i.get_datapoint(dt="2025-01-06 18:00:00").value == 6027.44
    assert i.get_datapoint(dt="2025-01-06 10:34:00").value == 6053.42
    assert i.get_datapoint(dt="2025-01-06 16:59:00").value == 6026.37
    assert i.get_datapoint(dt="2025-01-06 17:12:00") is None
    # Tue 1/7/25
    assert i.get_datapoint(dt="2025-01-07 18:00:00").value == 5954.96
    assert i.get_datapoint(dt="2025-01-07 12:15:00").value == 5982.14
    assert i.get_datapoint(dt="2025-01-07 16:59:00").value == 5955.38
    assert i.get_datapoint(dt="2025-01-07 17:24:00") is None


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculated_spotcheck_ES_eth_5m_EMA_close_l9_s2():
    """Test calculated spotcheck for ES ETH 5m EMA close l9 s2.

    Storage Usage: get_indicator, load_underlying_chart.
    """
    ind_calced = get_indicator(ind_id="ES_eth_5m_EMA_close_l9_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_calced.start_dt = "2024-12-29 00:00:00"
    ind_calced.end_dt = "2025-01-08 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    shared_assertions_Indicator_spotcheck_ES_eth_5m_EMA_close_l9_s2(
        ind_calced)


@pytest.mark.slow
@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_storage_spotcheck_ES_eth_5m_EMA_close_l9_s2():
    """Test storage spotcheck for ES ETH 5m EMA close l9 s2.

    Storage Usage: get_indicator, load_datapoints.
    """
    ind_stored = get_indicator(ind_id="ES_eth_5m_EMA_close_l9_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_stored.load_datapoints()
    shared_assertions_Indicator_spotcheck_ES_eth_5m_EMA_close_l9_s2(
        ind_stored)


# 15m ETH 9
def shared_assertions_Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2(i):
    """Assert ES ETH 15m EMA close l9 s2 datapoint values."""
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 2/9/25
    assert i.get_datapoint(dt="2025-02-09 18:00:00").value == 6048.67
    assert i.get_datapoint(dt="2025-02-09 20:34:00").value == 6062.08
    assert i.get_datapoint(dt="2025-02-09 23:59:00").value == 6064.90
    assert i.get_datapoint(dt="2025-02-09 15:45:00") is None
    # Mon 2/10/25
    assert i.get_datapoint(dt="2025-02-10 18:00:00").value == 6086.29
    assert i.get_datapoint(dt="2025-02-10 10:34:00").value == 6082.93
    assert i.get_datapoint(dt="2025-02-10 16:59:00").value == 6088.06
    assert i.get_datapoint(dt="2025-02-10 17:12:00") is None
    # Tue 2/11/25
    assert i.get_datapoint(dt="2025-02-11 18:00:00").value == 6090.27
    assert i.get_datapoint(dt="2025-02-11 12:15:00").value == 6080.61
    assert i.get_datapoint(dt="2025-02-11 16:59:00").value == 6090.58
    assert i.get_datapoint(dt="2025-02-11 17:24:00") is None
    # Wed 2/12/25
    assert i.get_datapoint(dt="2025-02-12 18:00:00").value == 6075.35
    assert i.get_datapoint(dt="2025-02-12 14:52:00").value == 6072.38
    assert i.get_datapoint(dt="2025-02-12 16:59:00").value == 6074.81
    assert i.get_datapoint(dt="2025-02-12 17:37:00") is None
    # Thu 2/13/25
    assert i.get_datapoint(dt="2025-02-13 18:00:00").value == 6130.98
    assert i.get_datapoint(dt="2025-02-13 09:20:00").value == 6078.08
    assert i.get_datapoint(dt="2025-02-13 16:59:00").value == 6129.97
    assert i.get_datapoint(dt="2025-02-13 17:45:00") is None
    # Fri 2/14/25
    assert i.get_datapoint(dt="2025-02-14 18:00:00") is None
    assert i.get_datapoint(dt="2025-02-14 16:34:00").value == 6133.21
    assert i.get_datapoint(dt="2025-02-14 16:59:00").value == 6133.37
    assert i.get_datapoint(dt="2025-02-14 21:47:00") is None
    # Sat 2/15/25
    assert i.get_datapoint(dt="2025-02-15 18:00:00") is None
    assert i.get_datapoint(dt="2025-02-15 20:34:00") is None
    assert i.get_datapoint(dt="2025-02-15 23:59:00") is None
    assert i.get_datapoint(dt="2025-02-15 15:45:00") is None
    # First and last candle of a different week
    assert i.get_datapoint(dt="2025-02-02 18:00:00").value == 6048.14
    assert i.get_datapoint(dt="2025-02-07 16:59:00").value == 6051.03


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculated_spotcheck_ES_eth_15m_EMA_close_l9_s2():
    """Spotcheck calculated EMA values.

    Storage Usage: get_indicator, load_underlying_chart.
    """
    ind_calced = get_indicator(ind_id="ES_eth_15m_EMA_close_l9_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_calced.start_dt = "2025-01-28 00:00:00"
    ind_calced.end_dt = "2025-02-16 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    shared_assertions_Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2(
        ind_calced)


@pytest.mark.slow
@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_storage_spotcheck_ES_eth_15m_EMA_close_l9_s2():
    """Spotcheck stored EMA values.

    Storage Usage: get_indicator, load_datapoints.
    """
    ind_stored = get_indicator(ind_id="ES_eth_15m_EMA_close_l9_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_stored.load_datapoints()
    shared_assertions_Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2(
        ind_stored)


# 15m ETH 20
def shared_assertions_Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2(i):
    """Assert ES ETH 15m EMA close l20 s2 datapoint values."""
    # Sun 12/8/24
    assert i.get_datapoint(dt="2024-12-08 18:00:00").value == 6096.55
    assert i.get_datapoint(dt="2024-12-08 23:05:00").value == 6096.91
    assert i.get_datapoint(dt="2024-12-08 23:59:00").value == 6096.26
    assert i.get_datapoint(dt="2024-12-08 11:47:00") is None
    # Mon 12/9/24
    assert i.get_datapoint(dt="2024-12-09 18:00:00").value == 6068.66
    assert i.get_datapoint(dt="2024-12-09 09:21:00").value == 6095.97
    assert i.get_datapoint(dt="2024-12-09 16:59:00").value == 6069.44
    assert i.get_datapoint(dt="2024-12-09 17:01:00") is None
    # Tue 12/10/24
    assert i.get_datapoint(dt="2024-12-10 18:00:00").value == 6054.87
    assert i.get_datapoint(dt="2024-12-10 10:52:00").value == 6068.51
    assert i.get_datapoint(dt="2024-12-10 16:59:00").value == 6055.25
    assert i.get_datapoint(dt="2024-12-10 17:59:00") is None
    # Wed 12/11/24
    assert i.get_datapoint(dt="2024-12-11 18:00:00").value == 6091.01
    assert i.get_datapoint(dt="2024-12-11 11:18:00").value == 6074.16
    assert i.get_datapoint(dt="2024-12-11 16:59:00").value == 6091.32
    assert i.get_datapoint(dt="2024-12-11 17:30:00") is None
    # Thu 12/12/24
    assert i.get_datapoint(dt="2024-12-12 18:00:00").value == 6067.47
    assert i.get_datapoint(dt="2024-12-12 14:36:00").value == 6076.89
    assert i.get_datapoint(dt="2024-12-12 16:59:00").value == 6067.97
    assert i.get_datapoint(dt="2024-12-12 17:21:00") is None
    # Fri 12/13/24
    assert i.get_datapoint(dt="2024-12-13 18:00:00") is None
    assert i.get_datapoint(dt="2024-12-13 11:11:00").value == 6069.36
    assert i.get_datapoint(dt="2024-12-13 16:59:00").value == 6053.99
    assert i.get_datapoint(dt="2024-12-13 19:00:00") is None
    # Sat 12/14/24
    assert i.get_datapoint(dt="2024-12-14 18:00:00") is None
    assert i.get_datapoint(dt="2024-12-14 12:30:00") is None
    assert i.get_datapoint(dt="2024-12-14 23:59:00") is None
    assert i.get_datapoint(dt="2024-12-14 04:12:00") is None
    # First and last candle of a different week
    assert i.get_datapoint(dt="2024-12-01 18:00:00").value == 6045.34
    assert i.get_datapoint(dt="2024-12-06 16:59:00").value == 6096.39


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculated_spotcheck_ES_eth_15m_EMA_close_l20_s2():
    """Spotcheck calculated EMA values.

    Storage Usage: get_indicator, load_underlying_chart.
    """
    ind_calced = get_indicator(ind_id="ES_eth_15m_EMA_close_l20_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_calced.start_dt = "2024-11-27 00:00:00"
    ind_calced.end_dt = "2024-12-15 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    shared_assertions_Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2(
        ind_calced)


@pytest.mark.slow
@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_storage_spotcheck_ES_eth_15m_EMA_close_l20_s2():
    """Spotcheck stored EMA values.

    Storage Usage: get_indicator, load_datapoints.
    """
    ind_stored = get_indicator(ind_id="ES_eth_15m_EMA_close_l20_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_stored.load_datapoints()
    shared_assertions_Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2(
        ind_stored)


# e1h ETH 9
def shared_assertions_Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2(i):
    """Assert ES ETH e1h EMA close l9 s2 datapoint values."""
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 11/10/24
    assert i.get_datapoint(dt="2024-11-10 18:00:00").value == 6025.22
    assert i.get_datapoint(dt="2024-11-10 18:55:00").value == 6025.22
    assert i.get_datapoint(dt="2024-11-10 21:01:00").value == 6029.39
    assert i.get_datapoint(dt="2024-11-10 14:25:00") is None
    # Mon 11/11/24
    assert i.get_datapoint(dt="2024-11-11 18:00:00").value == 6031.34
    assert i.get_datapoint(dt="2024-11-11 13:17:00").value == 6034.95
    assert i.get_datapoint(dt="2024-11-11 16:59:00").value == 6032.24
    assert i.get_datapoint(dt="2024-11-11 17:28:00") is None
    # Tue 11/12/24
    assert i.get_datapoint(dt="2024-11-12 18:00:00").value == 6013.94
    assert i.get_datapoint(dt="2024-11-12 14:19:00").value == 6017.15
    assert i.get_datapoint(dt="2024-11-12 16:59:00").value == 6015.42
    assert i.get_datapoint(dt="2024-11-12 17:37:00") is None
    # Wed 11/13/24
    assert i.get_datapoint(dt="2024-11-13 18:00:00").value == 6018.26
    assert i.get_datapoint(dt="2024-11-13 13:08:00").value == 6018.90
    assert i.get_datapoint(dt="2024-11-13 16:59:00").value == 6017.77
    assert i.get_datapoint(dt="2024-11-13 17:52:00") is None
    # Thu 11/14/24
    assert i.get_datapoint(dt="2024-11-14 18:00:00").value == 5986.26
    assert i.get_datapoint(dt="2024-11-14 13:56:00").value == 6000.96
    assert i.get_datapoint(dt="2024-11-14 16:59:00").value == 5991.02
    assert i.get_datapoint(dt="2024-11-14 17:44:00") is None
    # Fri 11/15/24
    assert i.get_datapoint(dt="2024-11-15 18:00:00") is None
    assert i.get_datapoint(dt="2024-11-15 10:48:00").value == 5935.75
    assert i.get_datapoint(dt="2024-11-15 16:59:00").value == 5904.87
    assert i.get_datapoint(dt="2024-11-15 22:45:00") is None
    # Sat 11/16/24
    assert i.get_datapoint(dt="2024-11-16 18:00:00") is None
    assert i.get_datapoint(dt="2024-11-16 04:10:00") is None
    assert i.get_datapoint(dt="2024-11-16 23:59:00") is None
    assert i.get_datapoint(dt="2024-11-16 12:12:00") is None
    # First and last candle of a different week
    assert i.get_datapoint(dt="2024-11-03 18:00:00").value == 5762.53
    assert i.get_datapoint(dt="2024-11-08 16:59:00").value == 6022.15


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculated_spotcheck_ES_eth_e1h_EMA_close_l9_s2():
    """Spotcheck calculated EMA values.

    Storage Usage: get_indicator, load_underlying_chart.
    """
    ind_calced = get_indicator(ind_id="ES_eth_e1h_EMA_close_l9_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_calced.start_dt = "2024-10-28 00:00:00"
    ind_calced.end_dt = "2024-11-17 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    shared_assertions_Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2(
        ind_calced)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_storage_spotcheck_ES_eth_e1h_EMA_close_l9_s2():
    """Spotcheck stored EMA values.

    Storage Usage: get_indicator, load_datapoints.
    """
    ind_stored = get_indicator(ind_id="ES_eth_e1h_EMA_close_l9_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_stored.load_datapoints()
    shared_assertions_Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2(
        ind_stored)


# e1h ETH 20
def shared_assertions_Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2(i):
    """Assert ES ETH e1h EMA close l20 s2 datapoint values."""
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 10/13/24
    assert i.get_datapoint(dt="2024-10-13 18:00:00").value == 5844.16
    assert i.get_datapoint(dt="2024-10-13 20:18:00").value == 5846.19
    assert i.get_datapoint(dt="2024-10-13 23:38:00").value == 5848.36
    assert i.get_datapoint(dt="2024-10-13 08:50:00") is None
    # Mon 10/14/24
    assert i.get_datapoint(dt="2024-10-14 18:00:00").value == 5886.73
    assert i.get_datapoint(dt="2024-10-14 02:08:00").value == 5850.69
    assert i.get_datapoint(dt="2024-10-14 16:59:00").value == 5884.13
    assert i.get_datapoint(dt="2024-10-14 17:44:00") is None
    # Tue 10/15/24
    assert i.get_datapoint(dt="2024-10-15 18:00:00").value == 5885.24
    assert i.get_datapoint(dt="2024-10-15 06:51:00").value == 5902.51
    assert i.get_datapoint(dt="2024-10-15 16:59:00").value == 5887.58
    assert i.get_datapoint(dt="2024-10-15 17:24:00") is None
    # Wed 10/16/24
    assert i.get_datapoint(dt="2024-10-16 18:00:00").value == 5875.07
    assert i.get_datapoint(dt="2024-10-16 15:22:00").value == 5873.35
    assert i.get_datapoint(dt="2024-10-16 16:59:00").value == 5874.34
    assert i.get_datapoint(dt="2024-10-16 17:11:00") is None
    # Thu 10/17/24
    assert i.get_datapoint(dt="2024-10-17 18:00:00").value == 5895.16
    assert i.get_datapoint(dt="2024-10-17 06:20:00").value == 5887.80
    assert i.get_datapoint(dt="2024-10-17 16:59:00").value == 5895.49
    assert i.get_datapoint(dt="2024-10-17 17:19:00") is None
    # Fri 10/18/24
    assert i.get_datapoint(dt="2024-10-18 18:00:00") is None
    assert i.get_datapoint(dt="2024-10-18 13:03:00").value == 5896.93
    assert i.get_datapoint(dt="2024-10-18 16:59:00").value == 5899.72
    assert i.get_datapoint(dt="2024-10-18 23:58:00") is None
    # Sat 10/19/24
    assert i.get_datapoint(dt="2024-10-19 18:00:00") is None
    assert i.get_datapoint(dt="2024-10-19 15:39:00") is None
    assert i.get_datapoint(dt="2024-10-19 23:59:00") is None
    assert i.get_datapoint(dt="2024-10-19 03:25:00") is None
    # First and last candle of a different week
    # Minor disagreement between my system and TV of only a penny here.
    # This may be problematic if I try to wrap this all in another set of
    # functions.  Push comes to shove just change this to a different week
    # that doesn't have the one-penny-off problem if that simplifies coding
    # and improves coverage
    assert i.get_datapoint(dt="2024-10-06 18:00:00").value in [5774.88,
                                                               5774.89]
    assert i.get_datapoint(dt="2024-10-11 16:59:00").value == 5842.65


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculated_spotcheck_ES_eth_e1h_EMA_close_l20_s2():
    """Spotcheck calculated EMA values.

    Storage Usage: get_indicator, load_underlying_chart.
    """
    ind_calced = get_indicator(ind_id="ES_eth_e1h_EMA_close_l20_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_calced.start_dt = "2024-10-01 00:00:00"
    ind_calced.end_dt = "2024-10-20 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    shared_assertions_Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2(
        ind_calced)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_storage_spotcheck_ES_eth_e1h_EMA_close_l20_s2():
    """Spotcheck stored EMA values.

    Storage Usage: get_indicator, load_datapoints.
    """
    ind_stored = get_indicator(ind_id="ES_eth_e1h_EMA_close_l20_s2",
                               autoload_datapoints=False,
                               autoload_chart=True,
                               )
    ind_stored.load_datapoints()
    shared_assertions_Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2(
        ind_stored)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_calculate():
    """Test for Indicator creation and basic calculation results.

    Storage Usage: load_underlying_chart.
    """
    # Building 5m9sma for 2025-01-08 9:30am-11:30am
    itest = IndicatorSMA(name="TestSMA-DELETEME",
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
                         )
    itest.load_underlying_chart()
    itest.calculate()
    assert len(itest.candle_chart.c_candles) == 25
    assert len(itest.datapoints) == 17

    # Building e1h9ema for 2025-01-08 - 2025-01-12
    # A test that spans a weekend closure covers most edge cases
    itest = IndicatorEMA(name="TestEMA-DELETEME",
                         timeframe="e1h",
                         trading_hours="eth",
                         symbol="ES",
                         description="yadda",
                         calc_version="yoda",
                         calc_details="yeeta",
                         start_dt="2025-01-08 00:00:00",
                         end_dt="2025-01-12 20:00:00",
                         parameters={"length": 9,
                                     "method": "close"
                                     },
                         )
    itest.load_underlying_chart()
    itest.calculate()
    # Validate candles as expected
    assert len(itest.candle_chart.c_candles) == 59
    expected_id = "ES_eth_e1h_TestEMA-DELETEME_close_l9_s2"
    expected = [IndicatorDataPoint(dt='2025-01-10 15:00:00',
                                   value=5890.25,
                                   ind_id=expected_id,
                                   epoch=1736539200),
                IndicatorDataPoint(dt='2025-01-10 16:00:00',
                                   value=5884.5,
                                   ind_id=expected_id,
                                   epoch=1736542800),
                IndicatorDataPoint(dt='2025-01-12 18:00:00',
                                   value=5879.6,
                                   ind_id=expected_id,
                                   epoch=1736722800),
                IndicatorDataPoint(dt='2025-01-12 19:00:00',
                                   value=5875.38,
                                   ind_id=expected_id,
                                   epoch=1736726400),
                IndicatorDataPoint(dt='2025-01-12 20:00:00',
                                   value=5869.3,
                                   ind_id=expected_id,
                                   epoch=1736730000),
                ]
    calculated = itest.datapoints[-5:]
    for i in range(5):
        assert expected[i] == calculated[i]


def clear_indicator_storage_by_name(name: str):
    """Delete all stored Indicators and datapoints with the given name."""
    delete_indicators_by_name(name)
    stored = get_indicators_by_name(name)
    assert len(stored) == 0


@pytest.fixture
def cleanup_indicator_storage():
    """Register Indicator names for pre- and post-test cleanup.

    The returned helper records each supplied name, immediately clears
    any matching Indicators and datapoints before the test continues,
    then clears the full registered set again during fixture teardown.
    """
    names = set()

    def register(*new_names):
        for name in new_names:
            names.add(name)
        for name in sorted(names):
            clear_indicator_storage_by_name(name)

    yield register

    for name in sorted(names):
        clear_indicator_storage_by_name(name)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_get_datapoints():
    """Test for Indicator get_datapoint, next, and prev methods.

    Storage Usage: load_underlying_chart.
    """
    itest = IndicatorEMA(name="TestEMA-DELETEME",
                         timeframe="e1h",
                         trading_hours="eth",
                         symbol="ES",
                         description="yadda",
                         calc_version="yoda",
                         calc_details="yeeta",
                         start_dt="2025-01-08 00:00:00",
                         end_dt="2025-01-12 20:00:00",
                         parameters={"length": 9,
                                     "method": "close"
                                     },
                         )
    itest.load_underlying_chart()
    itest.calculate()
    # Testing getting datapoints by dt using 2025-01-12 18:00:00
    dp_dt = "2025-01-12 18:00:00"
    # Current 2025-01-12 18:00:00 is expected (Sunday 6pm)")
    assert itest.get_datapoint(dt=dp_dt).dt == "2025-01-12 18:00:00"
    assert itest.get_datapoint(dt=dp_dt).value == 5879.6
    assert itest.next_datapoint(dt=dp_dt).dt == "2025-01-12 19:00:00"
    assert itest.next_datapoint(dt=dp_dt).value == 5875.38
    assert itest.prev_datapoint(dt=dp_dt).dt == "2025-01-10 16:00:00"
    assert itest.prev_datapoint(dt=dp_dt).value == 5884.5


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Indicator_store_retrieve_delete(cleanup_indicator_storage):
    """Verify Indicator and IndicatorDataPoint storage, retrieval, deletion.

    Storage Usage: store_indicator, get_indicator, delete_indicators_by_name,
                   get_indicator_datapoints, get_indicators_by_name.
    """
    name = "TestEMA-DELETEME"
    ind_id = "ES_eth_e1h_TestEMA-DELETEME_close_l9_s2"
    cleanup_indicator_storage(name)

    # Create and calculate an IndicatorEMA spanning a weekend (edge cases)
    itest = IndicatorEMA(name=name,
                         timeframe="e1h",
                         trading_hours="eth",
                         symbol="ES",
                         description="yadda",
                         calc_version="yoda",
                         calc_details="yeeta",
                         start_dt="2025-01-08 00:00:00",
                         end_dt="2025-01-12 20:00:00",
                         parameters={"length": 9,
                                     "method": "close"
                                     },
                         )
    itest.load_underlying_chart()
    itest.calculate()

    # Confirm Indicator is not in storage before we begin
    retrieved = get_indicator(ind_id=ind_id,
                              autoload_datapoints=False,
                              autoload_chart=False)
    assert retrieved is None

    # Store Indicator and its IndicatorDataPoints
    result = store_indicator(itest, store_datapoints=True)
    # Confirm storage result references the correct Indicator
    assert result["indicator"]["ind_id"] == ind_id

    # Verify Indicator retrieval - dates must be set as they don't get stored
    retrieved = get_indicator(ind_id=ind_id,
                              autoload_datapoints=True,
                              autoload_chart=True)
    retrieved.start_dt = "2025-01-08 00:00:00"
    retrieved.end_dt = "2025-01-12 20:00:00"
    retrieved.load_underlying_chart()
    assert retrieved == itest
    assert isinstance(retrieved, IndicatorEMA)
    assert retrieved.ind_id == ind_id

    # Verify IndicatorDataPoint retrieval
    datapoints = get_indicator_datapoints(ind_id=ind_id)
    assert len(datapoints) == 23
    assert all(isinstance(dp, IndicatorDataPoint) for dp in datapoints)
    assert datapoints[0].ind_id == ind_id
    assert datapoints[0].name == name
    assert datapoints[5].ind_id == ind_id
    assert datapoints[5].name == name

    # Verify incremental storage skips existing datapoints - extend date range
    # and re-store; existing 23 should be skipped, new 46 added (2 days added)
    itest.end_dt = "2025-01-14 20:00:00"
    itest.load_underlying_chart()
    itest.calculate()
    result = store_indicator(itest, store_datapoints=True)
    assert result["datapoints_stored"] == 46
    assert result["datapoints_skipped"] == 23

    # Delete by name and confirm Indicator and all IndicatorDataPoints gone
    delete_indicators_by_name(name)
    stored = get_indicators_by_name(name)
    assert len(stored) == 0
    dps = get_indicator_datapoints(ind_id=ind_id)
    assert len(dps) == 0


# #############################################################################
# IndicatorDataPoint
# #############################################################################

@pytest.mark.suppress_stdout
def test_IndicatorDataPoint_create_and_verify_common_methods():
    """Test IndicatorDataPoint __init__ values, __eq__, __ne__, __str__,
    __repr__, to_clean_dict, to_json, and pretty.

    IndicatorDataPoint does not define brief.
    """
    from dhtrader import dt_to_epoch
    dp = IndicatorDataPoint(
        dt="2099-01-15 10:30:00",
        value=5123.75,
        ind_id="ES_eth_15m_EMA_close_l9_s2",
        name="DELETEME-test-dp",
    )
    assert isinstance(dp, IndicatorDataPoint)
    # __init__
    assert dp.dt == "2099-01-15 10:30:00"
    assert dp.value == 5123.75
    assert dp.ind_id == "ES_eth_15m_EMA_close_l9_s2"
    assert dp.name == "DELETEME-test-dp"
    assert isinstance(dp.epoch, int)
    assert dp.epoch == dt_to_epoch("2099-01-15 10:30:00")
    # Explicit epoch overrides auto-calculation
    dp_explicit = IndicatorDataPoint(
        dt="2099-01-15 10:30:00",
        value=100.0,
        ind_id="test_ind",
        epoch=9999,
        name="DELETEME-test-dp-explicit",
    )
    assert dp_explicit.epoch == 9999
    expected_attrs = {"dt", "epoch", "ind_id", "name", "value"}
    actual_attrs = set(vars(dp).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "IndicatorDataPoint attributes changed. Update "
        "this test's __init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    dp2 = IndicatorDataPoint(
        dt="2099-01-15 10:30:00",
        value=5123.75,
        ind_id="ES_eth_15m_EMA_close_l9_s2",
        name="DELETEME-test-dp",
    )
    diff = IndicatorDataPoint(
        dt="2099-01-15 10:30:00",
        value=5001.0,
        ind_id="test_ind",
        name="DELETEME-test-dp-diff",
    )
    # __eq__
    assert dp == dp2
    assert not (dp == diff)
    assert not (dp == [])
    # __ne__
    assert not (dp != dp2)
    assert dp != diff
    assert dp != []
    # __str__
    assert isinstance(str(dp), str)
    assert len(str(dp)) > 0
    # __repr__
    assert isinstance(repr(dp), str)
    assert str(dp) == repr(dp)
    # to_clean_dict
    d = dp.to_clean_dict()
    assert isinstance(d, dict)
    assert d["dt"] == "2099-01-15 10:30:00"
    assert d["value"] == 5123.75
    assert d["ind_id"] == "ES_eth_15m_EMA_close_l9_s2"
    assert d["name"] == "DELETEME-test-dp"
    assert "epoch" in d
    # to_json
    j = dp.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["dt"] == "2099-01-15 10:30:00"
    assert parsed["value"] == 5123.75
    assert parsed["name"] == "DELETEME-test-dp"
    # pretty
    p = dp.pretty()
    assert isinstance(p, str)
    assert len(p.splitlines()) == 7


def _make_indicator_chart():
    """Create a minimal non-storage Chart for Indicator tests."""
    candle = Candle(c_datetime="2099-01-02 12:00:00",
                    c_timeframe="1m",
                    c_open=5000,
                    c_high=5007.75,
                    c_low=4995.5,
                    c_close=5002,
                    c_volume=1501,
                    c_symbol="ES",
                    )
    chart = Chart(c_timeframe="1m",
                  c_trading_hours="eth",
                  c_symbol="ES",
                  c_start="2099-01-02 12:00:00",
                  c_end="2099-01-02 12:10:00",
                  autoload=False,
                  )
    chart.add_candle(candle)
    return chart


@pytest.mark.suppress_stdout
def test_Indicator_create_and_verify_common_methods():
    """Test Indicator __init__ values, __eq__, __ne__, __str__, __repr__,
    to_clean_dict, to_json, and pretty.

    Indicator does not define brief.
    """
    chart = _make_indicator_chart()
    ind = Indicator(name="DELETEME",
                    description="Test indicator",
                    timeframe="1m",
                    trading_hours="eth",
                    symbol="ES",
                    calc_version="1.0.0",
                    calc_details="test",
                    start_dt="2099-01-02 12:00:00",
                    end_dt="2099-01-02 12:10:00",
                    autoload_chart=False,
                    candle_chart=chart,
                    )
    ind2 = Indicator(name="DELETEME",
                     description="Test indicator",
                     timeframe="1m",
                     trading_hours="eth",
                     symbol="ES",
                     calc_version="1.0.0",
                     calc_details="test",
                     start_dt="2099-01-02 12:00:00",
                     end_dt="2099-01-02 12:10:00",
                     autoload_chart=False,
                     candle_chart=chart,
                     )
    diff = Indicator(name="DIFFERENT",
                     description="Different indicator",
                     timeframe="1m",
                     trading_hours="eth",
                     symbol="ES",
                     calc_version="1.0.0",
                     calc_details="test",
                     start_dt="2099-01-02 12:00:00",
                     end_dt="2099-01-02 12:10:00",
                     autoload_chart=False,
                     candle_chart=chart,
                     )
    assert isinstance(ind, Indicator)
    # __init__
    assert ind.name == "DELETEME"
    assert ind.description == "Test indicator"
    assert ind.timeframe == "1m"
    assert ind.trading_hours == "eth"
    assert ind.symbol.ticker == "ES"
    assert ind.calc_version == "1.0.0"
    assert ind.calc_details == "test"
    assert ind.start_dt == "2099-01-02 12:00:00"
    assert ind.end_dt == "2099-01-02 12:10:00"
    assert ind.datapoints == []
    assert ind.parameters == {}
    assert "ES" in ind.ind_id
    assert "eth" in ind.ind_id
    assert "1m" in ind.ind_id
    assert "DELETEME" in ind.ind_id
    assert ind.class_name == "Indicator"
    assert ind.autoload_chart is False
    assert ind.candle_chart == chart
    expected_attrs = {
        "autoload_chart", "calc_details", "calc_version",
        "candle_chart", "class_name", "datapoints",
        "description", "end_dt", "ind_id", "name",
        "parameters", "start_dt", "symbol", "timeframe",
        "trading_hours",
    }
    actual_attrs = set(vars(ind).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "Indicator attributes changed. Update this test's "
        "__init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    # __eq__
    assert ind == ind2
    assert not (ind == diff)
    # __ne__
    assert not (ind != ind2)
    assert ind != diff
    # __str__
    assert isinstance(str(ind), str)
    assert len(str(ind)) > 0
    # __repr__
    assert isinstance(repr(ind), str)
    assert str(ind) == repr(ind)
    # to_clean_dict
    d = ind.to_clean_dict()
    assert isinstance(d, dict)
    assert d["name"] == "DELETEME"
    assert d["timeframe"] == "1m"
    assert d["trading_hours"] == "eth"
    # to_json
    j = ind.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "DELETEME"
    assert parsed["timeframe"] == "1m"
    # pretty
    p = ind.pretty()
    assert isinstance(p, str)
    assert "\n" in p
    assert "DELETEME" in p


@pytest.mark.suppress_stdout
def test_IndicatorSMA_create_and_verify_common_methods():
    """Test IndicatorSMA __init__ values, __eq__, __ne__, __str__, __repr__,
    to_clean_dict, to_json, and pretty.

    IndicatorSMA inherits common methods from Indicator and does not
    define brief.
    """
    chart = _make_indicator_chart()
    params = {"length": 3, "method": "close"}
    sma = IndicatorSMA(description="Test SMA",
                       timeframe="1m",
                       trading_hours="eth",
                       symbol="ES",
                       calc_version="1.0.0",
                       calc_details="test",
                       start_dt="2099-01-02 12:00:00",
                       end_dt="2099-01-02 12:10:00",
                       autoload_chart=False,
                       candle_chart=chart,
                       parameters=params,
                       )
    sma2 = IndicatorSMA(description="Test SMA",
                        timeframe="1m",
                        trading_hours="eth",
                        symbol="ES",
                        calc_version="1.0.0",
                        calc_details="test",
                        start_dt="2099-01-02 12:00:00",
                        end_dt="2099-01-02 12:10:00",
                        autoload_chart=False,
                        candle_chart=chart,
                        parameters=params,
                        )
    diff_params = {"length": 9, "method": "close"}
    diff = IndicatorSMA(description="Test SMA",
                        timeframe="1m",
                        trading_hours="eth",
                        symbol="ES",
                        calc_version="1.0.0",
                        calc_details="test",
                        start_dt="2099-01-02 12:00:00",
                        end_dt="2099-01-02 12:10:00",
                        autoload_chart=False,
                        candle_chart=chart,
                        parameters=diff_params,
                        )
    assert isinstance(sma, IndicatorSMA)
    # __init__
    assert sma.name == "SMA"
    assert sma.description == "Test SMA"
    assert sma.timeframe == "1m"
    assert sma.trading_hours == "eth"
    assert sma.symbol.ticker == "ES"
    assert sma.calc_version == "1.0.0"
    assert sma.calc_details == "test"
    assert sma.start_dt == "2099-01-02 12:00:00"
    assert sma.end_dt == "2099-01-02 12:10:00"
    assert sma.datapoints == []
    assert sma.parameters == params
    assert sma.length == 3
    assert sma.method == "close"
    assert "close" in sma.ind_id
    assert "l3" in sma.ind_id
    assert sma.class_name == "IndicatorSMA"
    assert sma.autoload_chart is False
    assert sma.candle_chart == chart
    expected_attrs = {
        "autoload_chart", "calc_details", "calc_version",
        "candle_chart", "class_name", "datapoints",
        "description", "end_dt", "ind_id", "length",
        "method", "name", "parameters", "start_dt",
        "symbol", "timeframe", "trading_hours",
    }
    actual_attrs = set(vars(sma).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "IndicatorSMA attributes changed. Update this "
        "test's __init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    # __eq__
    assert sma == sma2
    assert not (sma == diff)
    # __ne__
    assert not (sma != sma2)
    assert sma != diff
    # __str__
    assert isinstance(str(sma), str)
    assert len(str(sma)) > 0
    # __repr__
    assert isinstance(repr(sma), str)
    assert str(sma) == repr(sma)
    # to_clean_dict
    d = sma.to_clean_dict()
    assert isinstance(d, dict)
    assert d["name"] == "SMA"
    assert d["timeframe"] == "1m"
    # to_json
    j = sma.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "SMA"
    # pretty
    p = sma.pretty()
    assert isinstance(p, str)
    assert "\n" in p
    assert "SMA" in p


@pytest.mark.suppress_stdout
def test_IndicatorEMA_create_and_verify_common_methods():
    """Test IndicatorEMA __init__ values, __eq__, __ne__, __str__, __repr__,
    to_clean_dict, to_json, and pretty.

    IndicatorEMA inherits common methods from Indicator and does not
    define brief.
    """
    chart = _make_indicator_chart()
    params = {"length": 3, "method": "close", "smoothing": 2}
    ema = IndicatorEMA(description="Test EMA",
                       timeframe="1m",
                       trading_hours="eth",
                       symbol="ES",
                       calc_version="1.0.0",
                       calc_details="test",
                       start_dt="2099-01-02 12:00:00",
                       end_dt="2099-01-02 12:10:00",
                       autoload_chart=False,
                       candle_chart=chart,
                       parameters=params,
                       )
    ema2 = IndicatorEMA(description="Test EMA",
                        timeframe="1m",
                        trading_hours="eth",
                        symbol="ES",
                        calc_version="1.0.0",
                        calc_details="test",
                        start_dt="2099-01-02 12:00:00",
                        end_dt="2099-01-02 12:10:00",
                        autoload_chart=False,
                        candle_chart=chart,
                        parameters=params,
                        )
    diff_params = {"length": 9, "method": "close", "smoothing": 2}
    diff = IndicatorEMA(description="Test EMA",
                        timeframe="1m",
                        trading_hours="eth",
                        symbol="ES",
                        calc_version="1.0.0",
                        calc_details="test",
                        start_dt="2099-01-02 12:00:00",
                        end_dt="2099-01-02 12:10:00",
                        autoload_chart=False,
                        candle_chart=chart,
                        parameters=diff_params,
                        )
    assert isinstance(ema, IndicatorEMA)
    # __init__
    assert ema.name == "EMA"
    assert ema.description == "Test EMA"
    assert ema.timeframe == "1m"
    assert ema.trading_hours == "eth"
    assert ema.symbol.ticker == "ES"
    assert ema.calc_version == "1.0.0"
    assert ema.calc_details == "test"
    assert ema.start_dt == "2099-01-02 12:00:00"
    assert ema.end_dt == "2099-01-02 12:10:00"
    assert ema.datapoints == []
    assert ema.parameters == params
    assert ema.length == 3
    assert ema.method == "close"
    assert ema.smoothing == 2
    assert "close" in ema.ind_id
    assert "l3" in ema.ind_id
    assert "s2" in ema.ind_id
    assert ema.class_name == "IndicatorEMA"
    assert ema.autoload_chart is False
    assert ema.candle_chart == chart
    expected_attrs = {
        "autoload_chart", "calc_details", "calc_version",
        "candle_chart", "class_name", "datapoints",
        "description", "end_dt", "ind_id", "length",
        "method", "name", "parameters", "smoothing",
        "start_dt", "symbol", "timeframe", "trading_hours",
    }
    actual_attrs = set(vars(ema).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "IndicatorEMA attributes changed. Update this "
        "test's __init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    # __eq__
    assert ema == ema2
    assert not (ema == diff)
    # __ne__
    assert not (ema != ema2)
    assert ema != diff
    # __str__
    assert isinstance(str(ema), str)
    assert len(str(ema)) > 0
    # __repr__
    assert isinstance(repr(ema), str)
    assert str(ema) == repr(ema)
    # to_clean_dict
    d = ema.to_clean_dict()
    assert isinstance(d, dict)
    assert d["name"] == "EMA"
    assert d["timeframe"] == "1m"
    # to_json
    j = ema.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "EMA"
    # pretty
    p = ema.pretty()
    assert isinstance(p, str)
    assert "\n" in p
    assert "EMA" in p


def test_IndicatorDataPoint_eq_covers_all_attributes(
    assert_eq_fields_cover_instance,
):
    """_EQ_FIELDS | _EQ_EXCLUDE must exactly match instance __dict__."""
    dp = IndicatorDataPoint(
        dt="2025-01-01 00:00:00",
        value=1.0,
        ind_id="test_id",
    )
    assert_eq_fields_cover_instance(dp)


def test_IndicatorDataPoint_eq_field_sensitivity(
    run_eq_field_sensitivity,
):
    """Confirm _EQ_FIELDS drives inequality and _EQ_EXCLUDE does not."""
    obj = IndicatorDataPoint(
        dt="2025-01-01 00:00:00",
        value=1.0,
        ind_id="test_id",
    )
    run_eq_field_sensitivity(obj)


def test_Indicator_eq_covers_all_attributes(assert_eq_fields_cover_instance):
    """_EQ_FIELDS | _EQ_EXCLUDE must exactly match instance __dict__."""
    sym = Symbol(
        ticker="ES", name="ES", leverage_ratio=50.0, tick_size=0.25,
    )
    ind = Indicator(
        name="DELETEME",
        description="Coverage test",
        timeframe="1m",
        trading_hours="eth",
        symbol=sym,
        calc_version="1.0.0",
        calc_details="test",
        start_dt="2099-01-02 12:00:00",
        end_dt="2099-01-02 12:10:00",
        autoload_chart=False,
        candle_chart=None,
    )
    assert_eq_fields_cover_instance(ind)


def test_Indicator_eq_field_sensitivity(run_eq_field_sensitivity):
    """Confirm _EQ_FIELDS drives inequality and _EQ_EXCLUDE does not.

    'parameters' is in _EQ_EXCLUDE but compared via sub_eq(); it is
    passed as sub_eq_fields so it is also verified.
    """
    sym = Symbol(
        ticker="ES", name="ES", leverage_ratio=50.0, tick_size=0.25,
    )
    obj = Indicator(
        name="DELETEME",
        description="Coverage test",
        timeframe="1m",
        trading_hours="eth",
        symbol=sym,
        calc_version="1.0.0",
        calc_details="test",
        start_dt="2099-01-02 12:00:00",
        end_dt="2099-01-02 12:10:00",
        autoload_chart=False,
        candle_chart=None,
    )
    run_eq_field_sensitivity(obj, sub_eq_fields={"parameters"})
