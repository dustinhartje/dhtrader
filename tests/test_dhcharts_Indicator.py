import sys
import pytest
import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhstore as dhs


# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO what else can I do that covers subclasses without writing the tests
#      specifically for them?
# TODO Tests needed (some of these have already been written partially/fully
# IndicatorDataPoint __init__ stuff
# IndicatorDataPoint create & outputs (to_json, to_clean_dict, pretty,
#                    __str__, __repr__
# IndicatorDataPoint __eq and __ne pass and fail scenarios
# IndicatorDataPoint store, retreive, delete
# Indicator __init__ stuff
# Indicator create & outputs (to_json, to_clean_dict, pretty, __str__, __repr__
# Indicator __eq and __ne pass and fail scenarios
# Indicator store, retreive, delete
# Indicator get_info() - do I even really need this? test or deprecate in
#           favor of pretty()? (check dhtrader and backtesting repos for usage)
# Indicator load_underlying_charts() success and fail scenarios
# Indicator load_datapoints() success and fail scenarios
# Indicator get_datapoint() success and fail
# Indicator next/prev_datapoint() check for correct and check boundaries
#           should fail at edges, not wrap
# IndicatorEMA cover any additional attributes, methods, and parameters
# IndicatorEMA Also calculate(), sub_*(), what else?
#              --calculate should pull out-of-range candles to establish first
#              datapoints, be sure to test their values specifically
# TODO IndicatorEMA spotchecks "calculated" versions should build the indicator
#      itself from scratch also rather than get it from storage before calcs.
# TODO Write a function that tests several points around multiple holidays
#      Use the same times for each timeframe/trading_hours around
#      thanksgiving, christmas, and new years.  This way I get better
#      coverage without having to expand every test range as I can pass
#      in the indicator and then recalc or pull from storage just for the
#      timeframe needed for the test
# TODO rename IndicatorEMA specific functions to include EMA
#      (not just Indicator)
# TODO look for further ways to functionalize the IndicatorEMA tests so I will
#      be able to add more tests as time goes on / expand back in time
#      to improve coverage and better chance of catching edge cases.  One idea
#      around this is to write a function that builds, calcs/retrieves, and
#      tests for every assertion, so it would just take a bunch of parameters
#      including timeframe, trading_hours, length, and a target dt and value.
#      Then it would build a relatively small IndicatorEMA, test that value
#      and return True or False.  Could include calc vs storage as a param
#      or have two functions (probably do as param).  Try to keep the args
#      minimal enough to have assertions still be one line.  Assertion could
#      live inside the function or it could be on the oneline i.e.
#      assert myfunc(timeframe, trading_hours, length, calced).  maybe just use
#      positional args to keep it short.
#      -- would need to test how fast this is before replacing all my current
#         functions.  If building/calcing/retrieving slows it way down it
#         may not be worth it or maybe it can take a list of dicts of dates &
#         expected values to be tested and only build the indicator once for
#         each list then building a longer indicator is less problematic
# IndicatorSMA basically duplicate what I'm doing for Indicator
#              -- ideally do this in a way that loops so I can do the
#                 same for EMA and any future Indicators easily and
#                 consistently for all shared attributes & methods
# IndicatorSMA cover any additional attributes, methods, and parameters
# IndicatorSMA Also calculate(), sub_*(), what else?
#              --calculate should pull out-of-range candles to establish first
#              datapoints, be sure to test their values specifically

# TODO revamp this to have a create_* function similar to dhtrades patterns
# Create a base class Indicator and calculate
def hide_Indicator_demo_hod_creation_and_calculation():
    # Confirm RTH datapoints are calculated
    ind = dhc.Indicator(name="DELETEME-hod-demo",
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

    # TODO test ETH similarly
    # TODO test other timeframes mebe?

# ############################## IndicatorEMA() ###############################
# TODO write anything specific for EMA calculations or other varying attribs
#      & methods

# ##################### IndicatorEMA() Value Spot Checks ######################
# NOTE In TV, set chart to the timeframe of the EMA and hover over the bar that
#      contains the dateime in question.  This is the value it should be
#      returning.  Setting to lower timeframes shows the higher timeframes in
#      an unclear way that screwed up my testing initially.
# TODO consider spreading these out throughout the year or at least however
#      far back TV lets me go, possibly adding more as time goes on?  I could
#      add a reminder to add a new random check or 3 each day for each
#      indicator if I want to be super sure going forward
# TODO once all have been written and working, copy (or can I loop it?) to
#      have each of these functions also pull the raw candles and do the full
#      calculation of the indicator and verify the same values are found as
#      were stored.  This will help to catch future bugs affecting calculations
#      which may not get run against historical data and might be missed.
# #################################### ETH ####################################
# TODO error cause ideas:
#      * datapoints loading out of order?  possibly the index is not right
#      * candles not accurate
#      * wrong datapoints being returned, would explain why some wrong
#      * answers are exactly the same on the same day.  could be calcs
#        are right and it's the fetch that's hitting wrong.  Need to
#        review candles vs datapoints and check times on everything


# TODO 5m ETH 9
def Indicator_spotcheck_ES_eth_5m_EMA_close_l9_s2():
    pass


# def test_Indicator_calculated_spotcheck():
#     ind_calced = dhs.get_indicator(ind_id="",
#                                    autoload_datapoints=False,
#                                    )
#     ind_calced.start_dt = ""
#     ind_calced.end_dt = ""
#     ind_calced.load_underlying_chart()
#     ind_calced.calculate()
#     Indicator_spotcheck_(ind_calced)
#
#
# def test_Indicator_storage_spotcheck_():
#     ind_stored = dhs.get_indicator(ind_id="",
#                                    autoload_datapoints=False,
#                                    )
#     ind_stored.load_datapoints()
#     Indicator_spotcheck_(ind_stored)


# TODO 5m ETH 20
def Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2(i):
    return True


def hide_Indicator_calculated_spotcheck_ES_eth_5m_EMA_close_l20_s2():
    ind_calced = dhs.get_indicator(ind_id="ES_eth_5m_EMA_close_l20_s2",
                                   autoload_datapoints=False,
                                   )
    ind_calced.start_dt = "2025-01-01 00:00:00"
    ind_calced.end_dt = "2025-01-02 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2(ind_calced)


def hide_Indicator_storage_spotcheck_ES_eth_5m_EMA_close_l20_s2():
    ind_stored = dhs.get_indicator(ind_id="ES_eth_5m_EMA_close_l20_s2",
                                   autoload_datapoints=False,
                                   )
    ind_stored.load_datapoints()
    Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2(ind_stored)


# 15m ETH 9
def Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2(i):
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
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


@pytest.mark.storage
def test_Indicator_calculated_spotcheck_ES_eth_15m_EMA_close_l9_s2():
    ind_calced = dhs.get_indicator(ind_id="ES_eth_15m_EMA_close_l9_s2",
                                   autoload_datapoints=False,
                                   )
    ind_calced.start_dt = "2025-01-28 00:00:00"
    ind_calced.end_dt = "2025-02-16 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2(ind_calced)


@pytest.mark.storage
def test_Indicator_storage_spotcheck_ES_eth_15m_EMA_close_l9_s2():
    ind_stored = dhs.get_indicator(ind_id="ES_eth_15m_EMA_close_l9_s2",
                                   autoload_datapoints=False,
                                   )
    ind_stored.load_datapoints()
    Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2(ind_stored)


# 15m ETH 20
def Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2(i):
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
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


@pytest.mark.storage
def test_Indicator_calculated_spotcheck_ES_eth_15m_EMA_close_l20_s2():
    ind_calced = dhs.get_indicator(ind_id="ES_eth_15m_EMA_close_l20_s2",
                                   autoload_datapoints=False,
                                   )
    ind_calced.start_dt = "2024-11-27 00:00:00"
    ind_calced.end_dt = "2024-12-15 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2(ind_calced)


@pytest.mark.storage
def test_Indicator_storage_spotcheck_ES_eth_15m_EMA_close_l20_s2():
    ind_stored = dhs.get_indicator(ind_id="ES_eth_15m_EMA_close_l20_s2",
                                   autoload_datapoints=False,
                                   )
    ind_stored.load_datapoints()
    Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2(ind_stored)


# e1h ETH 9
def Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2(i):
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
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


@pytest.mark.storage
def test_Indicator_calculated_spotcheck_ES_eth_e1h_EMA_close_l9_s2():
    ind_calced = dhs.get_indicator(ind_id="ES_eth_e1h_EMA_close_l9_s2",
                                   autoload_datapoints=False,
                                   )
    ind_calced.start_dt = "2024-10-28 00:00:00"
    ind_calced.end_dt = "2024-11-17 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2(ind_calced)


@pytest.mark.storage
def test_Indicator_storage_spotcheck_ES_eth_e1h_EMA_close_l9_s2():
    ind_stored = dhs.get_indicator(ind_id="ES_eth_e1h_EMA_close_l9_s2",
                                   autoload_datapoints=False,
                                   )
    ind_stored.load_datapoints()
    Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2(ind_stored)


# e1h ETH 20
def Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2(i):
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
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


@pytest.mark.storage
def test_Indicator_calculated_spotcheck_ES_eth_e1h_EMA_close_l20_s2():
    ind_calced = dhs.get_indicator(ind_id="ES_eth_e1h_EMA_close_l20_s2",
                                   autoload_datapoints=False,
                                   )
    ind_calced.start_dt = "2024-10-01 00:00:00"
    ind_calced.end_dt = "2024-10-20 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2(ind_calced)


@pytest.mark.storage
def test_Indicator_storage_spotcheck_ES_eth_e1h_EMA_close_l20_s2():
    ind_stored = dhs.get_indicator(ind_id="ES_eth_e1h_EMA_close_l20_s2",
                                   autoload_datapoints=False,
                                   )
    ind_stored.load_datapoints()
    Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2(ind_stored)

# #################################### RTH ####################################
# TODO 5m RTH 9


def hide_Indicator_spotcheck_ES_rth_5m_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_5m_EMA_close_l9_s2",
                            autoload_datapoints=True,
                            )
    print(ind)
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando closed before,
    #           rando closed after
    # First and last candle of a different week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO 5m RTH 20
def hide_Indicator_spotcheck_ES_rth_5m_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_5m_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    print(ind)
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando closed before,
    #           rando closed after
    # First and last candle of a different week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday

# TODO 15m RTH 9


def Indicator_spotcheck_ES_rth_15m_EMA_close_l9_s2(i):
    # TODO values below should be correct based on TV, enable and run this
    #      once RTH calculations have been fixed and results wiped/restored
    #      in mongo
    # Sun-Sat - first & last candles, rando in the middle,
    #           rando closed before, rando closed after
    # Sun 7/14/24
    assert i.get_datapoint(dt="2024-07-14 05:21:00") is None
    assert i.get_datapoint(dt="2024-07-14 09:30:00") is None
    assert i.get_datapoint(dt="2024-07-14 13:47:00") is None
    assert i.get_datapoint(dt="2024-07-14 15:59:00") is None
    assert i.get_datapoint(dt="2024-07-14 18:12:00") is None
    # Mon 7/15/24
    assert i.get_datapoint(dt="2024-07-15 09:29:00") is None
    assert i.get_datapoint(dt="2024-07-15 09:30:00").value == 5685.59
    assert i.get_datapoint(dt="2024-07-15 10:50:00").value == 5695.13
    assert i.get_datapoint(dt="2024-07-15 15:59:00").value == 5687.14
    assert i.get_datapoint(dt="2024-07-15 22:01:00") is None
    # Tue 7/16/24
    assert i.get_datapoint(dt="2024-07-16 03:57:00") is None
    assert i.get_datapoint(dt="2024-07-16 09:30:00").value == 5689.51
    assert i.get_datapoint(dt="2024-07-16 12:41:00").value == 5697.65
    assert i.get_datapoint(dt="2024-07-16 15:59:00").value == 5710.84
    assert i.get_datapoint(dt="2024-07-16 16:10:00") is None
    # Wed 7/17/24
    assert i.get_datapoint(dt="2024-07-17 06:20:00") is None
    assert i.get_datapoint(dt="2024-07-17 09:30:00").value == 5701.69
    assert i.get_datapoint(dt="2024-07-17 13:36:00").value == 5644.69
    assert i.get_datapoint(dt="2024-07-17 15:59:00").value == 5642.65
    assert i.get_datapoint(dt="2024-07-17 21:58:00") is None
    # Thu 7/18/24
    assert i.get_datapoint(dt="2024-07-18 00:01:00") is None
    assert i.get_datapoint(dt="2024-07-18 09:30:00").value == 5642.80
    assert i.get_datapoint(dt="2024-07-18 13:31:00").value == 5609.72
    assert i.get_datapoint(dt="2024-07-18 15:59:00").value == 5591.28
    assert i.get_datapoint(dt="2024-07-18 20:30:00") is None
    # Fri 7/19/24
    assert i.get_datapoint(dt="2024-07-19 05:11:00") is None
    assert i.get_datapoint(dt="2024-07-19 09:30:00").value == 5592.17
    assert i.get_datapoint(dt="2024-07-19 14:15:00").value == 5555.82
    assert i.get_datapoint(dt="2024-07-19 15:59:00").value == 5555.12
    assert i.get_datapoint(dt="2024-07-19 17:52:00") is None
    # Sat 7/20/24
    assert i.get_datapoint(dt="2024-07-20 08:30:00") is None
    assert i.get_datapoint(dt="2024-07-20 09:30:00") is None
    assert i.get_datapoint(dt="2024-07-20 13:05:00") is None
    assert i.get_datapoint(dt="2024-07-20 23:59:00") is None
    assert i.get_datapoint(dt="2024-07-20 23:00:00") is None
#    # TODO why does TV show a 16:00 candle?  check 1m and other
#    #      timeframes, is this skewing their results?!?
#
#    # First and last candle of a different week
#    # TODO review this, my system got 5774.88 but TV says 5774.90
#    #      Is there a reason I should care about why this one is 0.02 off
#    #      while most others are exactly right on?
#    assert ind.get_datapoint(dt="2024-07-08 09:30:00").value == 5621.76
#    assert ind.get_datapoint(dt="2024-07-12 15:59:00").value == 5684.92
#    # TODO first and last candle of a month
#    # TODO Last candle before & after a holiday


def hide_Indicator_calculated_spotcheck_ES_rth_15m_EMA_close_l9_s2():
    ind_calced = dhs.get_indicator(ind_id="ES_rth_15m_EMA_close_l9_s2",
                                   autoload_datapoints=False,
                                   )
    ind_calced.start_dt = "2024-07-08 00:00:00"
    ind_calced.end_dt = "2024-07-21 00:00:00"
    ind_calced.load_underlying_chart()
    ind_calced.calculate()
    Indicator_spotcheck_ES_rth_15m_EMA_close_l9_s2(ind_calced)


def hide_Indicator_storage_spotcheck_ES_rth_15m_EMA_close_l9_s2():
    ind_stored = dhs.get_indicator(ind_id="ES_rth_15m_EMA_close_l9_s2",
                                   autoload_datapoints=False,
                                   )
    ind_stored.load_datapoints()
    Indicator_spotcheck_ES_rth_15m_EMA_close_l9_s2(ind_stored)

# TODO 15m RTH 20


def hide_Indicator_spotcheck_ES_rth_15m_EMA_close_l20_s2():
    pass


# TODO r1h RTH 9
def Indicator_spotcheck_ES_rth_r1h_EMA_close_l9_s2(i):
    pass


# TODO r1h RTH 20
def Indicator_spotcheck_ES_rth_r1h_EMA_close_l20_s2(i):
    pass


def hide_Indicator_create_and_calculate():
    # TODO break this up into however many functions make sense once converted
    # Building 5m9sma for 2025-01-08 9:30am-11:30am
    itest = dhc.IndicatorSMA(name="TestSMA-DELETEME",
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
    itest = dhc.IndicatorEMA(name="TestEMA-DELETEME",
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
    expected = [dhc.IndicatorDataPoint(dt='2025-01-10 15:00:00',
                                       value=5890.25,
                                       ind_id=expected_id,
                                       epoch=1736539200),
                dhc.IndicatorDataPoint(dt='2025-01-10 16:00:00',
                                       value=5884.5,
                                       ind_id=expected_id,
                                       epoch=1736542800),
                dhc.IndicatorDataPoint(dt='2025-01-12 18:00:00',
                                       value=5879.6,
                                       ind_id=expected_id,
                                       epoch=1736722800),
                dhc.IndicatorDataPoint(dt='2025-01-12 19:00:00',
                                       value=5875.38,
                                       ind_id=expected_id,
                                       epoch=1736726400),
                dhc.IndicatorDataPoint(dt='2025-01-12 20:00:00',
                                       value=5869.3,
                                       ind_id=expected_id,
                                       epoch=1736730000),
                ]
    calculated = itest.datapoints[-5:]
    for i in range(5):
        assert expected[i] == calculated[i]


def hide_Indicator_get_datapoints():
    itest = dhc.IndicatorEMA(name="TestEMA-DELETEME",
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


def hide_Indicator_storage_and_retrieval():
    itest = dhc.IndicatorEMA(name="TestEMA-DELETEME",
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
    # Confirm indicator is not already stored by first deleting it in case a
    # prior test run failed early
    dhs.delete_indicator(itest.ind_id)
    # Then attempting retrieval
    retrieve = dhs.get_indicator(ind_id=itest.ind_id)
    assert retrieve is None

    # Store it
    result = itest.store()

    # Confirm storage returned something that looks vaguely like an Indicator
    r_id = result["indicator"]["ind_id"]
    assert r_id == 'ES_eth_e1h_TestEMA-DELETEME_close_l9_s2'

    # Confirm we can retrieve it from storage now
    # Dates must be set as they don't get stored
    retrieve = dhs.get_indicator(ind_id=itest.ind_id,
                                 autoload_datapoints=True)
    retrieve.start_dt = "2025-01-08 00:00:00"
    retrieve.end_dt = "2025-01-12 20:00:00"
    retrieve.load_underlying_chart()
    assert retrieve == itest
    assert isinstance(retrieve, dhc.IndicatorEMA)
    assert retrieve.ind_id == itest.ind_id

    # Test datapoint retrieval
    datapoints = dhs.get_indicator_datapoints(
            ind_id=itest.ind_id)
    assert len(datapoints) == 23
    assert datapoints[5].ind_id == itest.ind_id

    # Updating TestEMA-DELETEME to add another day then storing again
    # This confirms only new datapoints get stored, existing should get
    # skipped which dramatically improves perodic update performance.
    # We should see 23 datapoints skipped and 46 stored on this operation
    itest.end_dt = "2025-01-14 20:00:00"
    itest.load_underlying_chart()
    itest.calculate()
    result = itest.store()
    assert result["datapoints_stored"] == 46
    assert result["datapoints_skipped"] == 23

    # Removing test object from storage
    dhs.delete_indicator(itest.ind_id)
    # And confirm it's gone
    retrieve = dhs.get_indicator(ind_id=itest.ind_id)
    assert retrieve is None
    dps = dhs.get_indicator_datapoints(ind_id=itest.ind_id)
    assert len(dps) == 0
