import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhstore as dhs

# ################################ Indicator() #################################

# TODO Test various methods (need more TODOs here)
# TODO Test storage and retrieval
# TODO what else?

# ############################## IndicatorEMA() ###############################
# TODO write anything specific for EMA calculations or other varying attribs
#      & methods

# ##################### IndicatorEMA() Value Spot Checks ######################

# #################################### ETH ####################################
# TODO ideas:
#      * datapoints loading out of order?  possibly the index is not right
#      * candles not accurate
#      * wrong datapoints being returned, would explain why some wrong
#      * answers are exactly the same on the same day.  could be calcs
#        are right and it's the fetch that's hitting wrong.  Need to
#        review candles vs datapoints and check times on everything

# TODO 5m ETH 9
def hide_Indicator_spotcheck_ES_eth_5m_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_eth_5m_EMA_close_l9_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO 5m ETH 20
def hide_Indicator_spotcheck_ES_eth_5m_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_eth_5m_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday

# TODO 15m ETH 9
def hide_Indicator_spotcheck_ES_eth_15m_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_eth_15m_EMA_close_l9_s2",
                            autoload_datapoints=False,
                            )
    ind.start_dt = "2025-02-09 16:00:00"
    ind.end_dt = "2025-02-15 18:00:00"
    ind.load_datapoints()
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 2/9/25
    assert ind.get_datapoint(dt="2025-02-09 18:00:00").value == 6048.67
    assert ind.get_datapoint(dt="2025-02-09 20:34:00").value == 6062.08
    assert ind.get_datapoint(dt="2025-02-09 23:59:00").value == 6064.90
    assert ind.get_datapoint(dt="2025-02-09 15:45:00") is None
    # Mon 2/10/25
    assert ind.get_datapoint(dt="2025-02-10 18:00:00").value == 6086.29
    assert ind.get_datapoint(dt="2025-02-10 10:34:00").value == 6082.93
    assert ind.get_datapoint(dt="2025-02-10 16:59:00").value == 6088.06
    assert ind.get_datapoint(dt="2025-02-10 17:12:00") is None
    # Tue 2/11/25
    assert ind.get_datapoint(dt="2025-02-11 18:00:00").value == 6090.27
    assert ind.get_datapoint(dt="2025-02-11 12:15:00").value == 6080.61
    assert ind.get_datapoint(dt="2025-02-11 16:59:00").value == 6090.58
    assert ind.get_datapoint(dt="2025-02-11 17:24:00") is None
    # Wed 2/12/25
    assert ind.get_datapoint(dt="2025-02-12 18:00:00").value == 6075.35
    assert ind.get_datapoint(dt="2025-02-12 14:52:00").value == 6072.38
    assert ind.get_datapoint(dt="2025-02-12 16:59:00").value == 6074.81
    assert ind.get_datapoint(dt="2025-02-12 17:37:00") is None
    # Thu 2/13/25
    assert ind.get_datapoint(dt="2025-02-13 18:00:00").value == 6130.98
    assert ind.get_datapoint(dt="2025-02-13 09:20:00").value == 6078.08
    assert ind.get_datapoint(dt="2025-02-13 16:59:00").value == 6129.97
    assert ind.get_datapoint(dt="2025-02-13 17:45:00") is None
    # Fri 2/14/25
    assert ind.get_datapoint(dt="2025-02-14 18:00:00") is None
    assert ind.get_datapoint(dt="2025-02-14 16:34:00").value == 6133.21
    assert ind.get_datapoint(dt="2025-02-14 16:59:00").value == 6133.37
    assert ind.get_datapoint(dt="2025-02-14 21:47:00") is None
    # Sat 2/15/25
    assert ind.get_datapoint(dt="2025-02-15 18:00:00") is None
    assert ind.get_datapoint(dt="2025-02-15 20:34:00") is None 
    assert ind.get_datapoint(dt="2025-02-15 23:59:00") is None 
    assert ind.get_datapoint(dt="2025-02-15 15:45:00") is None
    # First and last candle of a week
    # TODO Both of these are coming up with None
    #assert ind.get_datapoint(dt="2025-02-02 18:00:00").value == 6048.14
    #assert ind.get_datapoint(dt="2025-02-07 16:59:00").value == 6051.03
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO 15m ETH 20
def hide_Indicator_spotcheck_ES_eth_15m_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_eth_15m_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    # Sun 12/8/24
    assert ind.get_datapoint(dt="2024-12-08 18:00:00").value == 6096.55
    assert ind.get_datapoint(dt="2024-12-08 23:05:00").value == 6096.91
    assert ind.get_datapoint(dt="2024-12-08 23:59:00").value == 6096.26
    assert ind.get_datapoint(dt="2024-12-08 11:47:00") is None
    # Mon 12/9/24
    assert ind.get_datapoint(dt="2024-12-09 18:00:00").value == 6068.66
    assert ind.get_datapoint(dt="2024-12-09 09:21:00").value == 6095.97
    assert ind.get_datapoint(dt="2024-12-09 16:59:00").value == 6069.44
    assert ind.get_datapoint(dt="2024-12-09 17:01:00") is None
    # Tue 12/10/24
    assert ind.get_datapoint(dt="2024-12-10 18:00:00").value == 6054.87
    assert ind.get_datapoint(dt="2024-12-10 10:52:00").value == 6068.51
    assert ind.get_datapoint(dt="2024-12-10 16:59:00").value == 6055.25
    assert ind.get_datapoint(dt="2024-12-10 17:59:00") is None
    # Wed 12/11/24
    assert ind.get_datapoint(dt="2024-12-11 18:00:00").value == 6091.01
    assert ind.get_datapoint(dt="2024-12-11 11:18:00").value == 6074.16
    assert ind.get_datapoint(dt="2024-12-11 16:59:00").value == 6091.32
    assert ind.get_datapoint(dt="2024-12-11 17:30:00") is None
    # Thu 12/12/24
    assert ind.get_datapoint(dt="2024-12-12 18:00:00").value == 6067.47
    assert ind.get_datapoint(dt="2024-12-12 14:36:00").value == 6076.89
    assert ind.get_datapoint(dt="2024-12-12 16:59:00").value == 6067.97
    assert ind.get_datapoint(dt="2024-12-12 17:21:00") is None
    # Fri 12/13/24
    assert ind.get_datapoint(dt="2024-12-13 18:00:00") is None
    assert ind.get_datapoint(dt="2024-12-13 11:11:00").value == 6069.36
    assert ind.get_datapoint(dt="2024-12-13 16:59:00").value == 6053.99
    assert ind.get_datapoint(dt="2024-12-13 19:00:00") is None
    # Sat 12/14/24
    assert ind.get_datapoint(dt="2024-12-14 18:00:00") is None
    assert ind.get_datapoint(dt="2024-12-14 12:30:00") is None 
    assert ind.get_datapoint(dt="2024-12-14 23:59:00") is None 
    assert ind.get_datapoint(dt="2024-12-14 04:12:00") is None
    # First and last candle of a week
    assert ind.get_datapoint(dt="2024-12-01 18:00:00").value == 6045.34
    assert ind.get_datapoint(dt="2024-12-06 16:59:00").value == 6096.39
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO e1h ETH 9
def hide_Indicator_spotcheck_ES_eth_e1h_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_eth_e1h_EMA_close_l9_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # TODO did I do this against 20EMA or 9EMA?
    # Sun 11/10/24
    # TODO off by several points, my indicator says 6025.22
    #assert ind.get_datapoint(dt="2024-11-10 18:00:00").value == 6022.15
    # And yet this one is fine?!?
    assert ind.get_datapoint(dt="2024-11-10 18:55:00").value == 6025.22
    # TODO my system says 6029.39
    #assert ind.get_datapoint(dt="2024-11-10 21:01:00").value == 6028.43
    assert ind.get_datapoint(dt="2024-11-10 14:25:00") is None
    # Mon 11/11/24
    # TODO my system says 6031.34
    #assert ind.get_datapoint(dt="2024-11-11 18:00:00").value == 6032.24
    # TODO my system says 6034.95
    #assert ind.get_datapoint(dt="2024-11-11 13:17:00").value == 6037.12
    assert ind.get_datapoint(dt="2024-11-11 16:59:00").value == 6032.24
    assert ind.get_datapoint(dt="2024-11-11 17:28:00") is None
    # Tue 11/12/24
    # TODO my system says 6013.94
    #assert ind.get_datapoint(dt="2024-11-12 18:00:00").value == 6015.42
    # TODO 6017.15
    #assert ind.get_datapoint(dt="2024-11-12 14:19:00").value == 6015.43
    assert ind.get_datapoint(dt="2024-11-12 16:59:00").value == 6015.42
    assert ind.get_datapoint(dt="2024-11-12 17:37:00") is None
    # Wed 11/13/24
    # TODO 6018.26
    #assert ind.get_datapoint(dt="2024-11-13 18:00:00").value == 6017.77
    # TODO 6018.9
    #assert ind.get_datapoint(dt="2024-11-13 13:08:00").value == 6015.62
    assert ind.get_datapoint(dt="2024-11-13 16:59:00").value == 6017.77
    assert ind.get_datapoint(dt="2024-11-13 17:52:00") is None
    # Thu 11/14/24
    # TODO 5986.26
    #assert ind.get_datapoint(dt="2024-11-14 18:00:00").value == 5991.02
    assert ind.get_datapoint(dt="2024-11-14 13:56:00").value == 6000.96
    assert ind.get_datapoint(dt="2024-11-14 16:59:00").value == 5991.02
    assert ind.get_datapoint(dt="2024-11-14 17:44:00") is None
    # Fri 11/15/24
    assert ind.get_datapoint(dt="2024-11-15 18:00:00") is None
    # TODO 5935.75
    #assert ind.get_datapoint(dt="2024-11-15 10:48:00").value == 5904.87
    assert ind.get_datapoint(dt="2024-11-15 16:59:00").value == 5904.87
    assert ind.get_datapoint(dt="2024-11-15 22:45:00") is None
    # Sat 11/16/24
    assert ind.get_datapoint(dt="2024-11-16 18:00:00") is None
    assert ind.get_datapoint(dt="2024-11-16 04:10:00") is None 
    assert ind.get_datapoint(dt="2024-11-16 23:59:00") is None 
    assert ind.get_datapoint(dt="2024-11-16 12:12:00") is None
    # First and last candle of a week
    # TODO 5762.53
    #assert ind.get_datapoint(dt="2024-11-03 18:00:00").value == 5765.73
    assert ind.get_datapoint(dt="2024-11-08 16:59:00").value == 6022.15
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO e1h ETH 20
def test_Indicator_spotcheck_ES_eth_e1h_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_eth_e1h_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # Sun 10/13/24
    # TODO 5844.16
    #assert ind.get_datapoint(dt="2024-10-13 18:00:00").value == 5842.65
    # TODO 5846.19
    #assert ind.get_datapoint(dt="2024-10-13 20:18:00").value == 5845.24
    # TODO 5848.36
    #assert ind.get_datapoint(dt="2024-10-13 23:38:00").value == 5847.61
    assert ind.get_datapoint(dt="2024-10-13 08:50:00") is None
    # Mon 10/14/24
    # TODO 5886.73
    #assert ind.get_datapoint(dt="2024-10-14 18:00:00").value == 5884.13
    # TODO 5850.69
    #assert ind.get_datapoint(dt="2024-10-14 02:08:00").value == 5849.82
    assert ind.get_datapoint(dt="2024-10-14 16:59:00").value == 5884.13
    assert ind.get_datapoint(dt="2024-10-14 17:44:00") is None
    # Tue 10/15/24
    # TODO 5885.24
    #assert ind.get_datapoint(dt="2024-10-15 18:00:00").value == 5887.58
    assert ind.get_datapoint(dt="2024-10-15 06:51:00").value == 5902.51
    assert ind.get_datapoint(dt="2024-10-15 16:59:00").value == 5887.58
    assert ind.get_datapoint(dt="2024-10-15 17:24:00") is None
    # Wed 10/16/24
    # TODO 5875.07
    #assert ind.get_datapoint(dt="2024-10-16 18:00:00").value == 5874.34
    # TODO 5873.35
    #assert ind.get_datapoint(dt="2024-10-16 15:22:00").value == 5872.02
    assert ind.get_datapoint(dt="2024-10-16 16:59:00").value == 5874.34
    assert ind.get_datapoint(dt="2024-10-16 17:11:00") is None
    # Thu 10/17/24
    # TODO 5895.16
    #assert ind.get_datapoint(dt="2024-10-17 18:00:00").value == 5895.49
    # TODO 5887.80
    #assert ind.get_datapoint(dt="2024-10-17 06:20:00").value == 5885.07
    assert ind.get_datapoint(dt="2024-10-17 16:59:00").value == 5895.49
    assert ind.get_datapoint(dt="2024-10-17 17:19:00") is None
    # Fri 10/18/24
    assert ind.get_datapoint(dt="2024-10-18 18:00:00") is None
    # TODO 5896.93
    #assert ind.get_datapoint(dt="2024-10-18 13:03:00").value == 5895.87
    # TODO 5899.72
    #assert ind.get_datapoint(dt="2024-10-18 16:59:00").value == 5898.90
    assert ind.get_datapoint(dt="2024-10-18 23:58:00") is None
    # Sat 10/19/24
    assert ind.get_datapoint(dt="2024-10-19 18:00:00") is None
    assert ind.get_datapoint(dt="2024-10-19 15:39:00") is None 
    assert ind.get_datapoint(dt="2024-10-19 23:59:00") is None 
    assert ind.get_datapoint(dt="2024-10-19 03:25:00") is None
    # First and last candle of a week
    # TODO 5774.88
    #assert ind.get_datapoint(dt="2024-10-06 18:00:00").value == 5771.76
    assert ind.get_datapoint(dt="2024-10-11 16:59:00").value == 5842.65
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday

# #################################### RTH ####################################
# TODO 5m RTH 9
def hide_Indicator_spotcheck_ES_rth_5m_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_5m_EMA_close_l9_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO 5m RTH 20
def hide_Indicator_spotcheck_ES_rth_5m_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_5m_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday

# TODO 15m RTH 9
def hide_Indicator_spotcheck_ES_rth_15m_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_15m_EMA_close_l9_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday

# TODO 15m RTH 20
def hide_Indicator_spotcheck_ES_rth_15m_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_15m_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO e1h RTH 9
def hide_Indicator_spotcheck_ES_rth_e1h_EMA_close_l9_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_e1h_EMA_close_l9_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday


# TODO e1h RTH 20
def hide_Indicator_spotcheck_ES_rth_e1h_EMA_close_l20_s2():
    ind = dhs.get_indicator(ind_id="ES_rth_e1h_EMA_close_l20_s2",
                            autoload_datapoints=True,
                            )
    # print("\n")
    # print(ind.pretty())
    # Sun-Sat - first & last candles, rando in the middle, rando in closed
    # First and last candle of a week
    # TODO first and last candle of a month
    # TODO Last candle before & after a holiday
