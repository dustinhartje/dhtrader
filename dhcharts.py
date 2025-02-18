import datetime as dt
from datetime import timedelta
import sys
import json
import dhutil as dhu
import dhstore as dhs
from statistics import fmean
from copy import deepcopy

# TODO update docstrings in this and all other module files to google style:
#      https://sphinxcontrib-napoleon.readthedocs.io/
#          en/latest/example_google.html

CANDLE_TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', '1d', '1w']
BEGINNING_OF_TIME = "2024-01-01 00:00:00"


def bot():
    """Return universal beginning of time for this and other modules.  This
    represents the earliest time candles should be imported for among other
    things, setting a limit on how far back these modules can look to keep
    performance and resource needs reasonable."""

    return BEGINNING_OF_TIME


class Symbol():
    """Represents basic mechanics of a tradeable symbol a.k.a. ticker.  This
    might be a specific stock or future.
    """
    def __init__(self,
                 ticker: str,
                 name: str,
                 leverage_ratio: float,
                 tick_size: float,
                 ):

        self.ticker = ticker
        self.name = name
        self.leverage_ratio = float(leverage_ratio)
        self.tick_size = float(tick_size)
        self.set_times()

    def __eq__(self, other):
        return (self.ticker == other.ticker
                and self.name == other.name
                and self.leverage_ratio == other.leverage_ratio
                and self.tick_size == other.tick_size
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        w = deepcopy(self.__dict__)
        w["eth_open_time"] = str(w["eth_open_time"])
        w["eth_close_time"] = str(w["eth_close_time"])
        w["rth_open_time"] = str(w["rth_open_time"])
        w["rth_close_time"] = str(w["rth_close_time"])
        w["eth_week_open"]["time"] = str(w["eth_week_open"]["time"])
        w["eth_week_close"]["time"] = str(w["eth_week_close"]["time"])
        w["rth_week_open"]["time"] = str(w["rth_week_open"]["time"])
        w["rth_week_close"]["time"] = str(w["rth_week_close"]["time"])
        return json.dumps(w)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def set_times(self):
        """Sets times for known Symbol objects."""
        # DELETEME mimics ES and is used for testing storage to avoid polluting
        # actual ES related data
        if self.ticker in ["ES", "DELETEME"]:
            self.eth_open_time = dt.datetime.strptime("18:00:00",
                                                      "%H:%M:%S").time()
            self.eth_close_time = dt.datetime.strptime("16:59:00",
                                                       "%H:%M:%S").time()
            self.rth_open_time = dt.datetime.strptime("09:30:00",
                                                      "%H:%M:%S").time()
            self.rth_close_time = dt.datetime.strptime("16:14:00",
                                                       "%H:%M:%S").time()
            self.eth_week_open = {"day_of_week": 6,
                                  "time": self.eth_open_time}
            self.eth_week_close = {"day_of_week": 4,
                                   "time": self.eth_close_time}
            self.rth_week_open = {"day_of_week": 0,
                                  "time": self.rth_open_time}
            self.rth_week_close = {"day_of_week": 4,
                                   "time": self.rth_close_time}

        else:
            raise ValueError(f"Ticker {self.ticker} times have not yet been "
                             "defined, unable to calculate set_times()."
                             )

    def get_next_tick_up(self, f: float):
        """Returns the next tick available at or above the provided value"""
        # Ensure there's no more than 2 decimal places
        if str(f)[::-1].find('.') > 2:
            raise ValueError("More than 2 decimal places is not supported as "
                             f"input for this method, got f={str(f)}"
                             )
        r = f
        counter = 0
        # Increment by 0.01 until we reach a tick_size multiple
        while r % self.tick_size != 0:
            counter += 1
            if counter > (self.tick_size * 100):
                raise Exception("Error attempting to round up, too many loops "
                                "occurred which probably means a float math "
                                f"issue occurred.  Stopping after {counter} "
                                f"increments of {f} by 0.01 while trying to "
                                f"find a multiple of {self.tick_size}.  Value "
                                "at exit was {r}."
                                )
            r += 0.01
            # Rounding is needed because float math is imperfect and sometimes
            # adds numerous decimal places in simple arithmetic
            r = round(r, 2)

        return r

    def get_next_tick_down(self, f: float):
        """Returns the next tick available at or below the provided value"""
        # Ensure there's no more than 2 decimal places
        if str(f)[::-1].find('.') > 2:
            raise ValueError("More than 2 decimal places is not supported as "
                             f"input for this method, got f={str(f)}"
                             )
        r = f
        counter = 0
        # Increment by 0.01 until we reach a tick_size multiple
        while r % self.tick_size != 0:
            counter += 1
            if counter > (self.tick_size * 100):
                raise Exception("Error attempting to round up, too many loops "
                                "occurred which probably means a float math "
                                f"issue occurred.  Stopping after {counter} "
                                f"increments of {f} by 0.01 while trying to "
                                f"find a multiple of {self.tick_size}.  Value "
                                "at exit was {r}."
                                )
            r -= 0.01
            # Rounding is needed because float math is imperfect and sometimes
            # adds numerous decimal places in simple arithmetic
            r = round(r, 2)

        return r

    def market_is_open(self,
                       trading_hours: str,
                       target_dt,
                       check_closed_events: bool = True,
                       events: list = None,
                       ):
        """Returns True if target_dt is within market hours and no Events with
        category 'Closed' overlap as pulled from central storage
        """
        # Set vars needed to evaluate
        d = dhu.dt_as_dt(target_dt)
        dow = d.weekday()
        time = d.time()
        # Hours vary per day and overnight/weekends get tricky so
        # build a cheatsheet (0 = Monday)
        if trading_hours == "eth":
            closed = {0: [{"close": dhu.dt_as_time("17:00:00"),
                           "open": dhu.dt_as_time("18:00:00")}],
                      1: [{"close": dhu.dt_as_time("17:00:00"),
                           "open": dhu.dt_as_time("18:00:00")}],
                      2: [{"close": dhu.dt_as_time("17:00:00"),
                           "open": dhu.dt_as_time("18:00:00")}],
                      3: [{"close": dhu.dt_as_time("17:00:00"),
                           "open": dhu.dt_as_time("18:00:00")}],
                      4: [{"close": dhu.dt_as_time("17:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      5: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      6: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("18:00:00")}]
                      }
        elif trading_hours == "rth":
            closed = {0: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("09:30:00")},
                          {"close": dhu.dt_as_time("16:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      1: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("09:30:00")},
                          {"close": dhu.dt_as_time("16:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      2: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("09:30:00")},
                          {"close": dhu.dt_as_time("16:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      3: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("09:30:00")},
                          {"close": dhu.dt_as_time("16:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      4: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("09:30:00")},
                          {"close": dhu.dt_as_time("16:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      5: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}],
                      6: [{"close": dhu.dt_as_time("00:00:00"),
                           "open": dhu.dt_as_time("23:59:59")}]
                      }
        else:
            raise ValueError(f"trading_hours: {trading_hours} not supported")

        # Test open_times ranges based on weekday, returning False if time
        # falls inside any of them
        for c in closed[dow]:
            if c["close"] <= time < c["open"]:
                return False

        # Test against closure events
        if check_closed_events:
            # If list of events was not passed, retrieve them from storage
            if events is None:
                events = dhs.get_events(symbol=self.ticker,
                                        categories=["Closed"],
                                        )
            for e in events:
                if e.contains_datetime(d):
                    # Datetime falls inside a closure event
                    return False
                pass

        # If we got this far it must be inside market hours, right?
        return True

    def get_market_boundary(self,
                            target_dt,
                            trading_hours: str,
                            boundary: str,
                            order: str,
                            adjust_for_events: bool = True,
                            events: list = None,
                            ):
        """Returns the previous or next market open or close datetime for
        either regular trading hours (rth) or extended/globex trading hours
        (eth).  Optionally factors in events passed in to skip ahead or back
        when an Event (such as a market closure) would impact the datetime
        returned.  This requires the caller to determine event criteria and
        build the list rather than making assumptions.

        Typically this method does not need to be called directly, instead
        reference it via the wrapper methods below that provide better context
        and auto-include most of the parameters needed for streamlined code.

        Note that the current iteration only skips ahead to the next standard
        open or close if an event overlaps rather than evaluating the
        boundaries of the event itself.  This should have minimal impact on
        backtest results over meaningful periods of time and it's presumed
        one would probably not want to trade these periods anyways.
        """
        # TODO LOWPRI As noted in docstring, rare valid trading timeframes may
        #             be skipped due to events beginning or ending inside of
        #             standars market hours.  If this becomes problematic
        #             revisit and try to account for them in this and the other
        #             related _eth/rth_open/close methods.  I don't expect
        #             I'll actually trade within them though.

        # Prep vars
        if events is None:
            events = []
        allowed_hours = ["eth", "rth"]
        if trading_hours not in allowed_hours:
            raise ValueError(f"trading_hours must be in {allowed_hours}, got "
                             f"{trading_hours}")
        allowed_boundary = ["open", "close"]
        if boundary not in allowed_boundary:
            raise ValueError(f"boundary must be in {allowed_boundary}, got "
                             f"{boundary}")
        allowed_order = ["previous", "next"]
        if order not in allowed_order:
            raise ValueError(f"order must be in {allowed_order}, got "
                             f"{order}")

        # Evaluate requested boundary
        if self.ticker == "ES":
            this_date = dhu.dt_as_dt(target_dt).date()
            this_time = dhu.dt_as_dt(target_dt).time()
            # Set Target Time based on standard hours
            if trading_hours == "eth" and boundary == "open":
                tt = self.eth_open_time
            if trading_hours == "eth" and boundary == "close":
                tt = self.eth_close_time
            if trading_hours == "rth" and boundary == "open":
                tt = self.rth_open_time
            if trading_hours == "rth" and boundary == "close":
                tt = self.rth_close_time
            # Set Target Date based on time of input target_dt vs standard hrs
            if order == "next":
                # Target date is today if time before target, else tomorrow
                if this_time < tt:
                    td = this_date
                else:
                    td = this_date + timedelta(days=1)
                # Bump ahead for weekends
                if trading_hours == "eth":
                    if boundary == "open":
                        # Friday(4) and Saturdays(5) bump to Sunday(6)
                        while td.weekday() in [4, 5]:
                            td = td + timedelta(days=1)
                    if boundary == "close":
                        # Saturdays(5) and Sunday(6) bump to Monday(0)
                        while td.weekday() in [5, 6]:
                            td = td + timedelta(days=1)
                # for RTH Saturdays (5) and Sundays (6) bump to Monday (0)
                if trading_hours == "rth":
                    while td.weekday() in [5, 6]:
                        td = td + timedelta(days=1)
            if order == "previous":
                # Target date is today if time after target, else yesterday
                if this_time > tt:
                    td = this_date
                else:
                    td = this_date - timedelta(days=1)
                # Bump back for weekends
                if trading_hours == "eth":
                    if boundary == "open":
                        # Fridays(4) and Saturdays(5) fall back to Thursday(3)
                        while td.weekday() in [4, 5]:
                            td = td - timedelta(days=1)
                    if boundary == "close":
                        # Saturdays(5) and Sundays(6) fall back to Friday(4)
                        while td.weekday() in [5, 6]:
                            td = td - timedelta(days=1)
                if trading_hours == "rth":
                    # Saturdays(5) and Sundays(6) fall back to Friday(4)
                    while td.weekday() in [5, 6]:
                        td = td - timedelta(days=1)

            # Combine target date and target time into a datetime candidate
            r = dt.datetime.combine(td, tt)

            # Adjust for any closure events this might fall into by recursing
            # based on the end of any including event
            if adjust_for_events:
                for e in events:
                    if e.contains_datetime(r):
                        if order == "next":
                            new_target = e.end_dt
                        if order == "previous":
                            new_target = e.start_dt
                        r = self.get_market_boundary(
                                target_dt=new_target,
                                trading_hours=trading_hours,
                                boundary=boundary,
                                order=order,
                                adjust_for_events=adjust_for_events,
                                events=events,
                                )
        else:
            raise ValueError(f"Ticker {self.ticker} times have not yet been "
                             "defined, unable to calculate the open or close "
                             "datetime requested."
                             )

        return r

    def get_next_eth_open(self,
                          target_dt,
                          adjust_for_events: bool = True,
                          events: list = None,
                          ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="eth",
                                        boundary="open",
                                        order="next",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_previous_eth_open(self,
                              target_dt,
                              adjust_for_events: bool = True,
                              events: list = None,
                              ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="eth",
                                        boundary="open",
                                        order="previous",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_next_eth_close(self,
                           target_dt,
                           adjust_for_events: bool = True,
                           events: list = None,
                           ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="eth",
                                        boundary="close",
                                        order="next",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_previous_eth_close(self,
                               target_dt,
                               adjust_for_events: bool = True,
                               events: list = None,
                               ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="eth",
                                        boundary="close",
                                        order="previous",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_next_rth_open(self,
                          target_dt,
                          adjust_for_events: bool = True,
                          events: list = None,
                          ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="rth",
                                        boundary="open",
                                        order="next",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_previous_rth_open(self,
                              target_dt,
                              adjust_for_events: bool = True,
                              events: list = None,
                              ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="rth",
                                        boundary="open",
                                        order="previous",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_next_rth_close(self,
                           target_dt,
                           adjust_for_events: bool = True,
                           events: list = None,
                           ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="rth",
                                        boundary="close",
                                        order="next",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_previous_rth_close(self,
                               target_dt,
                               adjust_for_events: bool = True,
                               events: list = None,
                               ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours="rth",
                                        boundary="close",
                                        order="previous",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )


class Candle():
    def __init__(self,
                 c_datetime,
                 c_timeframe: str,
                 c_open: float,
                 c_high: float,
                 c_low: float,
                 c_close: float,
                 c_volume: int,
                 c_symbol,
                 c_tags: list = None,
                 c_epoch: int = None,
                 c_date: str = None,
                 c_time: str = None
                 ):

        # Passable attributes
        self.c_datetime = dhu.dt_as_str(c_datetime)
        self.c_timeframe = c_timeframe
        dhu.valid_timeframe(self.c_timeframe)
        self.c_open = float(c_open)
        self.c_high = float(c_high)
        self.c_low = float(c_low)
        self.c_close = float(c_close)
        self.c_volume = int(c_volume)
        if isinstance(c_symbol, Symbol):
            self.c_symbol = c_symbol
        else:
            self.c_symbol = dhs.get_symbol_by_ticker(ticker=c_symbol)
        if c_tags is None:
            c_tags = []
        else:
            self.c_tags = c_tags
        if c_epoch is None:
            c_epoch = dhu.dt_to_epoch(self.c_datetime)
        self.c_epoch = c_epoch
        if c_date is None:
            c_date = dhu.dt_as_str(c_datetime).split()[0]
        self.c_date = c_date
        if c_time is None:
            c_time = dhu.dt_as_str(c_datetime).split()[1]
        self.c_time = c_time

        # Calculated attributes
        delta = dhu.timeframe_delta(self.c_timeframe)
        self.c_end_datetime = dhu.dt_as_str(
                dhu.dt_as_dt(self.c_datetime) + delta)
        self.c_size = abs(self.c_high - self.c_low)
        self.c_body_size = abs(self.c_open - self.c_close)
        self.c_upper_wick_size = self.c_high - max(self.c_open, self.c_close)
        self.c_lower_wick_size = min(self.c_open, self.c_close) - self.c_low
        if self.c_size == 0:
            self.c_body_perc: float = None
            self.c_upper_wick_perc: float = None
            self.c_lower_wick_perc: float = None
        else:
            self.c_body_perc: float = self.c_body_size/self.c_size
            self.c_upper_wick_perc: float = self.c_upper_wick_size/self.c_size
            self.c_lower_wick_perc: float = self.c_lower_wick_size/self.c_size

        if self.c_close > self.c_open:
            self.c_direction = 'bullish'
        elif self.c_close < self.c_open:
            self.c_direction = 'bearish'
        else:
            self.c_direction = 'unchanged'

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = deepcopy(self.__dict__)
        working["c_symbol"] = working["c_symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def __eq__(self, other):
        return (self.c_datetime == other.c_datetime
                and self.c_timeframe == other.c_timeframe
                and self.c_open == other.c_open
                and self.c_high == other.c_high
                and self.c_low == other.c_low
                and self.c_close == other.c_close
                and self.c_volume == other.c_volume
                and self.c_symbol == other.c_symbol
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def store(self):
        return dhs.store_candle(self)

    def contains_datetime(self, d):
        """Return True if the datetime provided occurs in this candle"""
        return (dhu.dt_as_dt(self.c_datetime) < dhu.dt_as_dt(d) and
                dhu.dt_as_dt(d) < dhu.dt_as_dt(self.c_end_datetime))

    def contains_price(self, p):
        """Return True if the price provided falls at or between the high and
        low of this candle"""
        return self.c_low <= p <= self.c_high


class Chart():
    def __init__(self,
                 c_timeframe: str,
                 c_trading_hours: str,
                 c_symbol,
                 c_start: str = None,
                 c_end: str = None,
                 c_candles: list = None,
                 autoload: bool = False,
                 ):

        if dhu.valid_timeframe(c_timeframe):
            self.c_timeframe = c_timeframe
        if dhu.valid_trading_hours(c_trading_hours):
            self.c_trading_hours = c_trading_hours
        if isinstance(c_symbol, str):
            self.c_symbol = dhs.get_symbol_by_ticker(ticker=c_symbol)
        else:
            self.c_symbol = c_symbol
        self.c_start = dhu.dt_as_str(c_start)
        self.c_end = dhu.dt_as_str(c_end)
        if c_candles is None:
            self.c_candles = []
        else:
            self.c_candles = c_candles
        self.autoload = autoload
        if self.autoload:
            self.load_candles()  # includes review_candles()
        else:
            self.review_candles()

    def __eq__(self, other):
        return (self.c_timeframe == other.c_timeframe
                and self.c_symbol == other.c_symbol
                and self.c_start == other.c_start
                and self.c_end == other.c_end
                and self.c_candles == other.c_candles
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_json(self,
                suppress_candles: bool = True,
                ):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = deepcopy(self.__dict__)
        if suppress_candles:
            num = len(self.c_candles)
            clean_cans = [f"{num} Candles suppressed for output sanity"]
        else:
            clean_cans = []
            for c in working["c_candles"]:
                clean_cans.append(c.to_clean_dict())
        working["c_candles"] = clean_cans
        working["c_symbol"] = working["c_symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self,
                      suppress_candles: bool = True,
                      ):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json(suppress_candles=suppress_candles))

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def pretty(self,
               suppress_candles: bool = True,
               ):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        """
        return json.dumps(self.to_clean_dict(
            suppress_candles=suppress_candles),
            indent=4,
            )

    def sort_candles(self):
        self.c_candles.sort(key=lambda c: c.c_datetime)

    def add_candle(self, new_candle, sort=False):
        if not isinstance(new_candle, Candle):
            raise TypeError(f"new_candle {type(new_candle)} must be a "
                            "<class dhcharts.Candle> object")
        if new_candle.c_timeframe != self.c_timeframe:
            raise ValueError(f"new_candle c_timeframe of "
                             f"{new_candle.c_timeframe} does not match "
                             f"chart c_timeframe of {self.c_timeframe}")
        self.c_candles.append(new_candle)
        if sort:
            self.sort_candles()
        # Update chart start or end if candle falls outside current range
        if self.c_start is not None:
            self.c_start = min(self.c_start, new_candle.c_datetime)
        else:
            self.c_start = new_candle.c_datetime
        if self.c_end is not None:
            self.c_end = max(self.c_end, new_candle.c_datetime)
        else:
            self.c_end = new_candle.c_datetime
        self.review_candles()

    def load_candles(self):
        """Load candles from central storage based on current attributes"""
        cans = dhs.get_candles(
               start_epoch=dhu.dt_to_epoch(self.c_start),
               end_epoch=dhu.dt_to_epoch(self.c_end),
               timeframe=self.c_timeframe,
               symbol=self.c_symbol.ticker,
               )
        self.c_candles = []
        events = dhs.get_events(symbol=self.c_symbol.ticker,
                                categories=["Closed"],
                                )
        for c in cans:
            if self.c_symbol.market_is_open(target_dt=c.c_datetime,
                                            trading_hours=self.c_trading_hours,
                                            events=events,
                                            ):
                self.c_candles.append(c)
        self.sort_candles()
        self.review_candles()

    def review_candles(self):
        if len(self.c_candles) > 0:
            self.candles_count = len(self.c_candles)
            self.earliest_candle = dhu.dt_as_str(self.c_candles[0].c_datetime)
            self.latest_candle = dhu.dt_as_str(self.c_candles[-1].c_datetime)
        else:
            self.candles_count = 0
            self.earliest_candle = None
            self.latest_candle = None

        return {"candles_count": self.candles_count,
                "earliest_candle": self.earliest_candle,
                "latest_candle": self.latest_candle,
                }


class Event():
    """Classifies periods of time that are notable and may need to be
    correlated or excluded from charts during analysis.  May include holiday
    closures, FOMC meetings, and any other substantial occurences, planned
    or unplanned, that have immediate impact on the data beyond the usual.
    category is context dependent and to be defined by the user.  These should
    be unique enough to easily group similar events together, and the
    combination of start_dt + category is used to delineate unique events in
    storage to prevent duplication."""
    def __init__(self,
                 start_dt,
                 end_dt,
                 symbol,
                 category: str,
                 tags: list = None,
                 notes: str = "",
                 ):
        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        if isinstance(symbol, Symbol):
            self.symbol = symbol
        else:
            self.symbol = dhs.get_symbol_by_ticker(ticker=symbol)
        self.category = category
        self.tags = tags
        if tags is None:
            tags = []
        self.notes = notes
        self.start_epoch = dhu.dt_to_epoch(self.start_dt)
        self.end_epoch = dhu.dt_to_epoch(self.end_dt)

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = deepcopy(self.__dict__)
        working["symbol"] = working["symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def store(self):
        return dhs.store_event(self)

    def contains_datetime(self,
                          dt,
                          ):
        """Determine if the given datetime (dt) falls within this Event's
        timeframe, returning True or False.
        """
        start = dhu.dt_as_dt(self.start_dt)
        end = dhu.dt_as_dt(self.end_dt)
        this = dhu.dt_as_dt(dt)
        if start <= this <= end:
            return True
        else:
            return False


class Day():
    # TODO - Review this class for possible deprecation.  I suspect I created
    #        it mostly for use on the assumption I'd be writing candle data
    #        to disk in a Day based folder structure which is no longer needed.
    #        It looks like the only place I've used it is in firstrate.py
    #        in the process_unpacked_data() function which appears to also
    #        probably be obsolete at a glance and possibly also deprecatable.
    #      --on the flip side, this would still be handy for identifying things
    #        like opening range characteristics.  Perhaps rather than having
    #        candle specific attributes it should just retrieve r1d and e1d
    #        candles from storage during init then calculate things like
    #        opening range and such that are day specific.  HOD, LOD, stuff
    #        like that.  Only things that aren't already in a candle or are
    #        essentially indicators.  Not sure what all that would mean yet
    #        so basically just simplify it down to unique things, possibly
    #        remove the charts or maybe those are helpful at least for rth?
    #        Then just add things to it if/when I find uses for it while
    #        building backtesters.  If I get several backtesters built and
    #        still have not found a use for this class or see a likely use for
    #        it in upcoming backtests then it will be safe to remove.
    #      --it could also be potentially useful for somehow capturing Brooks
    #        style encyclopedia patterns across the whole day?  How would I use
    #        something like that?
    def __init__(self,
                 d_symbol,
                 d_date,
                 d_charts: list = None,
                 d_open_eth: float = None,
                 d_open_rth: float = None,
                 d_high_eth: float = None,
                 d_high_rth: float = None,
                 d_low_eth: float = None,
                 d_low_rth: float = None,
                 d_close_eth: float = None,
                 d_close_rth: float = None,
                 d_volume_eth: int = None,
                 d_volume_rth: int = None,
                 d_tags: list = None,
                 d_pattern_rth=None,  # brooks style day pattern for future use
                 ):

        # Establish extended and regular hours boundaries
        eth_start_time = dt.datetime.strptime('2000-01-01 00:00:00',
                                              '%Y-%m-%d %H:%M:%S').time()
        rth_start_time = dt.datetime.strptime('2000-01-01 09:30:00',
                                              '%Y-%m-%d %H:%M:%S').time()
        eth_end_time = dt.datetime.strptime('2000-01-01 23:59:00',
                                            '%Y-%m-%d %H:%M:%S').time()
        rth_end_time = dt.datetime.strptime('2000-01-01 15:59:00',
                                            '%Y-%m-%d %H:%M:%S').time()

        # Setup class attributes
        self.d_symbol = d_symbol
        if not isinstance(self.d_symbol, Symbol):
            raise TypeError(f"d_symbol {type(d_symbol)} must be a"
                            "<class dhcharts.Symbol> object")
        self.d_date = d_date
        if not isinstance(self.d_date, dt.date):
            raise TypeError(f"d_date {type(d_date)} must be a"
                            "<class datetime.date> object")
        if d_charts is None:
            self.d_charts = []
        else:
            self.d_charts = d_charts
        for c in d_charts:
            if not isinstance(self.d_symbol, Symbol):
                raise TypeError(f"c {type(c)} must be a"
                                "<class dhcharts.Chart> object")
        self.d_open_eth = d_open_eth
        self.d_open_rth = d_open_rth
        self.d_high_eth = d_high_eth
        self.d_high_rth = d_high_rth
        self.d_low_eth = d_low_eth
        self.d_low_rth = d_low_rth
        self.d_close_eth = d_close_eth
        self.d_close_rth = d_close_rth
        self.d_volume_eth = d_volume_eth
        self.d_volume_rth = d_volume_rth
        if d_tags is None:
            self.d_tags = []
        else:
            self.d_tags = d_tags
        self.d_pattern_rth = d_pattern_rth

        # Combine self.d_date and time boundaries for date-specific boundaries
        self.eth_start = dt.datetime.combine(self.d_date, eth_start_time)
        self.rth_start = dt.datetime.combine(self.d_date, rth_start_time)
        self.eth_end = dt.datetime.combine(self.d_date, eth_end_time)
        self.rth_end = dt.datetime.combine(self.d_date, rth_end_time)

        # Run through the 1m chart to further develop attributes
        self.recalc_from_1m()

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        return json.dumps(self.__dict__)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def recalc_from_1m(self):
        # Ensure we have a 1m chart or fail out
        base_chart = self.get_chart('1m')
        if base_chart is None:
            sys.exit('No 1m chart found, cannot recalc Day object')
        else:
            for candle in base_chart.c_candles:
                # TODO need to factor in day of week here, no weekends!
                if (candle.c_datetime >= self.rth_start
                        and candle.c_datetime <= self.rth_end):
                    in_rth = True
                else:
                    in_rth = False
                # Daily open
                if candle.c_datetime == self.eth_start:
                    self.d_open_eth = candle.c_open
                if candle.c_datetime == self.rth_start:
                    self.d_open_rth = candle.c_open
                # Daily close
                if candle.c_datetime == self.eth_end:
                    self.d_close_eth = candle.c_close
                if candle.c_datetime == self.rth_end:
                    self.d_close_rth = candle.c_close
                # Daily high
                if self.d_high_eth is None:
                    self.d_high_eth = candle.c_high
                else:
                    self.d_high_eth = max(self.d_high_eth, candle.c_high)
                if in_rth:
                    if self.d_high_rth is None:
                        self.d_high_rth = candle.c_high
                    else:
                        self.d_high_rth = max(self.d_high_rth, candle.c_high)
                # Daily low
                if self.d_low_eth is None:
                    self.d_low_eth = candle.c_low
                else:
                    self.d_low_eth = min(self.d_low_eth, candle.c_low)
                if in_rth:
                    if self.d_low_rth is None:
                        self.d_low_rth = candle.c_low
                    else:
                        self.d_low_rth = min(self.d_low_rth, candle.c_low)
                # Daily volume
                if self.d_volume_eth is None:
                    self.d_volume_eth = candle.c_volume
                else:
                    self.d_volume_eth += candle.c_volume
                if in_rth:
                    if self.d_volume_rth is None:
                        self.d_volume_rth = candle.c_volume
                    else:
                        self.d_volume_rth += candle.c_volume

    def add_chart(self, new_chart):
        if not isinstance(new_chart, Chart):
            raise TypeError(f"new_chart {type(new_chart)} must be a "
                            "<class dhcharts.Chart> object")
        chart_exists = False
        for c in self.d_charts:
            # TODO need to test this stuff
            if c.c_timeframe == new_chart.c_timeframe:
                chart_exists = True
        if not chart_exists:
            self.d_charts.append(new_chart)
        else:
            print('Unable to add chart with timeframe '
                  f"{new_chart['c_timeframe']} because a chart already exists"
                  ' using this timeframe.  Use update_chart() to overwrite.')

    def update_chart(self, new_chart):
        # TODO I need to test this function thoroughly, does it actually remove
        #      and then update what I want it to?  should it output?
        if not isinstance(new_chart, Chart):
            raise TypeError(f"new_chart {type(new_chart)} must be a "
                            "<class dhcharts.Chart> object")
        to_remove = []
        # TODO need to test this stuff, I think it should be like c.c_timeframe
        #      rather than c['c_timeframe'] right?
        for c in self.d_charts:
            if c.c_timeframe == new_chart.c_timeframe:
                to_remove.append(self.d_charts.index(c))
        to_remove.sort(reverse=True)
        for i in to_remove:
            self.d_charts.pop(i)
        self.d_charts.append(new_chart)
        if new_chart.c_timeframe == '1m':
            self.recalc_from_1m()

    def get_chart(self, timeframe: str):
        for c in self.d_charts:
            if c.c_timeframe == timeframe:
                return c
        return None


class IndicatorDataPoint():
    def __init__(self,
                 dt: str,
                 value: float,
                 ind_id: str,
                 epoch: int = None,
                 ):
        self.dt = dhu.dt_as_str(dt)
        self.value = value
        self.ind_id = ind_id
        if epoch is None:
            self.epoch = dhu.dt_to_epoch(dt)
        else:
            self.epoch = epoch
    """Simple class to handle time series datapoints for indicators.  I might
    swap this out for an even more generic TSDataPoint or similar if I find
    more uses for time series beyond this."""

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        return json.dumps(self.__dict__)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def __eq__(self, other):
        # Testing isinstance fails due to namespace differences when running
        # this as a script and it's a rabbithole to fix.  Since other types
        # are very unlikely to have these same attributes it's reasonably safe
        # to just treat any non-Exception result with matching values as good.
        # For example it gets an Exception when comparing to an empty list
        # which is returned from storage when there is no matching datapoint.
        # TODO this can likely be restored if and when I move tests out of
        # __main__ in favor of unit tests, and stop running this as a script.
        try:
            return (self.dt == other.dt and
                    self.value == other.value and
                    self.ind_id == other.ind_id and
                    self.epoch == other.epoch)
        except Exception:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def store(self, checker=None):
        """Optionally provide an IndicatorDataPoint as 'checker' if you want
        a test for this to compare, before running the storage operation as
        many queries slow down large batches.  This is meant to be provided by
        a wrapper performing periodic updates that may be attempting to store
        duplicate already in central storage.  The wrapper should retrieve all
        datapoints that might match from storage first and provide them for
        comparison to help this method avoid extra work.  There's probably
        a more elegant way to do this, but this will work and is easy enough.
        """
        if checker == self:
            # Return an object similar to what dhs.store_indicator would
            # provide on a skip if no checker had been provided.
            return {"skipped": 1,
                    "stored": 0,
                    "elapsed": None,
                    }
        else:
            return dhs.store_indicator_datapoints(datapoints=[self])


class Indicator():
    def __init__(self,
                 name: str,
                 description: str,
                 timeframe: str,
                 trading_hours: str,
                 symbol,
                 calc_version: str,
                 calc_details: str,
                 start_dt=bot(),
                 end_dt=None,
                 ind_id=None,
                 candle_chart=None,
                 datapoints: list = None,
                 parameters={},
                 ):
        """Base class for indicators.  Names should be short and simple
        abbreviations as they are used in tagging and storage, think like
        sma, hod, vwap, etc.  This class won't be used directly, use it's
        child classes which will be indicator type specific"""
        # TODO method to populate datapoints from storage, which should then
        #      possibly also calculate anything missing based on underlying
        #      candles' timestamps.  Or maybe just trigger a full recalc
        #      if anything seems amiss?
        #      TODO note earliest and latest timestamps then use expected
        #      candle functionality to check for missing datapoints.  Will
        #      need to account somehow for things like the first 8 bars of a
        #      9 bar ema/sma not being included, that's gonna get tricky
        #      if I don't want to write this to be indicator specific.  Maybe
        #      I can include leading and trailing gap parameters with default
        #      zero in the base class then adjust to be indicator specific
        #      in the subclasses, that should work!
        # TODO need some functions to review and cleanup both indicator meta
        #      and datapoints stuff
        # TODO create a calculations versions changelog for indicators as
        #      a separate text/md file with a section for each indicator
        # TODO Review get_info(), is it still needed after creation of
        #      .pretty()?
        #      If so, add earliest and latest datapoints to get_info()
        #      see dhmongo review_candles() for example
        #      If ditching it maybe add them as attributes?  be sure to recalc
        #      them whenever I adjust datapoints if I do
        self.name = name
        self.description = description
        if not dhu.valid_timeframe(timeframe):
            raise ValueError(f"{timeframe} not valid for timeframe")
        self.timeframe = timeframe
        if not dhu.valid_trading_hours(trading_hours):
            raise ValueError(f"{trading_hours} not valid for trading_hours")
        self.trading_hours = trading_hours
        if isinstance(symbol, Symbol):
            self.symbol = symbol
        else:
            self.symbol = dhs.get_symbol_by_ticker(ticker=symbol)
        self.calc_version = calc_version
        self.calc_details = calc_details
        self.start_dt = start_dt
        self.end_dt = end_dt
        if self.end_dt is None:
            self.end_dt = dhu.dt_as_str(dt.datetime.now())
        self.candle_chart = candle_chart
        if self.candle_chart is None:
            self.load_underlying_chart()
        if datapoints is None:
            self.datapoints = []
        else:
            self.datapoints = datapoints
        self.parameters = parameters
        if ind_id is None:
            self.ind_id = (f"{self.symbol.ticker}_{self.trading_hours}_"
                           f"{self.timeframe}_{self.name}")
        else:
            self.ind_id = ind_id
        self.class_name = "Indicator"

    def to_json(self,
                suppress_datapoints: bool = True,
                suppress_chart_candles: bool = True,
                ):
        """returns a json version of this object while normalizing
        custom types (like datetime to string).
        """
        working = deepcopy(self.__dict__)
        working["candle_chart"] = working["candle_chart"].to_clean_dict(
                suppress_candles=suppress_chart_candles,
                )
        if suppress_datapoints:
            num = len(self.datapoints)
            clean_dps = [f"{num} Datapoints suppressed for output sanity"]
        else:
            clean_dps = []
            for d in working["datapoints"]:
                clean_dps.append(d.to_clean_dict())
        working["datapoints"] = clean_dps
        working["symbol"] = working["symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self,
                      suppress_datapoints: bool = True,
                      suppress_chart_candles: bool = True,
                      ):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json(
                          suppress_datapoints=suppress_datapoints,
                          suppress_chart_candles=suppress_chart_candles,
                          ))

    def pretty(self,
               suppress_datapoints: bool = True,
               suppress_chart_candles: bool = True,
               ):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        Optionally suppress_datapoints to reduce output size when not needed.
        """
        working = self.to_clean_dict(
                suppress_datapoints=suppress_datapoints,
                suppress_chart_candles=suppress_chart_candles,
                )

        return json.dumps(working,
                          indent=4,
                          )

    def __str__(self):
        return str(self.get_info())

    def __repr__(self):
        return str(self)

    def get_info(self,
                 pretty: bool = False,
                 ):
        """Provide a basic overview of this object"""
        output = {"ind_id": self.ind_id,
                  "name": self.name,
                  "description": self.description,
                  "timeframe": self.timeframe,
                  "trading_hours": self.trading_hours,
                  "symbol": self.symbol.ticker,
                  "calc_version": self.calc_version,
                  "calc_details": self.calc_details,
                  "start_dt": self.start_dt,
                  "end_dt": self.end_dt,
                  "parameters": self.parameters,
                  "datapoints_count": len(self.datapoints)
                  }
        if pretty:
            return json.dumps(output,
                              indent=4,
                              )
        else:
            return output

    def load_underlying_chart(self):
        """Load the underlying candle chart from central storage"""
        self.candle_chart = Chart(c_timeframe=self.timeframe,
                                  c_trading_hours=self.trading_hours,
                                  c_symbol=self.symbol,
                                  c_start=self.start_dt,
                                  c_end=self.end_dt,
                                  autoload=True,
                                  )

    def load_datapoints(self):
        """Load any datapoints available from central storage by ind_id,
        start_dt, and end_dt.
        """
        self.datapoints = dhs.get_indicator_datapoints(
                ind_id=self.ind_id,
                earliest_dt=self.start_dt,
                latest_dt=self.end_dt,
                )

    def calculate(self):
        """This method will be specific to each type of indicator.  It should
        accpet only a list of Candles, sort it, and calculate new indicator
        datapoints from the candles.  Copy and modify this method as needed
        in subclasses."""
        if self.candle_chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhcharts.Chart> object")
        self.candle_chart.sort_candles()

        # Subclass specific functionality starts here

        # The code below is used for testing and demonstration only as this
        # class is not meant to be used directly.  You should create a
        # subclass and rewrite this method based on the specific needs of
        # your target indicator.

        # For demo purposes, let's calculate the high of the day
        self.datapoints = []
        hod = 0
        prev_day = dhu.dt_as_dt("1900-01-01 00:00:00").date()
        for c in self.candle_chart.c_candles:
            today = dhu.dt_as_dt(c.c_datetime).date()
            # Reset HOD for first candle of a new day
            if today > prev_day:
                prev_day = today
                hod = c.c_high
            else:
                hod = max(hod, c.c_high)
            self.datapoints.append(IndicatorDataPoint(dt=c.c_datetime,
                                                      value=hod,
                                                      ind_id=self.ind_id,
                                                      ))

        return True

    def store(self,
              store_datapoints: bool = True,
              fast_dps_check: bool = False,
              show_progress: bool = False,
              ):
        """uses DHStore functionality to store metadata and time series
        datapoints into central storage
        """
        return dhs.store_indicator(self,
                                   store_datapoints=store_datapoints,
                                   fast_dps_check=fast_dps_check,
                                   show_progress=show_progress,
                                   )

        return result

    def get_datapoint(self,
                      dt,
                      offset: int = 0,
                      ):
        """Returns a single datapoint based on datetime provided.  Because
        this is typically based on candle close, we often want the previous
        datapoint from the candle we are working through in a backtest so
        offset is allowed to go back or forward in the list by the provided
        value.  Wrapper methods assist with the most common previous and
        next requests.
        """
        can_dt = dhu.this_candle_start(dt=dt, timeframe=self.timeframe)
        index = next((i for i, dp in enumerate(self.datapoints)
                      if dhu.dt_as_dt(dp.dt) == dhu.dt_as_dt(can_dt)), None)
        # If no datapoints was found, return None
        if index is None:
            return None
        index += offset
        if index < 0:
            raise ValueError(f"index cannot be < 0, we got {index}.  "
                             "Something has gone terribly awry!",
                             )

        return self.datapoints[index]

    def next_datapoint(self,
                       dt,
                       ):
        """Wrapper for get_datapoint"""
        return self.get_datapoint(dt=dt, offset=1)

    def prev_datapoint(self,
                       dt,
                       ):
        """Wrapper for get_datapoint"""
        return self.get_datapoint(dt=dt, offset=-1)


class IndicatorSMA(Indicator):
    def __init__(self,
                 description,
                 timeframe,
                 trading_hours,
                 symbol,
                 calc_version,
                 calc_details,
                 start_dt=bot(),
                 end_dt=None,
                 ind_id=None,
                 candle_chart=None,
                 name="SMA",
                 datapoints=None,
                 parameters={},
                 ):
        super().__init__(name=name,
                         description=description,
                         timeframe=timeframe,
                         trading_hours=trading_hours,
                         symbol=symbol,
                         calc_version=calc_version,
                         calc_details=calc_details,
                         start_dt=start_dt,
                         end_dt=end_dt,
                         ind_id=ind_id,
                         candle_chart=candle_chart,
                         datapoints=datapoints,
                         parameters=parameters,
                         )
        """Subclass of Indicator() specifically used for simple moving avg"""
        # TODO Need to revisit before using this class.  I've made further
        #      refinements to IndicatorEMA that should be reflected here as
        #      well, particularly around name, description, and ind_id attribs.
        #      Also go through all the methods too though and catch up any
        #      other changes I've made since these should be pretty much the
        #      same other than the calculation formula.  In fact maybe they
        #      can be merged into a single IndicatorMA class with EMA/SMA as
        #      a parameter?

        # Confirm that parameters includes the subclass specific arguments
        # needed for this type of indicator
        # For simple SMA we just need a length and a method/value to use
        if "length" in parameters.keys():
            self.length = int(parameters["length"])
        else:
            raise ValueError("Must provide length in parameters")
        if "method" in parameters.keys():
            self.method = parameters["method"]
        else:
            self.method = "close"
        supported_methods = ["close"]
        if not parameters["method"] in supported_methods:
            raise TypeError(f"Method {parameters['method']} not supported, "
                            f"must be one of: f{supported_methods}"
                            )
        # Ensure ind_id has all optional parameters needed for this subclass
        # These may already be appended if this was retrieved from storage
        ind_id_suffix = f"_{self.method}_l{str(self.length)}"
        if ind_id_suffix not in self.ind_id:
            self.ind_id += ind_id_suffix
        self.class_name = "IndicatorSMA"

    def calculate(self):
        """Calculate a simple moving average over time.  Defaults to using
        the 'close' value of each candle."""
        if self.candle_chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhcharts.Chart> object")
        self.candle_chart.sort_candles()

        # Subclass specific functionality starts here
        self.datapoints = []
        counter = 0
        # Build a working list of the same length as our SMA
        values = []
        while counter < self.length:
            values.append(0)
            counter += 1
        counter = 0
        # Work through candles, updating/calcing from the working list
        for c in self.candle_chart.c_candles:
            # Drop the oldest value and add the current to the end of the list
            values = values[1:]
            if self.method == "close":
                values.append(c.c_close)
            else:
                raise ValueError(f"Unsupported method: {self.method}")
            # Once enough candles are in the working list, calc the datapoint
            if counter >= (self.length - 1):
                dp = IndicatorDataPoint(dt=dhu.dt_as_str(c.c_datetime),
                                        value=round(fmean(values), 2),
                                        ind_id=self.ind_id,
                                        )
                self.datapoints.append(dp)
            counter += 1

        return True


class IndicatorEMA(Indicator):
    def __init__(self,
                 description,
                 timeframe,
                 trading_hours,
                 symbol,
                 calc_version,
                 calc_details,
                 start_dt=bot(),
                 end_dt=None,
                 ind_id=None,
                 candle_chart=None,
                 name="EMA",
                 datapoints=None,
                 parameters={},
                 ):
        super().__init__(name=name,
                         description=description,
                         timeframe=timeframe,
                         trading_hours=trading_hours,
                         symbol=symbol,
                         calc_version=calc_version,
                         calc_details=calc_details,
                         start_dt=start_dt,
                         end_dt=end_dt,
                         ind_id=ind_id,
                         candle_chart=candle_chart,
                         datapoints=datapoints,
                         parameters=parameters,
                         )
        """Sublcass of Indicator() used for exponential moving averages.
        Requires a length, method (default: close) and smoothing (default: 2).
        The first 4*length calculated values will not be kept as the early
        part of the calculation has to start with a simple average and does
        not reach true EMA values until it has substantial history to
        effectively factor out the initial non-EMA baseline.  Spot testing
        found roughly 4 times the length is when accuracy reaches +/- $0.01 vs
        the same timestamp datapoints when the series is started at a much
        earlier point in time."""
        # Confirm that parameters includes the subclass specific arguments
        # needed for this type of indicator
        # For EMA we just need a length, method, and smoothing factor
        if "length" in parameters.keys():
            self.length = int(parameters["length"])
        else:
            raise ValueError("Must provide length in parameters")
        if "method" in parameters.keys():
            self.method = parameters["method"]
        else:
            self.method = "close"
        supported_methods = ["close"]
        if not parameters["method"] in supported_methods:
            raise TypeError(f"Method {parameters['method']} not supported, "
                            f"must be one of: f{supported_methods}"
                            )
        if "smoothing" in parameters.keys():
            self.smoothing = int(parameters["smoothing"])
        else:
            self.smoothing = 2
        # Ensure ind_id has all optional parameters needed for this subclass
        # These may already be appended if this was retrieved from storage
        ind_id_suffix = (f"_{self.method}_l{str(self.length)}"
                         f"_s{str(self.smoothing)}")
        if ind_id_suffix not in self.ind_id:
            self.ind_id += ind_id_suffix
        self.class_name = "IndicatorEMA"

    def calculate(self):
        """Calculate an exponential simple moving average over time.  Defaults
        to using the 'close' value of each candle and a smoothing factor of 2.
        """
        if self.candle_chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhcharts.Chart> object")
        self.candle_chart.sort_candles()
        # Subclass specific functionality starts here
        self.datapoints = []
        counter = 0
        # Build an initial SMA to use on the first EMA datapoint
        starting_values = []
        counter = 0
        min_cans = self.length * 4
        # Work through candles, updating/calcing from the working list
        for c in self.candle_chart.c_candles:
            # Until we reach length only build list of the initial candles
            # which will be averaged to fudge a "prior_ema" for the first calc
            if counter < self.length:
                if self.method == "close":
                    starting_values.append(c.c_close)
                counter += 1
            else:
                # Use the initial simple average for the first prior_ema
                # After this we'll always have a prior_ema from the prev calc
                if counter == self.length:
                    prior_ema = round(fmean(starting_values), 2)
                if self.method == "close":
                    v = c.c_close
                s = self.smoothing
                ln = self.length + 1
                p = prior_ema
                # Calculate
                # ref: https://www.investopedia.com/terms/e/ema.asp
                this_ema = (v * (s / ln)) + (p * (1 - (s / ln)))
                # Only keep/ store once we reach the minimum needed to ensure
                # accurate values.  See docstring for details.
                if counter >= min_cans:
                    dp = IndicatorDataPoint(dt=dhu.dt_as_str(c.c_datetime),
                                            value=round(this_ema, 2),
                                            ind_id=self.ind_id,
                                            )
                    self.datapoints.append(dp)
                # Update vars for next candle
                prior_ema = this_ema
                counter += 1

# TODO -----CURRENT PRIORITIES JAN 2025-----
# TODO add low hanging fruit like HOD/LOD/YRHI/YRLO/OOD/GXHI/GXLO/YRCL
# TODO add subclass of Inidicators class for each type of indicator I want
#      9 & 20 EMAs, HOD, LOD, OOD, GXLO, GXHI should all be easy to do now
#      when I get around to adding 1d timeframes also do 20/50/100/200dSMA
#      later maybe do VWAP, RSI, what else?
# TODO get a backtester and analyzer built for EMAs as a framework that can
#      be used for other indicators.  Should spit out raw "best" as well as
#      things like running-last-week, running-lastX as the first version.
#      Other more complex scenarios can be added later per-idea, including
#      stuff like what the opening range or daily chart looks like as triggers
#      --------------------------------------

# TODO add function to update some or all indicators based on incoming candles
#      this should accept a list of indicators to loop through by short_name,
#      with "all" being the default only item in the list.  For each indicator
#      if it's in the list or all is in the list go ahead and run it.  Each of
#      the indicators gets built as an object then it's store method is run
#      by default
#      --really the looper should go in either dhutility.py or refreshdata.py.
#      the method on each indicator should just be able to update itself
#      using the latest candles avail in storage.  It should have options
#      to just calculate new datapoints or wipe and recalc from beginning


def test_boundary(name, actual, expected):
    """Quick function to streamline repetitive output in testing below"""
    global boundaries_all_ok
    if actual == expected:
        print(f"OK: ({name})   {actual} (actual) == {expected} (expected)")
        return True
    else:
        print(f"ERROR: ({name})   {actual} (actual) != {expected} (expected)")
        boundaries_all_ok = False
        return False


if __name__ == '__main__':
    # TODO LOWPRI these should be unit tests or something similar eventually
    # Run a few tests to confirm desired functionality

    # Test pretty output which also confirms json and clean_dict working
    # TODO in lieu of real unit tests, start a test_results empty list and
    #      record a quick oneliner for each easily confirmable test as it
    #      finishes, something like "OK - Trade() Storage and retrieval"
    #      then print them all at the end.  For non-easily-confirmed could
    #      add a note like "UNKNOWN - Visual confirm needed for Trade.pretty()
    print("\n######################### OUTPUTS ###########################")
    print("All objects should print 'pretty' which confirms .to_json(), "
          ".to_clean_dict(), and .pretty() methods all work properly"
          )
    print("\n---------------------------- CANDLE ---------------------------")
    out_candle = Candle(c_datetime="2025-01-02 12:00:00",
                        c_timeframe="1m",
                        c_open=5000,
                        c_high=5007.75,
                        c_low=4995.5,
                        c_close=5002,
                        c_volume=1501,
                        c_symbol="ES",
                        )
    print(out_candle.pretty())
    print("\n----------------------------- CHART ----------------------------")
    out_chart = Chart(c_timeframe="1m",
                      c_trading_hours="rth",
                      c_symbol="ES",
                      c_start="2025-01-02 12:00:00",
                      c_end="2025-01-02 12:10:00",
                      autoload=False,
                      )
    out_chart.add_candle(out_candle)
    print("Without chart candles")
    print(out_chart.pretty())
    print("With candles")
    print(out_chart.pretty(suppress_candles=False))
    print("\n----------------------------- EVENT ----------------------------")
    out_event = Event(start_dt="2025-01-02 12:00:00",
                      end_dt="2025-01-02 18:00:00",
                      symbol="ES",
                      category="Closed",
                      tags=["holiday"],
                      notes="Test Holiday",
                      )
    print(out_event.pretty())
    print("\n------------------- INDICATOR DATAPOINT ------------------------")
    out_dp = IndicatorDataPoint(dt="2025-01-02 12:00:00",
                                value=100,
                                ind_id="ES1mTESTSMA-DELETEME9",
                                )
    print(out_dp.pretty())
    print("\n------------------------- INDICATOR ----------------------------")
    out_ind = IndicatorSMA(name="TestSMA-DELETEME",
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
    out_ind.datapoints = [out_dp]
    print("Without chart candles or datapoints")
    print(out_ind.pretty())
    print("With chart candles and datapoints")
    print(out_ind.pretty(suppress_datapoints=False,
                         suppress_chart_candles=False,
                         ))
    print("\nIndicator.get_info(pretty=True):")
    print(out_ind.get_info(pretty=True))

    print("\n######################### SYMBOLS ###########################")
    print("Creating an ES symbol:")

    sym = Symbol(ticker="ES",
                 name="ES",
                 leverage_ratio=50.0,
                 tick_size=0.25,
                 )
    print(sym.pretty())

    print("\n----------------------------------------------------------------")
    print("Testing market_is_open method\n")
    # Sunday 2024-03-17
    # Monday 2024-03-18
    # Wednesday 2024-03-20
    # Friday 2024-03-22
    # Saturday 2024-03-23

    # ETH
    h = "eth"
    print(f"\n{h}\n")
    # Saturday should fail at 4am, 5:30pm, and 9pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-23 04:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Saturday at 4am - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-23 17:30:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Saturday at 5:30pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-23 21:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Saturday at 9pm - {r}")
    # Friday should succeed at noon, fail after 5pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-22 12:00:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Friday at 12pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-22 17:30:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Friday at 5:30pm - {r}")
    # Sunday should fail at noon, succeed after 6pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-17 12:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Sunday at 12pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-17 19:30:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Sunday at 7:30pm - {r}")
    # Wednesday should succeed at noon and 8pm, fail at 5:30pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-20 12:00:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Wednesday at 12pm - {r}")
    # TODO why is this one failing? ######################
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-20 17:30:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Wednesday at 5:30pm - {r}")
    ######################################################
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-20 20:00:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Wednesday at 8pm - {r}")

    # RTH
    h = "rth"
    print(f"\n{h}\n")
    # Saturday should fail at 4am, 5:30pm, and 9pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-16 04:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Saturday at 4am - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-16 17:30:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Saturday at 5:30pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-16 21:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Saturday at 9pm - {r}")
    # Sunday should fail at 4am, 5:30pm, and 9pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-17 04:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Sunday at 4am - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-17 17:30:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Sunday at 5:30pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-17 21:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Sunday at 9pm - {r}")
    # Monday should fail at 8am, succeed after 2pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-18 08:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Monday at 8am - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-18 14:00:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Monday at 2pm - {r}")
    # Friday should succeed at noon, fail after 4pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-22 12:00:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Friday at 12pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-22 17:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Friday at 5pm - {r}")
    # Wednesday should fail at 4am, succeed at noon, and fail at 8pm
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-20 04:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Wednesday at 4am - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-20 12:00:00")
    s = "OK" if r is True else "ERROR"
    print(f"{s} {h}  Market open Wednesday at 12pm - {r}")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-20 20:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open Wednesday at 8pm - {r}")

    # Events should also indicate market closed
    print(f"\nTesting that datetime inside a holiday closure fails using "
          "2024-03-29 12:00:00 which is noon on Good Friday\n")
    h = "rth"
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-29 12:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open on Good Friday at 12pm - {r}")
    h = "eth"
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-29 12:00:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open on Good Friday at 12pm - {r}")
    print("\nand just to be sure it's not going to flip somehow test during "
          "daily eth closure window on the same holiday\n")
    r = sym.market_is_open(trading_hours=h, target_dt="2024-03-29 17:30:00")
    s = "OK" if r is False else "ERROR"
    print(f"{s} {h}  Market open on Good Friday at 5:30pm - {r}")

    print("\n----------------------------------------------------------------")
    print("-----------------------------------------------------------------")
    print("Confirming get_market_boundary wrappers working as expected")
    boundaries_all_ok = True
    print("All results will be displayed as Expected then Actual")
    print("\nTesting All boundaries mid-week Wednesday noon datetime: "
          "2024-03-20 12:00:00")
    print("This confirms non-weekend mechanics all work properly")
    t = dhu.dt_as_dt("2024-03-20 12:00:00")
    print(f"{str(t)}\n")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t))
    test_boundary("next_eth_open", actual, "2024-03-20 18:00:00")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t))
    test_boundary("next_eth_close", actual, "2024-03-20 16:59:00")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t))
    test_boundary("previous_eth_open", actual, "2024-03-19 18:00:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t))
    test_boundary("previous_eth_close", actual, "2024-03-19 16:59:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t))
    test_boundary("next_rth_open", actual, "2024-03-21 09:30:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t))
    test_boundary("next_rth_close", actual, "2024-03-20 15:59:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t))
    test_boundary("previous_rth_open", actual, "2024-03-20 09:30:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t))
    test_boundary("previous_rth_close", actual, "2024-03-19 15:59:00")
    print("-----------------------------------------------------------------")
    print("\nTesting Next boundaries from Thursday noon 2024-03-21 12:00:00 "
          "(should hit Thursday/Friday)")
    print("This confirms we don't accidentally slip into or over the weekend "
          "due to miscalculations")
    t = dhu.dt_as_dt("2024-03-21 12:00:00")
    print(f"{str(t)}\n")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t))
    test_boundary("next_eth_open", actual, "2024-03-21 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t))
    test_boundary("next_rth_open", actual, "2024-03-22 09:30:00")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t))
    test_boundary("next_eth_close", actual, "2024-03-21 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t))
    test_boundary("next_rth_close", actual, "2024-03-21 15:59:00")
    print("-----------------------------------------------------------------")
    print("\nTesting Next boundaries from Friday noon 2024-03-22 12:00:00 "
          "(should hit Sunday/Monday)")
    print("This confirms we span the weekend as expected when appropriate")
    t = dhu.dt_as_dt("2024-03-22 12:00:00")
    print(f"{str(t)}\n")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t))
    test_boundary("next_eth_open", actual, "2024-03-24 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t))
    test_boundary("next_rth_open", actual, "2024-03-25 09:30:00")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t))
    test_boundary("next_eth_close", actual, "2024-03-22 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t))
    test_boundary("next_rth_close", actual, "2024-03-22 15:59:00")
    print("-----------------------------------------------------------------")
    print("\nTesting Previous boundaries from Tuesday noon 2024-03-19 "
          "12:00:00 (should hit Monday/Tuesday)")
    print("This confirms we don't accidentally slip into or over the weekend "
          "due to miscalculations")
    t = dhu.dt_as_dt("2024-03-19 12:00:00")
    print(f"{str(t)}\n")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t))
    test_boundary("previous_eth_open", actual, "2024-03-18 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t))
    test_boundary("previous_rth_open", actual, "2024-03-19 09:30:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t))
    test_boundary("previous_eth_close", actual, "2024-03-18 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t))
    test_boundary("previous_rth_close", actual, "2024-03-18 15:59:00")
    print("-----------------------------------------------------------------")
    print("\nTesting Previous boundaries from Monday noon 2024-03-18 "
          "12:00:00 (should hit Friday/Sunday)")
    print("This confirms we span the weekend as expected when appropriate")
    t = dhu.dt_as_dt("2024-03-18 12:00:00")
    print(f"{str(t)}\n")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t))
    test_boundary("previous_eth_open", actual, "2024-03-17 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t))
    test_boundary("previous_rth_open", actual, "2024-03-18 09:30:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t))
    test_boundary("previous_eth_close", actual, "2024-03-15 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t))
    test_boundary("previous_rth_close", actual, "2024-03-15 15:59:00")
    print("-----------------------------------------------------------------")
    print("Setting up a few events to test that boundary mechanics respect")
    events = [Event(start_dt="2024-03-28 17:00:00",
                    end_dt="2024-03-31 17:59:00",
                    symbol="ES",
                    category="Closed",
                    notes="Good Friday Closed",
                    ),
              Event(start_dt="2024-03-18 00:00:00",
                    end_dt="2024-03-19 23:59:00",
                    symbol="ES",
                    category="Closed",
                    notes="Tues-Wed Full days closure",
                    ),
              Event(start_dt="2024-03-18 13:00:00",
                    end_dt="2024-03-18 17:59:00",
                    symbol="ES",
                    category="Closed",
                    notes="Tues early closure",
                    ),
              ]
    for ev in events:
        print(ev.pretty())
    print("\nTesting Next against Good Friday closure starting Thursday "
          "2024-03-28 17:00:00 through Sunday 2024-03-31 17:59:00")
    print("Checking from noon on Thursday 2024-03-28 12:00:00")
    print("This confirms we cross the event and weekend where appropriate.")
    t = dhu.dt_as_dt("2024-03-28 12:00:00")
    print(f"{str(t)}\n")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t, events=events))
    test_boundary("next_eth_open", actual, "2024-03-31 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t, events=events))
    test_boundary("next_rth_open", actual, "2024-04-01 09:30:00")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t, events=events))
    test_boundary("next_eth_close", actual, "2024-03-28 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t, events=events))
    test_boundary("next_rth_close", actual, "2024-03-28 15:59:00")

    print("\nTesting same closure window from within using Friday at Noon "
          "2024-03-29 12:00:00")
    print("This confirms times inside a closure are moved outside of it in "
          "both direction")
    t = dhu.dt_as_dt("2024-03-29 12:00:00")
    print(f"{str(t)}\n")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t, events=events))
    test_boundary("next_eth_close", actual, "2024-04-01 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t, events=events))
    test_boundary("next_rth_close", actual, "2024-04-01 15:59:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t, events=events))
    test_boundary("previous_eth_close", actual, "2024-03-28 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t, events=events))
    test_boundary("previous_rth_close", actual, "2024-03-28 15:59:00")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t, events=events))
    test_boundary("next_eth_open", actual, "2024-03-31 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t, events=events))
    test_boundary("next_rth_open", actual, "2024-04-01 09:30:00")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t, events=events))
    test_boundary("previous_eth_open", actual, "2024-03-27 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t, events=events))
    test_boundary("previous_rth_open", actual, "2024-03-28 09:30:00")

    print("\nTesting same closure window from the following Monday at Noon "
          "2024-04-01 12:00:00")
    print("This confirms Previous crosses the event to the prior week.")
    t = dhu.dt_as_dt("2024-04-01 12:00:00")
    print(f"{str(t)}\n")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t, events=events))
    test_boundary("previous_eth_open", actual, "2024-03-31 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t, events=events))
    test_boundary("previous_rth_open", actual, "2024-04-01 09:30:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t, events=events))
    test_boundary("previous_eth_close", actual, "2024-03-28 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t, events=events))
    test_boundary("previous_rth_close", actual, "2024-03-28 15:59:00")
    print("\nTesting nested closure window from within both using "
          "2024-03-18 14:00:00")
    print("This confirms that multiple overlapping events don't muck it up.")
    t = dhu.dt_as_dt("2024-03-18 14:00:00")
    print(f"{str(t)}\n")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t, events=events))
    test_boundary("next_eth_close", actual, "2024-03-20 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t, events=events))
    test_boundary("next_rth_close", actual, "2024-03-20 15:59:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t, events=events))
    test_boundary("previous_eth_close", actual, "2024-03-15 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t, events=events))
    test_boundary("previous_rth_close", actual, "2024-03-15 15:59:00")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t, events=events))
    test_boundary("next_eth_open", actual, "2024-03-20 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t, events=events))
    test_boundary("next_rth_open", actual, "2024-03-20 09:30:00")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t, events=events))
    test_boundary("previous_eth_open", actual, "2024-03-17 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t, events=events))
    test_boundary("previous_rth_open", actual, "2024-03-15 09:30:00")
    # TODO test nested from inside only 1
    print("\nTesting nested closure window from within outer only using "
          "2024-03-18 10:00:00")
    print("This confirms that multiple overlapping events don't muck it up.")
    t = dhu.dt_as_dt("2024-03-18 10:00:00")
    print(f"{str(t)}\n")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t, events=events))
    test_boundary("next_eth_close", actual, "2024-03-20 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t, events=events))
    test_boundary("next_rth_close", actual, "2024-03-20 15:59:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t, events=events))
    test_boundary("previous_eth_close", actual, "2024-03-15 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t, events=events))
    test_boundary("previous_rth_close", actual, "2024-03-15 15:59:00")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t, events=events))
    test_boundary("next_eth_open", actual, "2024-03-20 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t, events=events))
    test_boundary("next_rth_open", actual, "2024-03-20 09:30:00")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t, events=events))
    test_boundary("previous_eth_open", actual, "2024-03-17 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t, events=events))
    test_boundary("previous_rth_open", actual, "2024-03-15 09:30:00")
    print("\nTesting nested closure window from within outer only using "
          "2024-03-19 06:00:00")
    print("This confirms that multiple overlapping events don't muck it up.")
    t = dhu.dt_as_dt("2024-03-19 06:00:00")
    print(f"{str(t)}\n")
    # Next ETH Close
    actual = dhu.dt_as_str(sym.get_next_eth_close(t, events=events))
    test_boundary("next_eth_close", actual, "2024-03-20 16:59:00")
    # Next RTH Close
    actual = dhu.dt_as_str(sym.get_next_rth_close(t, events=events))
    test_boundary("next_rth_close", actual, "2024-03-20 15:59:00")
    # Previous ETH Close
    actual = dhu.dt_as_str(sym.get_previous_eth_close(t, events=events))
    test_boundary("previous_eth_close", actual, "2024-03-15 16:59:00")
    # Previous RTH Close
    actual = dhu.dt_as_str(sym.get_previous_rth_close(t, events=events))
    test_boundary("previous_rth_close", actual, "2024-03-15 15:59:00")
    # Next ETH Open
    actual = dhu.dt_as_str(sym.get_next_eth_open(t, events=events))
    test_boundary("next_eth_open", actual, "2024-03-20 18:00:00")
    # Next RTH Open
    actual = dhu.dt_as_str(sym.get_next_rth_open(t, events=events))
    test_boundary("next_rth_open", actual, "2024-03-20 09:30:00")
    # Previous ETH Open
    actual = dhu.dt_as_str(sym.get_previous_eth_open(t, events=events))
    test_boundary("previous_eth_open", actual, "2024-03-17 18:00:00")
    # Previous RTH Open
    actual = dhu.dt_as_str(sym.get_previous_rth_open(t, events=events))
    test_boundary("previous_rth_open", actual, "2024-03-15 09:30:00")
    print("-----------------------------------------------------------------")
    print("-----------------------------------------------------------------")
    if boundaries_all_ok:
        print("OK: All boundary tests were successful")
    else:
        print("ERROR: Some boundary tests failed, please review.")

    print("\n######################### INDICATORS ###########################")
    # Indicators
    print("Building 5m9sma for 2025-01-08 9:30am-11:30am")
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
    print(itest.get_info())
    print(f"length of candle_chart: {len(itest.candle_chart.c_candles)}")
    print("Last 5 datapoints:")
    for d in itest.datapoints[:5]:
        print(d)
    print("\n################################################")
    print("Building e1h9ema for 2025-01-08 - 2025-01-12")
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
    print("\n.get_info():")
    print(itest.get_info())
    print("\n------------------------------------------------")
    print("Validate candles as expected")
    print(f"length of candle_chart: {len(itest.candle_chart.c_candles)}")
    expected = [{'dt': '2025-01-10 15:00:00', 'value': 5890.25,
                 'ind_id': 'ethESe1hTestEMA-DELETEME', 'epoch': 1736539200},
                {'dt': '2025-01-10 16:00:00', 'value': 5884.5,
                 'ind_id': 'ethESe1hTestEMA-DELETEME', 'epoch': 1736542800},
                {'dt': '2025-01-12 18:00:00', 'value': 5879.6,
                 'ind_id': 'ethESe1hTestEMA-DELETEME', 'epoch': 1736722800},
                {'dt': '2025-01-12 19:00:00', 'value': 5875.38,
                 'ind_id': 'ethESe1hTestEMA-DELETEME', 'epoch': 1736726400},
                {'dt': '2025-01-12 20:00:00', 'value': 5869.3,
                 'ind_id': 'ethESe1hTestEMA-DELETEME', 'epoch': 1736730000},
                ]
    calculated = itest.datapoints[-5:]
    print("(E)xpected vs (C)alculated last 5 datapoints:")
    for i in range(5):
        print(f"E: {expected[i]}")
        print(f"C: {calculated[i]}")
    print("If expected and calculated don't match above, something is broken")
    print("\n------------------------------------------------")
    print("\nTesting getting datapoints by dt using 2025-01-12 18:00:00")
    dp_dt = "2025-01-12 18:00:00"
    print("\nCurrent 2025-01-12 18:00:00 is expected (Sunday 6pm)")
    print(itest.get_datapoint(dt=dp_dt))
    print("\nNext    2025-01-12 19:00:00 is expected (7pm / 1hr later)")
    print(itest.next_datapoint(dt=dp_dt))
    print("\nPrev    2025-01-10 16:00:00 is expected (last candle on Friday)")
    print(itest.prev_datapoint(dt=dp_dt))
    print("\n################################################")

    # Testing storage and retrieval
    print("Testing indicator storage and retrieval")
    result = itest.store()
    print(f"Actual completion time: {result['elapsed'].elapsed_str}")
    print("\nIndicators storage result:")
    print(result["indicator"])
    print(result.keys())
    print(f"Skipped datapoints: {result['datapoints_skipped']}")
    print(f"Stored datapoints: {result['datapoints_stored']}")
    print("\n------------------------------------------------")
    print(f"Listing all indicators in storage, should include {itest.ind_id}")
    indicators = dhs.list_indicators()
    for i in indicators:
        print(f"* {i['ind_id']} {i['description']}")
    print(f"\nRetrieving stored datapoints for {itest.ind_id}\n")
    datapoints = dhs.get_indicator_datapoints(
            ind_id=itest.ind_id)
    for d in datapoints:
        print(d)
    print("\n------------------------------------------------")
    print("Updating TestEMA-DELETEME to add another day then storing again")
    print("We should see 23 datapoints skipped and 46 stored here")
    itest.end_dt = "2025-01-14 20:00:00"
    itest.load_underlying_chart()
    itest.calculate()
    result = itest.store()
    print(f"Actual completion time: {result['elapsed'].elapsed_str}")
    print("\nIndicators storage result:")
    print(result["indicator"])
    print(result.keys())
    print(f"Skipped datapoints: {result['datapoints_skipped']}")
    print(f"Stored datapoints: {result['datapoints_stored']}")
    if (result["datapoints_skipped"] == 23
            and result["datapoints_stored"] == 46):
        print("OK: 23 skipped and 46 stored are the expected results!")
    else:
        print("ERROR: 23 skipped and 46 stored are the expected results...")
    print("\n------------------------------------------------")
    print(f"\nRetrieving stored datapoints for {itest.ind_id}, confirm "
          "there are no duplicate 'dt' datetimes.\n")
    datapoints = dhs.get_indicator_datapoints(
            ind_id=itest.ind_id)
    for d in datapoints:
        print(d)
    print("\n------------------------------------------------")
    print("Attempting to load the stored Indicator and it's Datapoints "
          "into a new variable from storage.  This should match the existing.")
    itoo = dhs.get_indicator(ind_id=itest.ind_id)
    prev_info = itest.get_info()
    new_info = itoo.get_info()
    # adjusting dates to allow comparison as they don't get stored
    new_info["start_dt"] = "2025-01-08 00:00:00"
    new_info["end_dt"] = "2025-01-14 20:00:00"
    print(f"Source var:\n{itest.get_info()}")
    print(f"New var:\n{itoo.get_info()}")
    if prev_info == new_info:
        print("\nOK: Everything matches!")
    else:
        print("\nERROR: Something doesn't look right")
    print("\n\n------------------------------------------------")
    print("\nRemoving test documents from storage\n")
    print(dhs.delete_indicator(itest.ind_id))
    print(f"\nListing stored indicators; should NOT include {itest.ind_id}")
    indicators = dhs.list_indicators()
    for i in indicators:
        print(f"* {i['ind_id']} {i['description']}")
    print(f"\nRetrieving stored datapoints for {itest.ind_id} which should "
          "no longer exist:\n")
    datapoints = dhs.get_indicator_datapoints(
            ind_id=itest.ind_id)
    for d in datapoints:
        print(d)
    print("\n------------------------------------------------")
    print("We're done here.")
