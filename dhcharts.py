import datetime as dt
from datetime import timedelta
import sys
import json
import dhutil as dhu
from dhutil import log_say
import dhstore as dhs
from statistics import fmean
from copy import deepcopy
import logging
from math import ceil, floor

CANDLE_TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', '1d', '1w']
BEGINNING_OF_TIME = "2008-01-01 00:00:00"

# Market Era Definitions
# Define different historical market structures with start dates and schedules.
# To add new eras: append a new dict to this list with start_date and schedule.
# Eras are ordered chronologically; the system finds the era by checking which
# start_date the target_dt is >= to (uses the latest matching start_date).
MARKET_ERAS = [
    {
        "name": "2008_thru_2012",
        "start_date": dt.date(2008, 1, 1),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(17, 29, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 15, 0),
        },
        "closed_hours": {
            "eth": {
                # Two close periods per day: 16:15-16:30 and 17:30-18:00
                0: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "18:00:00"}],
                1: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "18:00:00"}],
                2: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "18:00:00"}],
                3: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "18:00:00"}],
                4: [{"close": "16:15:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "18:00:00"}]
            },
            "rth": {
                # RTH closed at 16:15
                0: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:15:00", "open": "23:59:59"}],
                1: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:15:00", "open": "23:59:59"}],
                2: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:15:00", "open": "23:59:59"}],
                3: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:15:00", "open": "23:59:59"}],
                4: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:15:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "23:59:59"}]
            }
        }
    },
    {
        "name": "2013_thru_present",
        "start_date": dt.date(2012, 11, 17),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(16, 59, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 14, 0),
        },
        "closed_hours": {
            "eth": {
                # Single close period: 17:00-18:00
                0: [{"close": "17:00:00", "open": "18:00:00"}],
                1: [{"close": "17:00:00", "open": "18:00:00"}],
                2: [{"close": "17:00:00", "open": "18:00:00"}],
                3: [{"close": "17:00:00", "open": "18:00:00"}],
                4: [{"close": "17:00:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "18:00:00"}]
            },
            "rth": {
                # RTH closes at 16:00
                0: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:00:00", "open": "23:59:59"}],
                1: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:00:00", "open": "23:59:59"}],
                2: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:00:00", "open": "23:59:59"}],
                3: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:00:00", "open": "23:59:59"}],
                4: [{"close": "00:00:00", "open": "09:30:00"},
                    {"close": "16:00:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "23:59:59"}]
            }
        }
    }
]

log = logging.getLogger("dhcharts")
log.addHandler(logging.NullHandler())


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
        self._closed_hours_cache = {}
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
        """Sets times for known Symbol objects. These are the CURRENT era
        times (latest era in MARKET_ERAS). Use get_times_for_era() with
        dynamic era detection for historical times.
        """
        # DELETEME mimics ES and is used for testing storage to avoid
        # polluting actual ES related data
        if self.ticker in ["ES", "DELETEME"]:
            # Set to latest era times
            latest_era = MARKET_ERAS[-1]
            times = latest_era["times"]
            self.eth_open_time = times["eth_open"]
            self.eth_close_time = times["eth_close"]
            self.rth_open_time = times["rth_open"]
            self.rth_close_time = times["rth_close"]
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
        return round(ceil(f / self.tick_size) * self.tick_size, 2)

    def get_next_tick_down(self, f: float):
        """Returns the next tick available at or below the provided value"""
        return round(floor(f / self.tick_size) * self.tick_size, 2)

    def get_era(self, target_dt):
        """Determine which market era the target_dt falls into.

        Finds the appropriate era by checking which start_date the target
        falls on or after. Uses the latest matching era definition.

        Args:
            target_dt: datetime, date, or string representing the target date

        Returns:
            dict: The era definition dict containing name, start_date, times,
                  and closed_hours
        """
        d = dhu.dt_as_dt(target_dt).date()

        # Find the latest era whose start_date is <= target date
        # MARKET_ERAS must be sorted chronologically
        matching_era = None
        for era in MARKET_ERAS:
            if d >= era["start_date"]:
                matching_era = era
            else:
                break

        if matching_era is None:
            raise ValueError(
                f"No market era defined for date {d}. "
                f"Earliest era starts at {MARKET_ERAS[0]['start_date']}"
            )

        return matching_era

    def get_times_for_era(self, era):
        """Get the market times (open/close) for a specific era.

        Args:
            era: Either an era dict (from get_era()) or an era name string

        Returns:
            dict: {eth_open, eth_close, rth_open, rth_close} as time objects
        """
        # Support both era dict and era name string
        if isinstance(era, dict):
            return era["times"]
        else:
            # era is a name string, find it in MARKET_ERAS
            for era_def in MARKET_ERAS:
                if era_def["name"] == era:
                    return era_def["times"]
            raise ValueError(f"Unknown era name: {era}")

    def get_closed_hours_for_era(self, target_dt, trading_hours: str):
        """Build the closed hours dictionary for a specific era and
        trading_hours type.

        Args:
            target_dt: datetime to determine which era
            trading_hours: "eth" or "rth"

        Returns:
            dict: {day_of_week -> list of {close, open} time ranges}
        """
        era = self.get_era(target_dt)

        if trading_hours not in era["closed_hours"]:
            raise ValueError(
                f"trading_hours '{trading_hours}' not defined for "
                f"era '{era['name']}'"
            )

        # Check cache first
        cache_key = (era["name"], trading_hours)
        if cache_key in self._closed_hours_cache:
            return self._closed_hours_cache[cache_key]

        # Convert string times to time objects
        closed_schedule = era["closed_hours"][trading_hours]
        closed = {}
        for day, periods in closed_schedule.items():
            closed[day] = [
                {
                    "close": dhu.dt_as_time(period["close"]),
                    "open": dhu.dt_as_time(period["open"])
                }
                for period in periods
            ]

        # Cache the result
        self._closed_hours_cache[cache_key] = closed

        return closed

    def market_is_open(self,
                       trading_hours: str,
                       target_dt,
                       check_closed_events: bool = True,
                       events: list = None,
                       ):
        """Returns True if target_dt is within market hours and no Events with
        category 'Closed' overlap as pulled from central storage. Uses dynamic
        era detection to apply correct historical market hours.
        """
        # Set vars needed to evaluate
        d = dhu.dt_as_dt(target_dt)
        dow = d.weekday()
        time = d.time()

        # Get closed hours for the era this datetime falls into
        closed = self.get_closed_hours_for_era(target_dt, trading_hours)

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
            # Set Target Time based on standard hours for the era
            # containing target_dt
            era_times = self.get_times_for_era(self.get_era(target_dt))
            if trading_hours == "eth" and boundary == "open":
                tt = era_times["eth_open"]
            if trading_hours == "eth" and boundary == "close":
                tt = era_times["eth_close"]
            if trading_hours == "rth" and boundary == "open":
                tt = era_times["rth_open"]
            if trading_hours == "rth" and boundary == "close":
                tt = era_times["rth_close"]
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

    def get_next_open(self,
                      target_dt,
                      trading_hours,
                      adjust_for_events: bool = True,
                      events: list = None,
                      ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours=trading_hours,
                                        boundary="open",
                                        order="next",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_previous_open(self,
                          target_dt,
                          trading_hours,
                          adjust_for_events: bool = True,
                          events: list = None,
                          ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours=trading_hours,
                                        boundary="open",
                                        order="previous",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_next_close(self,
                       target_dt,
                       trading_hours,
                       adjust_for_events: bool = True,
                       events: list = None,
                       ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours=trading_hours,
                                        boundary="close",
                                        order="next",
                                        adjust_for_events=adjust_for_events,
                                        events=events,
                                        )

    def get_previous_close(self,
                           target_dt,
                           trading_hours,
                           adjust_for_events: bool = True,
                           events: list = None,
                           ):
        """Simple wrapper for get_market_boundary()"""
        return self.get_market_boundary(target_dt=target_dt,
                                        trading_hours=trading_hours,
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

    def brief(self):
        """Return a single line summary string of this Candle's vitals"""
        if isinstance(self.c_symbol, str):
            ticker = self.c_symbol
        else:
            ticker = self.c_symbol.ticker
        return (f"{ticker} {self.c_timeframe} {self.c_datetime} | "
                f"O: {self.c_open} H: {self.c_high} L: {self.c_low} "
                f"C: {self.c_close} V: {self.c_volume}")

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

    def restrict_dates(self, new_start_dt: str, new_end_dt: str):
        """Reduce the date range of the Chart and remove any Candles that are
        no longer in bounds"""
        os = dhu.dt_as_dt(self.c_start)
        oe = dhu.dt_as_dt(self.c_end)
        ns = dhu.dt_as_dt(new_start_dt)
        ne = dhu.dt_as_dt(new_end_dt)
        ns_epoch = dhu.dt_to_epoch(new_start_dt)
        ne_epoch = dhu.dt_to_epoch(new_end_dt)
        # Ensure new dates don't expand the daterange, they should only reduce
        # or keep unchanged
        if ns < os:
            raise ValueError(f"new_start_dt {new_start_dt} cannot be earlier "
                             f"than the current self.c_start {self.c_start}")
        if ne > oe:
            raise ValueError(f"new_end_dt {new_end_dt} cannot be later "
                             f"than the current self.c_end {self.c_end}")
        # Update Chart dates
        self.c_start = new_start_dt
        self.c_end = new_end_dt
        # Remove any candles outside of the new range by rebuilding the list
        # with only candles that fall in the new range using epoch comparison
        self.c_candles = [c for c in self.c_candles
                          if ns_epoch <= c.c_epoch <= ne_epoch
                          ]
        self.review_candles()


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
        # Get rth_close_time dynamically from symbol for the era of d_date
        target_dt = dt.datetime.combine(d_date, dt.time(16, 0, 0))
        era_times = d_symbol.get_times_for_era(d_symbol.get_era(target_dt))
        rth_end_time = era_times["rth_close"]

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
            if c.c_timeframe == new_chart.c_timeframe:
                chart_exists = True
        if not chart_exists:
            self.d_charts.append(new_chart)
        else:
            print('Unable to add chart with timeframe '
                  f"{new_chart['c_timeframe']} because a chart already exists"
                  ' using this timeframe.  Use update_chart() to overwrite.')

    def update_chart(self, new_chart):
        if not isinstance(new_chart, Chart):
            raise TypeError(f"new_chart {type(new_chart)} must be a "
                            "<class dhcharts.Chart> object")
        to_remove = []
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
                 autoload_chart=True,
                 candle_chart=None,
                 datapoints: list = None,
                 parameters={},
                 ):
        """Base class for indicators.  Names should be short and simple
        abbreviations as they are used in tagging and storage, think like
        sma, hod, vwap, etc.  This class won't be used directly, use it's
        child classes which will be indicator type specific"""
        self.name = name
        self.description = description
        if not dhu.valid_timeframe(timeframe):
            raise ValueError(f"{timeframe} not valid for timeframe")
        self.timeframe = timeframe
        if not dhu.valid_trading_hours(trading_hours):
            raise ValueError(f"{trading_hours} not valid for trading_hours")
        self.trading_hours = trading_hours
        if isinstance(symbol, str):
            self.symbol = dhs.get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.calc_version = calc_version
        self.calc_details = calc_details
        self.start_dt = start_dt
        self.end_dt = end_dt
        if self.end_dt is None:
            self.end_dt = dhu.dt_as_str(dt.datetime.now())
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
        self.autoload_chart = autoload_chart
        self.candle_chart = candle_chart
        if self.candle_chart is None and self.autoload_chart:
            self.load_underlying_chart()
        self.sort_datapoints()

    def __eq__(self, other):
        return (self.name == other.name
                and self.description == other.description
                and self.timeframe == other.timeframe
                and self.trading_hours == other.trading_hours
                and self.symbol == other.symbol
                and self.calc_version == other.calc_version
                and self.calc_details == other.calc_details
                and self.start_dt == other.start_dt
                and self.end_dt == other.end_dt
                and self.ind_id == other.ind_id
                and self.candle_chart == other.candle_chart
                and self.datapoints == other.datapoints
                and self.sub_eq(other)
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def sub_eq(self, other):
        """Placeholder method for subclasses to add additional attributes
        or conditions required to evaluate __eq__ for this class.  Any
        comparison of parameters should be done here as they are subclass
        specific."""
        return self.parameters == other.parameters

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
        if self.start_dt is None or self.end_dt is None:
            self.candle_chart = None
        else:
            self.candle_chart = Chart(c_timeframe=self.timeframe,
                                      c_trading_hours=self.trading_hours,
                                      c_symbol=self.symbol,
                                      c_start=self.start_dt,
                                      c_end=self.end_dt,
                                      autoload=True,
                                      )
        return self.candle_chart

    def load_datapoints(self):
        """Load any datapoints available from central storage by ind_id,
        start_dt, and end_dt.
        """
        self.datapoints = dhs.get_indicator_datapoints(
                ind_id=self.ind_id,
                earliest_dt=self.start_dt,
                latest_dt=self.end_dt,
                )
        self.sort_datapoints()

    def sort_datapoints(self):
        """Sort attached datapoints in chronological order"""
        if len(self.datapoints) == 0:
            return False
        self.datapoints.sort(key=lambda dp: dp.epoch)

    def datapoint_indexes_by_epoch(self):
        result = {}
        for i, dp in enumerate(self.datapoints):
            result[dp.epoch] = i
        return result

    def datapoint_indexes_by_dt(self):
        result = {}
        for i, dp in enumerate(self.datapoints):
            result[dp.dt] = i
        return result

    def calculate(self):
        """This method will be specific to each type of indicator.  It should
        accept only a list of Candles, sort it, and calculate new indicator
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
        log_say("Parent class calculations are for testing purposes only")
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
        self.sort_datapoints()

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
                 autoload_chart=True,
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
                         autoload_chart=autoload_chart,
                         candle_chart=candle_chart,
                         datapoints=datapoints,
                         parameters=parameters,
                         )
        """Subclass of Indicator() specifically used for simple moving avg"""
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
                 autoload_chart=True,
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
                         autoload_chart=autoload_chart,
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
        if self.method not in supported_methods:
            raise TypeError(f"self.method {self.method} not supported, "
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
