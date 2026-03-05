"""Common utility functions and constants for the trading system.

This module serves as a dependency-free utility layer, importing only from
the Python standard library. It contains functions for:
- DateTime conversion and formatting
- Timeframe and trading hour validation
- Market calendar calculations (with market_is_open checks via Symbol
  objects)
- Progress bar utilities
- General helpers and constants

CRITICAL ARCHITECTURAL ROLE: This module is at the base of the import tree
and contains NO external dependencies (no dhstore, dhtypes, or other
dhtrader imports). This makes it the safe place to define functions that
would otherwise create circular import issues.

Core functions here include:
- next_candle_start(): Calculate next valid candle start time (requires
  Symbol)
- expected_candle_datetimes(): Calculate all expected candle times in a
  range
- this_candle_start(): Find the start of the current candle
- DateTime utilities: dt_as_str, dt_as_dt, dt_to_epoch, dt_from_epoch,
  etc.

These functions are imported by other modules throughout the system,
including dhstore, dhutil, and dhtypes.
"""
from datetime import datetime as dt
from datetime import timedelta, date, time
from copy import deepcopy
import re
import logging
import json
import progressbar

TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', 'r1d', 'e1d', 'r1w', 'e1w',
              'r1mo', 'e1mo']
TRADING_HOURS = ['rth', 'eth']
EVENT_CATEGORIES = ['Closed', 'Data', 'Unplanned', 'LowVolume', 'Rollover']

DT_STR_FORMAT = "%Y-%m-%d %H:%M:%S"
DT_STR_CANONICAL_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
)
DT_STR_FLEX_REGEX = re.compile(
    r"^\s*(\d{2,4})-(\d{1,2})-(\d{1,2})\s+"
    r"(\d{1,2}):(\d{1,2}):(\d{1,2})\s*$"
)
TIMEFRAME_DELTAS = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "r1h": timedelta(hours=1),
    "e1h": timedelta(hours=1),
    "r1d": timedelta(days=1),
    "e1d": timedelta(days=1),
    "r1w": timedelta(weeks=1),
    "e1w": timedelta(weeks=1),
}

log = logging.getLogger("dhcommon")
log.addHandler(logging.NullHandler())


def log_say(msg, level="info"):
    """Log messages while also printing to console"""
    print(msg)
    if level == "debug":
        log.debug(msg)
    if level == "info":
        log.info(msg)
    if level == "warn":
        log.warn(msg)
    if level == "error":
        log.error(msg)
    if level == "critical":
        log.critical(msg)


class OperationTimer():
    def __init__(self,
                 name: str,
                 start_dt=None,
                 end_dt=None,
                 elapsed_dt=None,
                 elapsed_str="",
                 auto_start: bool = True,
                 ):
        self.name = name
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.elapsed_dt = elapsed_dt
        self.elapsed_str = elapsed_str
        self.auto_start = auto_start
        if self.auto_start:
            self.start()

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        self.update_elapsed()
        working = deepcopy(self.__dict__)
        if self.start_dt is not None:
            working["start_dt"] = dt_as_str(self.start_dt)
        if self.end_dt is not None:
            working["end_dt"] = dt_as_str(self.end_dt)
        if self.elapsed_dt is not None:
            working["elapsed_dt"] = str(self.elapsed_dt)

        return json.dumps(working)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def summary(self):
        """Provide a one line str summary of the timer's current status, useful
        for monotiring running timers or final review."""
        self.update_elapsed()
        return (f"OpTimer {self.name} | started {dt_as_str(self.start_dt)} | "
                f"elapsed {self.elapsed_str} | ended {dt_as_str(self.end_dt)}")

    def start(self):
        self.start_dt = dt.now()

    def update_elapsed(self):
        if self.end_dt is None:
            now = dt.now()
        else:
            now = self.end_dt
        self.elapsed_dt = now - self.start_dt
        self.elapsed_str = re.sub("\\..*", "", str(self.elapsed_dt))

    def stop(self):
        self.end_dt = dt.now()
        self.update_elapsed()


class ProgBar():
    """Wrapper to make progress bars smoother to implement"""
    def __init__(self,
                 total: int,
                 desc: str = "TradeSeries calculated",
                 auto_start: bool = True,
                 ):
        self.total = total
        self.desc = desc
        self.auto_start = auto_start
        if self.auto_start:
            self.start()

    def start(self):
        bar_label = (f"%(value)d of {self.total} {self.desc} in %(elapsed)s ")
        eta_widget = progressbar.ETA(
                format_not_started='--:--:--',
                format_finished='Time: %(elapsed)8s',
                format='Remaining: %(eta)8s',
                format_zero='Remaining: 00:00:00',
                format_na='Remaining: N/A',
                )
        widgets = [progressbar.Percentage(),
                   progressbar.Bar(),
                   progressbar.FormatLabel(bar_label),
                   eta_widget,
                   ]
        self.this_bar = progressbar.ProgressBar(widgets=widgets,
                                                max_value=self.total).start()

    def update(self, val):
        self.this_bar.update(val)

    def increment(self, val=1):
        self.this_bar.increment(val)

    def finish(self):
        self.this_bar.finish()


def sort_dict(d: dict):
    """Uses insertion ordering to sort a dictionary by keys"""
    keys = []
    sorted_dict = {}
    for k in d.keys():
        keys.append(k)
    keys = sorted(keys)
    for k in keys:
        sorted_dict[k] = d[k]

    return sorted_dict


def diff_dicts(dict1, dict2):
    """
    Compares two dictionaries and returns a dictionary of differences in the
    form of tuples representing the values found in each dict in the order
    that the dicts were pass in.

    This will present None if a key in one dict was not found in the other.

    It will return {} if no differences were found.
    """
    diff = {}
    all_keys = set(dict1.keys()) | set(dict2.keys())
    for key in all_keys:
        if key not in dict1:
            diff[key] = (None, dict2[key])
        elif key not in dict2:
            diff[key] = (dict1[key], None)
        elif dict1[key] != dict2[key]:
            diff[key] = (dict1[key], dict2[key])
    return diff


def prompt_yn(msg):
    p = ""
    while p not in ["Y", "y", "N", "n"]:
        p = input(f"{msg} (Y/N)?:")
    if p in ["Y", "y"]:
        return True
    else:
        return False


def valid_timeframe(t, exit=True):
    if t in TIMEFRAMES:
        return True
    else:
        err_msg = f"{t} is not a valid timeframe in {TIMEFRAMES}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def valid_trading_hours(t, exit=True):
    if t in TRADING_HOURS:
        return True
    else:
        err_msg = f"{t} is not a valid specifier in {TRADING_HOURS}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def check_tf_th_compatibility(tf, th, exit=True):
    """Confirm that a given timeframe (tf) and trading hours (th) are
    compatible.  Usually we want to exit if this is not so as data cannot be
    trusted otherwise.
    """
    result = True
    if th == "eth":
        if tf in ["r1h", "r1d", "r1w", "r1mo"]:
            result = False
    if th == "rth":
        if tf in ["e1h", "e1d", "e1w", "e1mo"]:
            result = False
    if exit and not result:
        raise ValueError(f"timeframe {tf} and trading_hours {th} cannot "
                         "coexist, please change one of them.")

    return result


def valid_event_category(c, exit=True):
    if c in EVENT_CATEGORIES:
        return True
    else:
        err_msg = "{c} is not a valid event category in {EVENT_CATEGORIES}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def dt_as_dt(d):
    """return a datetime object representing the given datetime, string, or
    None input"""
    if d is None:
        return None
    if isinstance(d, dt):
        return d

    if not isinstance(d, str):
        raise TypeError(f"d must be str, datetime, or None. Got {type(d)}")

    # First attempt to match the standard canonical format
    if DT_STR_CANONICAL_REGEX.fullmatch(d):
        return dt.strptime(d, DT_STR_FORMAT)

    # If that fails, attempt to match the more flexible format which may
    # exclude leading zeroes in single digit values or reduce years to 2 digits
    match = DT_STR_FLEX_REGEX.fullmatch(d)
    if match is not None:
        year = int(match.group(1))
        if year < 100:
            year += 2000
        month = int(match.group(2))
        day = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))
        second = int(match.group(6))
        return dt(year, month, day, hour, minute, second)

    # Raise a ValueError Exception if we could not successfully parse a valid
    # datetime from the given string
    raise ValueError(
        f"Unsupported datetime string format: {d}. Expected "
        "YYYY-mm-dd HH:MM:SS or shorthand with '-' separators."
    )


def dt_as_str(d):
    """return a string object representing the given datetime, string, or
     None input"""
    if d is None:
        return None

    # If a string is given, return an equivalent string in the standard format
    if isinstance(d, str):
        if DT_STR_CANONICAL_REGEX.fullmatch(d):
            return d
        return dt_as_dt(d).strftime(DT_STR_FORMAT)

    # If a datetime is given, convert to a string in the standard format
    if isinstance(d, dt):
        return d.strftime(DT_STR_FORMAT)

    # Raise an error if the input was not a string, datetime, or None
    raise TypeError(f"d must be str, datetime, or None. Got {type(d)}")


def dt_as_time(time: str):
    """Return a datetime.time object for the given %H:%M:%S string"""
    if time is None:
        return None
    return dt_as_dt(f"2000-01-01 {time}").time()


def dow_name(dow: int):
    """Return the human name for a day of the week given it's index as
    represented in datetime.weekday()"""
    names = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
             4: "Friday", 5: "Saturday", 6: "Sunday"}
    return names[dow]


def dt_to_epoch(d):
    """return an epoch integer from a datetime or string"""
    if d is None:
        return None
    return int(dt_as_dt(d).timestamp())


def dt_from_epoch(d):
    """return a datetime object from an epoch integer"""
    if d is None:
        return None
    return dt.fromtimestamp(d)


def timeframe_delta(timeframe: str):
    """return a precalculated timedelta object for the timeframe given"""
    if timeframe in TIMEFRAME_DELTAS:
        return TIMEFRAME_DELTAS[timeframe]

    raise ValueError(f"timeframe: {timeframe} not supported")


def start_of_week_date(dt):
    """Return the date object for the starting Sunday of the week in which
    the provided datetime exists."""
    dt = dt_as_dt(dt)
    if dt.weekday() == 6:
        week_dt = dt.date()
    else:
        week_dt = dt.date() - timedelta(days=dt.weekday() + 1)
    return week_dt


def dict_of_weeks(start_dt, end_dt, template):
    """Return a template dictionary with keys for each week that exist in
    the provided timeframe from start_dt to end_dt, with a value equal to the
    template passed in.  Generally this will be used for aggregating weekly
    stats.  Sunday is used as the start of the week to match market behavior
    and cover extended hours trading strategies.

    template should be provided as a dictionary with default values for each
    week.  For example:

        template = {"total_trades": 0,
                    "profitable_trades": 0,
                    "losing_trades": 0,
                    "gl_in_ticks": 0,
                    "success_rate": "nil",
                    }
    """
    start_dt = dt_as_dt(start_dt)
    end_dt = dt_as_dt(end_dt)
    adder = timedelta(weeks=1)
    week_dt = start_of_week_date(start_dt)
    result = {}
    while week_dt <= end_dt.date():
        result[f"{week_dt}"] = deepcopy(template)
        week_dt += adder
    return result


def this_candle_start(dt, timeframe: str):
    """Returns the datetime that represents a parent candle start
    in which the given datetime would exist in this timeframe.  May return the
    same as input.  This does not confirm market open like next_candle_start()
    since it may not be able to provide an answer in some cases..
    """
    this_dt = dt_as_dt(dt)
    min_delta = timedelta(minutes=1)
    # Start by removing secs and microsecs to get to the whole minute
    this_dt = this_dt.replace(microsecond=0, second=0)
    # Now drop back 1 minute at a time to reach the timeframe correct start
    if timeframe == "1m":
        pass
    elif timeframe == "5m":
        while this_dt.minute % 5 != 0:
            this_dt = this_dt - min_delta
    elif timeframe == "15m":
        while this_dt.minute % 15 != 0:
            this_dt = this_dt - min_delta
    elif timeframe == "r1h":
        while this_dt.minute != 30:
            this_dt = this_dt - min_delta
    elif timeframe == "e1h":
        while this_dt.minute != 0:
            this_dt = this_dt - min_delta
    else:
        raise ValueError(f"timeframe: {timeframe} not supported")

    return this_dt


def next_candle_start(dt,
                      trading_hours: str,
                      symbol,
                      timeframe: str = "1m",
                      events: list = None,
                      ):
    """Return the next datetime that represents a valid candle start.

    symbol must be a Symbol-like object implementing market_is_open().
    """
    if isinstance(symbol, str):
        raise TypeError("symbol must be a Symbol object, not str")
    valid_trading_hours(trading_hours)
    check_tf_th_compatibility(tf=timeframe, th=trading_hours)
    next_dt = dt_as_dt(dt).replace(microsecond=0, second=0)
    min_delta = timedelta(minutes=1)

    done = False
    while not done:
        next_dt = next_dt + min_delta
        if timeframe == "5m":
            while next_dt.minute % 5 != 0:
                next_dt = next_dt + min_delta
        elif timeframe == "15m":
            while next_dt.minute % 15 != 0:
                next_dt = next_dt + min_delta
        elif timeframe == "r1h":
            while next_dt.minute != 30:
                next_dt = next_dt + min_delta
        elif timeframe == "e1h":
            while next_dt.minute != 0:
                next_dt = next_dt + min_delta
        elif timeframe != "1m":
            raise ValueError(f"timeframe: {timeframe} not supported")
        done = symbol.market_is_open(trading_hours=trading_hours,
                                     target_dt=next_dt,
                                     check_closed_events=True,
                                     events=events,
                                     )

    return next_dt


def expected_candle_datetimes(start_dt,
                              end_dt,
                              timeframe: str,
                              symbol,
                              events: list = None,
                              exclude_categories: list = None,
                              ):
    """Return expected candle datetimes for a symbol in a datetime range."""
    if isinstance(symbol, str):
        raise TypeError("symbol must be a Symbol object, not str")
    if symbol.ticker == "ES":
        trading_hours = "rth" if timeframe == "r1h" else "eth"
    else:
        raise ValueError("Only ES is currently supported as symbol for now")

    result_std = []
    adder = timeframe_delta(timeframe)
    this = this_candle_start(dt=start_dt,
                             timeframe=timeframe,
                             )
    if this != dt_as_dt(start_dt):
        this = next_candle_start(dt=start_dt,
                                 timeframe=timeframe,
                                 trading_hours=trading_hours,
                                 symbol=symbol,
                                 events=events,
                                 )
    ender = dt_as_dt(end_dt)
    while this <= ender:
        if symbol.market_is_open(trading_hours=trading_hours,
                                 target_dt=this,
                                 check_closed_events=False,
                                 ):
            result_std.append(this)
        this = this + adder

    if events is None:
        events = []
    closures = []
    for event in events:
        if event.category == "Closed":
            if exclude_categories and event.category in exclude_categories:
                continue
            closures.append(event)

    result = []
    for candle_dt in result_std:
        include = True
        candle_epoch = dt_to_epoch(candle_dt)
        for event in closures:
            if event.start_epoch <= candle_epoch <= event.end_epoch:
                include = False
        if include:
            result.append(candle_dt)

    return result


def rangify_candle_times(times: list,
                         timeframe: str,
                         ):
    """Takes a list of datetimes and returns a list of aggregated datetime
    ranges.  Primarily intended to make human review sane on large sets of
    gap and unexpected candles during integrity checks"""
    delta = timeframe_delta(timeframe)
    sorted_times = sorted(times)
    ranges = []
    this_range = None
    for t in sorted_times:
        # For the starting range just set both values to the current time
        if this_range is None:
            this_range = {"start_dt": dt_as_str(t), "end_dt": dt_as_str(t)}
        else:
            # If the time is one increment after the previously seen time
            # just update the current range
            if dt_as_dt(t) == dt_as_dt(this_range["end_dt"]) + delta:
                this_range["end_dt"] = dt_as_str(t)
            # Otherwise add the current range to the list and start a new one
            else:
                ranges.append(this_range)
                this_range = None
                this_range = {"start_dt": dt_as_str(t), "end_dt": dt_as_str(t)}
    # The last range won't get added in the loop so add it after
    ranges.append(this_range)

    return ranges


def summarize_candles(timeframe: str,
                      symbol: str = "ES",
                      candles: list = None,
                      ):
    if not isinstance(candles, list):
        raise TypeError(f"candles must be a list, not {type(candles)}")
    times = set()
    mins = set()
    hours = set()
    dates = set()
    datetimes = set()
    dows = set()
    for c in candles:
        this_dt = dt_as_dt(c.c_datetime)
        times.add(str(this_dt.time()))
        minute = str(this_dt.minute)
        if len(minute) < 2:
            minute = "0" + minute
        mins.add(minute)
        hour = str(this_dt.hour)
        if len(hour) < 2:
            hour = "0" + hour
        hours.add(hour)
        dates.add(str(this_dt.date()))
        datetimes.add(str(this_dt))
        dows.add(f"{this_dt.weekday()}{this_dt.strftime('%A')}")
    mins_list = sorted(list(mins))
    hours_list = sorted(list(hours))
    times_list = sorted(list(times))
    dates_list = sorted(list(dates))
    dows_list_nums = sorted(list(dows))
    dows_list = []
    for d in dows_list_nums:
        dows_list.append(d[1:])
    summary_data = {"minutes": mins_list,
                    "hours": hours_list,
                    "times": times_list,
                    "dates": dates_list,
                    "Days of Week": dows_list,
                    }
    summary_expected = {}
    if timeframe == '5m':
        minutes = ['00', '05', '10', '15', '20', '25', '30', '35', '40', '45',
                   '50', '55']
        hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                 '10', '11', '12', '13', '14', '15', '16', '18', '19', '20',
                 '21', '22', '23']
        times = ['00:00:00', '00:05:00', '00:10:00', '00:15:00', '00:20:00',
                 '00:25:00', '00:30:00', '00:35:00', '00:40:00', '00:45:00',
                 '00:50:00', '00:55:00', '01:00:00', '01:05:00', '01:10:00',
                 '01:15:00', '01:20:00', '01:25:00', '01:30:00', '01:35:00',
                 '01:40:00', '01:45:00', '01:50:00', '01:55:00', '02:00:00',
                 '02:05:00', '02:10:00', '02:15:00', '02:20:00', '02:25:00',
                 '02:30:00', '02:35:00', '02:40:00', '02:45:00', '02:50:00',
                 '02:55:00', '03:00:00', '03:05:00', '03:10:00', '03:15:00',
                 '03:20:00', '03:25:00', '03:30:00', '03:35:00', '03:40:00',
                 '03:45:00', '03:50:00', '03:55:00', '04:00:00', '04:05:00',
                 '04:10:00', '04:15:00', '04:20:00', '04:25:00', '04:30:00',
                 '04:35:00', '04:40:00', '04:45:00', '04:50:00', '04:55:00',
                 '05:00:00', '05:05:00', '05:10:00', '05:15:00', '05:20:00',
                 '05:25:00', '05:30:00', '05:35:00', '05:40:00', '05:45:00',
                 '05:50:00', '05:55:00', '06:00:00', '06:05:00', '06:10:00',
                 '06:15:00', '06:20:00', '06:25:00', '06:30:00', '06:35:00',
                 '06:40:00', '06:45:00', '06:50:00', '06:55:00', '07:00:00',
                 '07:05:00', '07:10:00', '07:15:00', '07:20:00', '07:25:00',
                 '07:30:00', '07:35:00', '07:40:00', '07:45:00', '07:50:00',
                 '07:55:00', '08:00:00', '08:05:00', '08:10:00', '08:15:00',
                 '08:20:00', '08:25:00', '08:30:00', '08:35:00', '08:40:00',
                 '08:45:00', '08:50:00', '08:55:00', '09:00:00', '09:05:00',
                 '09:10:00', '09:15:00', '09:20:00', '09:25:00', '09:30:00',
                 '09:35:00', '09:40:00', '09:45:00', '09:50:00', '09:55:00',
                 '10:00:00', '10:05:00', '10:10:00', '10:15:00', '10:20:00',
                 '10:25:00', '10:30:00', '10:35:00', '10:40:00', '10:45:00',
                 '10:50:00', '10:55:00', '11:00:00', '11:05:00', '11:10:00',
                 '11:15:00', '11:20:00', '11:25:00', '11:30:00', '11:35:00',
                 '11:40:00', '11:45:00', '11:50:00', '11:55:00', '12:00:00',
                 '12:05:00', '12:10:00', '12:15:00', '12:20:00', '12:25:00',
                 '12:30:00', '12:35:00', '12:40:00', '12:45:00', '12:50:00',
                 '12:55:00', '13:00:00', '13:05:00', '13:10:00', '13:15:00',
                 '13:20:00', '13:25:00', '13:30:00', '13:35:00', '13:40:00',
                 '13:45:00', '13:50:00', '13:55:00', '14:00:00', '14:05:00',
                 '14:10:00', '14:15:00', '14:20:00', '14:25:00', '14:30:00',
                 '14:35:00', '14:40:00', '14:45:00', '14:50:00', '14:55:00',
                 '15:00:00', '15:05:00', '15:10:00', '15:15:00', '15:20:00',
                 '15:25:00', '15:30:00', '15:35:00', '15:40:00', '15:45:00',
                 '15:50:00', '15:55:00', '16:00:00', '16:05:00', '16:10:00',
                 '16:15:00', '16:20:00', '16:25:00', '16:30:00', '16:35:00',
                 '16:40:00', '16:45:00', '16:50:00', '16:55:00', '18:00:00',
                 '18:05:00', '18:10:00', '18:15:00', '18:20:00', '18:25:00',
                 '18:30:00', '18:35:00', '18:40:00', '18:45:00', '18:50:00',
                 '18:55:00', '19:00:00', '19:05:00', '19:10:00', '19:15:00',
                 '19:20:00', '19:25:00', '19:30:00', '19:35:00', '19:40:00',
                 '19:45:00', '19:50:00', '19:55:00', '20:00:00', '20:05:00',
                 '20:10:00', '20:15:00', '20:20:00', '20:25:00', '20:30:00',
                 '20:35:00', '20:40:00', '20:45:00', '20:50:00', '20:55:00',
                 '21:00:00', '21:05:00', '21:10:00', '21:15:00', '21:20:00',
                 '21:25:00', '21:30:00', '21:35:00', '21:40:00', '21:45:00',
                 '21:50:00', '21:55:00', '22:00:00', '22:05:00', '22:10:00',
                 '22:15:00', '22:20:00', '22:25:00', '22:30:00', '22:35:00',
                 '22:40:00', '22:45:00', '22:50:00', '22:55:00', '23:00:00',
                 '23:05:00', '23:10:00', '23:15:00', '23:20:00', '23:25:00',
                 '23:30:00', '23:35:00', '23:40:00', '23:45:00', '23:50:00',
                 '23:55:00']
    elif timeframe == '15m':
        minutes = ['00', '15', '30', '45']
        hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                 '10', '11', '12', '13', '14', '15', '16', '18', '19', '20',
                 '21', '22', '23']
        times = ['00:00:00', '00:15:00', '00:30:00', '00:45:00',
                 '01:00:00', '01:15:00', '01:30:00', '01:45:00',
                 '02:00:00', '02:15:00', '02:30:00', '02:45:00',
                 '03:00:00', '03:15:00', '03:30:00', '03:45:00',
                 '04:00:00', '04:15:00', '04:30:00', '04:45:00',
                 '05:00:00', '05:15:00', '05:30:00', '05:45:00',
                 '06:00:00', '06:15:00', '06:30:00', '06:45:00',
                 '07:00:00', '07:15:00', '07:30:00', '07:45:00',
                 '08:00:00', '08:15:00', '08:30:00', '08:45:00',
                 '09:00:00', '09:15:00', '09:30:00', '09:45:00',
                 '10:00:00', '10:15:00', '10:30:00', '10:45:00',
                 '11:00:00', '11:15:00', '11:30:00', '11:45:00',
                 '12:00:00', '12:15:00', '12:30:00', '12:45:00',
                 '13:00:00', '13:15:00', '13:30:00', '13:45:00',
                 '14:00:00', '14:15:00', '14:30:00', '14:45:00',
                 '15:00:00', '15:15:00', '15:30:00', '15:45:00',
                 '16:00:00', '16:15:00', '16:30:00', '16:45:00',
                 '18:00:00', '18:15:00', '18:30:00', '18:45:00',
                 '19:00:00', '19:15:00', '19:30:00', '19:45:00',
                 '20:00:00', '20:15:00', '20:30:00', '20:45:00',
                 '21:00:00', '21:15:00', '21:30:00', '21:45:00',
                 '22:00:00', '22:15:00', '22:30:00', '22:45:00',
                 '23:00:00', '23:15:00', '23:30:00', '23:45:00',
                 ]
    elif timeframe == 'r1h':
        minutes = ['30']
        hours = ['09', '10', '11', '12', '13', '14', '15']
        times = ['09:30:00', '10:30:00', '11:30:00', '12:30:00', '13:30:00',
                 '14:30:00', '15:30:00']
    elif timeframe == 'e1h':
        minutes = ['00']
        hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                 '10', '11', '12', '13', '14', '15', '16', '18', '19', '20',
                 '21', '22', '23']
        times = ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00',
                 '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00',
                 '10:00:00', '11:00:00', '12:00:00', '13:00:00', '14:00:00',
                 '15:00:00', '16:00:00', '18:00:00', '19:00:00', '20:00:00',
                 '21:00:00', '22:00:00', '23:00:00']
    else:
        summary_expected = None
    if summary_expected is not None:
        summary_expected = {'minutes': minutes,
                            'hours': hours,
                            'times': times,
                            }

    return {"summary_data": summary_data,
            "summary_expected": summary_expected,
            }


CANDLE_TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', '1d', '1w']
BEGINNING_OF_TIME = "2008-01-01 00:00:00"

MARKET_ERAS = [
    {
        "name": "2008_thru_2012",
        "start_date": date(2008, 1, 1),
        "times": {
            "eth_open": time(18, 0, 0),
            "eth_close": time(17, 29, 0),
            "rth_open": time(9, 30, 0),
            "rth_close": time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                0: [{"close": "16:16:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "17:59:59"}],
                1: [{"close": "16:16:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "17:59:59"}],
                2: [{"close": "16:16:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "17:59:59"}],
                3: [{"close": "16:16:00", "open": "16:30:00"},
                    {"close": "17:30:00", "open": "17:59:59"}],
                4: [{"close": "16:16:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "17:59:59"}]
            },
            "rth": {
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
    },
    {
        "name": "2012holidays_thru_2015holidays",
        "start_date": date(2012, 11, 17),
        "times": {
            "eth_open": time(18, 0, 0),
            "eth_close": time(17, 15, 0),
            "rth_open": time(9, 30, 0),
            "rth_close": time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                0: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:16:00", "open": "17:59:59"}],
                1: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:16:00", "open": "17:59:59"}],
                2: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:16:00", "open": "17:59:59"}],
                3: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:16:00", "open": "17:59:59"}],
                4: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:16:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "17:59:59"}]
            },
            "rth": {
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
    },
    {
        "name": "2015holidays_thru_2020",
        "start_date": date(2015, 9, 19),
        "times": {
            "eth_open": time(18, 0, 0),
            "eth_close": time(16, 59, 0),
            "rth_open": time(9, 30, 0),
            "rth_close": time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                0: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                1: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                2: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                3: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                4: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "17:59:59"}]
            },
            "rth": {
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
    },
    {
        "name": "2021-01_thru_2021-06",
        "start_date": date(2021, 1, 1),
        "times": {
            "eth_open": time(18, 0, 0),
            "eth_close": time(16, 59, 0),
            "rth_open": time(9, 30, 0),
            "rth_close": time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                0: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                1: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                2: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                3: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "17:59:59"}],
                4: [{"close": "16:15:00", "open": "16:30:00"},
                    {"close": "17:00:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "17:59:59"}]
            },
            "rth": {
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
    },
    {
        "name": "2021-06_thru_present",
        "start_date": date(2021, 6, 26),
        "times": {
            "eth_open": time(18, 0, 0),
            "eth_close": time(16, 59, 0),
            "rth_open": time(9, 30, 0),
            "rth_close": time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                0: [{"close": "17:00:00", "open": "17:59:59"}],
                1: [{"close": "17:00:00", "open": "17:59:59"}],
                2: [{"close": "17:00:00", "open": "17:59:59"}],
                3: [{"close": "17:00:00", "open": "17:59:59"}],
                4: [{"close": "17:00:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "17:59:59"}]
            },
            "rth": {
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


def bot():
    """Return universal beginning of time for this and other modules."""
    return BEGINNING_OF_TIME
