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

- next_candle_start(): Calculate next valid candle start time
  (requires Symbol)
- expected_candle_datetimes(): Calculate all expected candle times
  in a range
- this_candle_start(): Find the start of the current candle
- DateTime utilities: dt_as_str, dt_as_dt, dt_to_epoch,
  dt_from_epoch, etc.

These functions are imported by other modules throughout the system,
including dhstore, dhutil, and dhtypes.
"""
from datetime import datetime as dt
from datetime import timedelta, date, time
from copy import deepcopy
import re
import logging
import json
import sys
import progressbar

TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', 'r1d', 'e1d', 'r1w', 'e1w',
              'r1mo', 'e1mo']
TRADING_HOURS = ['rth', 'eth']
TF_TH_MAP = {
    "1m": ["rth", "eth"],
    "5m": ["rth", "eth"],
    "15m": ["rth", "eth"],
    "r1h": ["rth"],
    "e1h": ["eth"],
    "r1d": ["rth"],
    "e1d": ["eth"],
    "r1w": ["rth"],
    "e1w": ["eth"],
    "r1mo": ["rth"],
    "e1mo": ["eth"]
    }
EVENT_CATEGORIES = ['Closed', 'Data', 'Unplanned', 'LowVolume', 'Rollover']
DEFAULT_OBJ_NAME = "nameless"

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


def log_say(msg, level="info", prefix="", suffix=""):
    """Log messages while also printing to console."""
    if prefix:
        msg = f"{prefix} {msg}"
    if suffix:
        msg = f"{msg} {suffix}"
    print(msg, flush=True)
    if level == "debug":
        log.debug(msg)
    if level == "info":
        log.info(msg)
    if level in ["warn", "warning"]:
        log.warning(msg)
    if level == "error":
        log.error(msg)
    if level == "critical":
        log.critical(msg)


class OperationTimer():
    """Tracks elapsed time for a named operation.

    Starts timing automatically on creation unless auto_start is False.
    """

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
        """Return a string representation of the timer's state."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return a string representation suitable for debugging."""
        return str(self)

    def to_json(self):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to strings
        for portability.
        """
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
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json())

    def pretty(self):
        """Return a formatted, indented string representation of this object.

        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def summary(self):
        """Return a one-line string summary of the timer's current status.

        Useful for monitoring running timers or final review.
        """
        self.update_elapsed()
        return (f"OpTimer {self.name} | started {dt_as_str(self.start_dt)} | "
                f"elapsed {self.elapsed_str} | ended {dt_as_str(self.end_dt)}")

    def start(self):
        """Record the current datetime as the start time."""
        self.start_dt = dt.now()

    def update_elapsed(self):
        """Recalculate elapsed time from start to now or to the end time."""
        if self.end_dt is None:
            now = dt.now()
        else:
            now = self.end_dt
        self.elapsed_dt = now - self.start_dt
        self.elapsed_str = re.sub("\\..*", "", str(self.elapsed_dt))

    def stop(self):
        """Record the current datetime as the end time and update elapsed."""
        self.end_dt = dt.now()
        self.update_elapsed()


class ProgBar():
    """Wrapper to make progress bars smoother to implement.

    Args:
        total: Total number of items to track progress for.
        desc: Label string to display alongside the progress count.
        auto_start: If True, start the progress bar immediately.
    """
    def __init__(self,
                 total: int,
                 desc: str = "Things Handled",
                 auto_start: bool = True,
                 ):
        """Initialize a ProgBar instance and optionally start it."""
        self.total = total
        self.desc = desc
        self.auto_start = auto_start
        if self.auto_start:
            self.start()

    def start(self):
        """Initialize and display the underlying progressbar widget."""
        bar_label = f"%(value)d of {self.total} {self.desc} in %(elapsed)s "
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
        self.this_bar = progressbar.ProgressBar(
            widgets=widgets,
            max_value=self.total,
            fd=sys.stdout,
        ).start()

    def update(self, val):
        """Update the progress bar to an absolute value.

        Args:
            val: Absolute progress value to display.
        """
        self.this_bar.update(val)

    def increment(self, val=1):
        """Increment the progress bar by val steps.

        Args:
            val: Number of steps to advance the bar (default 1).
        """
        self.this_bar.increment(val)

    def finish(self):
        """Mark the progress bar as complete and finalize its display."""
        self.this_bar.finish()


def sort_dict(d: dict):
    """Uses insertion ordering to sort a dictionary by keys."""
    keys = []
    sorted_dict = {}
    for k in d.keys():
        keys.append(k)
    keys = sorted(keys)
    for k in keys:
        sorted_dict[k] = d[k]

    return sorted_dict


def diff_dicts(dict1, dict2):
    """Compare two dicts and return a dict of differences as value tuples.

    Each tuple holds the values from each dict in the order they were
    passed.  None is used for keys missing from one dict.  Returns {}
    if no differences are found.
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
    """Prompt the user with msg and return True for Y/y, False for N/n."""
    p = ""
    while p not in ["Y", "y", "N", "n"]:
        p = input(f"{msg} (Y/N)?:")
    if p in ["Y", "y"]:
        return True
    else:
        return False


def valid_timeframe(t, exit=True):
    """Return True if t is valid, otherwise raise or print an error."""
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
    """Return True if t is a valid trading hours specifier, else raise."""
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
    """Confirm the given timeframe (tf) and trading hours (th) are compatible.

    Usually we want to exit if incompatible as data cannot be trusted
    otherwise.
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
    """Return True if c is a valid event category, otherwise raise or print."""
    if c in EVENT_CATEGORIES:
        return True
    else:
        err_msg = f"{c} is not a valid event category in {EVENT_CATEGORIES}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def dt_as_dt(d):
    """Return a datetime from the given datetime, string, or None input.
    """
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
    """Return a string from the given datetime, string, or None input.
    """
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
    """Return a datetime.time object for the given %H:%M:%S string."""
    if time is None:
        return None
    return dt_as_dt(f"2000-01-01 {time}").time()


def dow_name(dow: int):
    """Return the weekday name for the given datetime.weekday() index.
    """
    names = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
             4: "Friday", 5: "Saturday", 6: "Sunday"}
    return names[dow]


def dt_to_epoch(d):
    """Return an epoch integer from a datetime or string."""
    if d is None:
        return None
    return int(dt_as_dt(d).timestamp())


def dt_from_epoch(d):
    """Return a datetime object from an epoch integer."""
    if d is None:
        return None
    return dt.fromtimestamp(d)


def timeframe_delta(timeframe: str):
    """Return a precalculated timedelta object for the timeframe given."""
    if timeframe in TIMEFRAME_DELTAS:
        return TIMEFRAME_DELTAS[timeframe]

    raise ValueError(f"timeframe: {timeframe} not supported")


def start_of_week_date(dt):
    """Return the Sunday that starts the week containing the given datetime.
    """
    dt = dt_as_dt(dt)
    if dt.weekday() == 6:
        week_dt = dt.date()
    else:
        week_dt = dt.date() - timedelta(days=dt.weekday() + 1)
    return week_dt


def dict_of_weeks(start_dt, end_dt, template):
    """Return a template dictionary with keys for each week in the timeframe.

    Keys are created for each week from start_dt to end_dt, with a value
    equal to the template passed in.  Generally used for aggregating weekly
    stats.  Sunday is the start of each week to match market behavior and
    cover extended hours trading strategies.

    template should be a dictionary with default values, for example::

        template = {
            "total_trades": 0,
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
    """Return the parent candle start datetime for the given dt and timeframe.

    May return the same as input.  Unlike next_candle_start(), this does
    not confirm market open and may not be able to answer in all cases.
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
    elif timeframe == "e1d":
        # e1d candles start at 18:00:00
        # Return the most recent 18:00:00 datetime
        if this_dt.hour >= 18:
            # At or after 6pm, return today at 18:00
            this_dt = this_dt.replace(
                hour=18, minute=0, second=0, microsecond=0)
        else:
            # Before 6pm, return yesterday at 18:00
            this_dt = (this_dt - timedelta(days=1)).replace(
                hour=18, minute=0, second=0, microsecond=0)
    elif timeframe == "e1w":
        # e1w candles start at Sunday 18:00:00
        # Return the most recent Sunday at 18:00:00
        if this_dt.weekday() == 6 and this_dt.hour >= 18:
            # Sunday at/after 6pm, return Sunday at 18:00
            this_dt = this_dt.replace(
                hour=18, minute=0, second=0, microsecond=0)
        else:
            # Find the most recent Sunday
            if this_dt.weekday() == 6:  # Already Sunday but before 6pm
                days_back = 7
            else:
                # Days since last Sunday (weekday 6)
                days_back = (this_dt.weekday() + 1) % 7
            this_dt = (this_dt - timedelta(days=days_back)).replace(
                hour=18, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"timeframe: {timeframe} not supported")

    return this_dt


def next_candle_start(dt,
                      trading_hours: str,
                      symbol,
                      timeframe: str = "1m",
                      events: list = None,
                      ):
    """Return the next valid candle start datetime during open market hours.

    symbol must be a Symbol-like object implementing market_is_open().
    """
    if isinstance(symbol, str):
        raise TypeError("symbol must be a Symbol object, not str")
    valid_trading_hours(trading_hours)
    check_tf_th_compatibility(tf=timeframe, th=trading_hours)
    next_dt = dt_as_dt(dt).replace(microsecond=0, second=0)
    min_delta = timedelta(minutes=1)
    # Check if symbol has context-based predicates for efficient lookups
    use_context = all([
        hasattr(symbol, "build_market_hours_context"),
        hasattr(symbol, "is_open_dt"),
    ])
    context = None

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
        elif timeframe == "e1d":
            # e1d candles start at 18:00:00
            # Return the next 18:00:00 after current datetime
            if next_dt.hour < 18:
                # Before 6pm, move to today at 18:00
                next_dt = next_dt.replace(
                    hour=18, minute=0, second=0, microsecond=0)
            else:
                # At or after 6pm, move to tomorrow at 18:00
                next_dt = (next_dt + timedelta(days=1)).replace(
                    hour=18, minute=0, second=0, microsecond=0)
        elif timeframe == "e1w":
            # e1w candles start at Sunday 18:00:00
            # Return the next Sunday 18:00:00 after current datetime
            if next_dt.weekday() == 6:  # Sunday
                if next_dt.hour < 18:
                    # Sunday before 6pm, move to Sunday 18:00
                    next_dt = next_dt.replace(
                        hour=18, minute=0, second=0, microsecond=0)
                else:
                    # Sunday at/after 6pm, move to next Sunday 18:00
                    next_dt = (next_dt + timedelta(days=7)).replace(
                        hour=18, minute=0, second=0, microsecond=0)
            else:
                # Not Sunday, find next Sunday
                days_until_sunday = (6 - next_dt.weekday()) % 7
                if days_until_sunday == 0:
                    days_until_sunday = 7
                next_dt = (next_dt + timedelta(
                    days=days_until_sunday)).replace(
                    hour=18, minute=0, second=0, microsecond=0)
        elif timeframe != "1m":
            raise ValueError(f"timeframe: {timeframe} not supported")
        if use_context:
            # Reuse context for 14-day window, rebuilding when moved beyond end
            if (context is None
                    or dt_to_epoch(next_dt) > context["end_epoch"]):
                context = symbol.build_market_hours_context(
                    trading_hours=trading_hours,
                    events=events,
                    start_dt=next_dt,
                    end_dt=next_dt + timedelta(days=14),
                )
            done = symbol.is_open_dt(target_dt=next_dt,
                                     context=context)
        else:
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
                              show_progress: bool = False,
                              ):
    """Return expected candle datetimes for a symbol in a datetime range."""
    if isinstance(symbol, str):
        raise TypeError("symbol must be a Symbol object, not str")
    if symbol.ticker == "ES":
        trading_hours = "rth" if timeframe == "r1h" else "eth"
    else:
        raise ValueError("Only ES is currently supported as symbol for now")

    # Only include closure events (event.category == "Closed")
    if events is None:
        events = []
    closed_events = [e for e in events if e.category == "Closed"]

    # Determine start and end boundaries to loop through
    result = []
    adder = timeframe_delta(timeframe)
    this = this_candle_start(dt=start_dt,
                             timeframe=timeframe,
                             )
    # Align to next candle start when requested start_dt is not a candle start
    if this != dt_as_dt(start_dt):
        this = next_candle_start(dt=start_dt,
                                 timeframe=timeframe,
                                 trading_hours=trading_hours,
                                 symbol=symbol,
                                 events=closed_events,
                                 )

    ender = dt_as_dt(end_dt)

    # Build one shared context for entire date range for efficient lookups
    context = None
    if this <= ender:
        context = symbol.build_market_hours_context(
            trading_hours=trading_hours,
            events=closed_events,
            start_dt=this,
            end_dt=ender,
        )

    if show_progress and this <= ender:
        total = ((ender - this) // adder) + 1
        pbar = ProgBar(total=total,
                       desc="Expected candle starts calculated")
    else:
        total = 0
        pbar = None

    # Loop through timeframe candles, check if any minute falls within
    # open market hours using shared context
    while this <= ender:
        if timeframe in TIMEFRAMES:
            # Check each minute in this candle bucket
            minute = dt_as_dt(this)
            bucket_end = minute + adder - timedelta(minutes=1)
            has_open_minute = False

            while minute <= bucket_end:
                if symbol.is_open_dt(target_dt=minute, context=context):
                    has_open_minute = True
                    break
                minute = minute + timedelta(minutes=1)

            if has_open_minute:
                result.append(this)
        else:
            raise ValueError(f"timeframe: {timeframe} not supported")
        if pbar is not None:
            pbar.increment()
        this = this + adder

    if pbar is not None:
        pbar.finish()

    return result


def rangify_candle_times(times: list,
                         timeframe: str,
                         ):
    """Aggregate a list of datetimes into a list of datetime ranges.

    Primarily intended to make human review sane on large sets of gap and
    unexpected candles during integrity checks.
    """
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
                      expected_dts: list = None,
                      ):
    """Return a summary of candle datetimes grouped by minute, hour, and date.

    Compares actual candle times against expected times for the given
    timeframe. When expected_dts is provided the expected minutes, hours,
    and times are derived dynamically from that list, making the check
    era-aware and eliminating static per-timeframe hardcoded lists.
    """
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
    if expected_dts is not None:
        exp_minutes = set()
        exp_hours = set()
        exp_times = set()
        for edt in expected_dts:
            d = dt_as_dt(edt)
            m = str(d.minute)
            if len(m) < 2:
                m = "0" + m
            exp_minutes.add(m)
            h = str(d.hour)
            if len(h) < 2:
                h = "0" + h
            exp_hours.add(h)
            exp_times.add(str(d.time()))
        summary_expected = {
            'minutes': sorted(list(exp_minutes)),
            'hours': sorted(list(exp_hours)),
            'times': sorted(list(exp_times)),
        }
    else:
        summary_expected = None

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
    },
    {
        "name": "2099_test_era",
        "start_date": date(2099, 1, 1),
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
