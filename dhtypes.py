"""Domain model classes for trading system.

This module defines core data types: Symbol, Candle, Chart, Trade, etc.
representing:

- Symbol: Market symbols (e.g., ES) with market hours and schedules
- Candle: OHLCV bars at various timeframes
  (1m, 5m, 15m, r1h, e1h, 1d, 1w)
- Day: Daily calendar wrapper around Candles
- Chart: Collection of candles with technical analysis capabilities
- Event: Market events (closures, announcements, etc.) that affect
  candle validity or reflect unusual market conditions
- Indicator: Base class for technical indicators (SMA, EMA, etc.)
- IndicatorDataPoint: Individual indicator calculation results
- Trade: Single trade execution with entry, exit, and P&L data
- TradeSeries: Collection of trades with aggregate statistics
- Backtest: Backtest results and performance metrics

ARCHITECTURE NOTE: This module imports only utility functions from dhcommon
(which has no external dependencies) and avoids importing from dhstore or
dhutil. Storage operations are delegated to dhstore.py while this module
focuses on data structure definitions.

The MARKET_ERAS configuration defines trading hours for ES futures across
different historical time periods.
"""
import datetime as dt
from datetime import timedelta
import sys
import json
from statistics import fmean
from copy import copy, deepcopy
import logging
from math import ceil, floor
import numpy as np
from .dhcommon import (
    dt_as_dt, dt_as_str, dt_as_time, dt_to_epoch, timeframe_delta,
    valid_timeframe, valid_trading_hours, log_say, this_candle_start,
    check_tf_th_compatibility, start_of_week_date, dict_of_weeks, bot)
CANDLE_TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', '1d', '1w']
BEGINNING_OF_TIME = "2008-01-01 00:00:00"

# Market Era Definitions
# Define different historical market structures with start dates and schedules.
# To add new eras: append a new dict to this list with start_date and schedule.
# Eras are ordered chronologically; the system finds the era by checking which
# start_date the target_dt is >= to (uses the latest matching start_date).
# Based on analysis of ES futures data presence patterns 2008-2026.
MARKET_ERAS = [
    {
        "name": "2008_thru_2012",
        "start_date": dt.date(2008, 1, 1),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(17, 29, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                # ETH close period: 17:30-17:59:59 with 15min break at 16:16
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
                # RTH: 9:30-16:00
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
        "start_date": dt.date(2012, 11, 17),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(17, 15, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                # ETH close period: 17:16-17:59:59 with 15min break at 16:15
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
    },
    {
        "name": "2015holidays_thru_2020",
        "start_date": dt.date(2015, 9, 19),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(16, 59, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                # ETH close period: 17:00-17:59:59 with 15min break at 16:15
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
    },
    {
        "name": "2021-01_thru_2021-06",
        "start_date": dt.date(2021, 1, 1),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(16, 59, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                # Nearly 24/5 trading with extended RTH
                # Daily close: 16:15-16:29 on weekdays, then 17:00-17:59:59
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
    },
    {
        "name": "2021-06_thru_present",
        "start_date": dt.date(2021, 6, 26),
        "times": {
            "eth_open": dt.time(18, 0, 0),
            "eth_close": dt.time(16, 59, 0),
            "rth_open": dt.time(9, 30, 0),
            "rth_close": dt.time(16, 0, 0),
        },
        "closed_hours": {
            "eth": {
                # Nearly 24/5 trading with extended RTH
                # Daily close: 17:00-17:59:59 (16:15-16:29 close removed)
                0: [{"close": "17:00:00", "open": "17:59:59"}],
                1: [{"close": "17:00:00", "open": "17:59:59"}],
                2: [{"close": "17:00:00", "open": "17:59:59"}],
                3: [{"close": "17:00:00", "open": "17:59:59"}],
                4: [{"close": "17:00:00", "open": "23:59:59"}],
                5: [{"close": "00:00:00", "open": "23:59:59"}],
                6: [{"close": "00:00:00", "open": "17:59:59"}]
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

log = logging.getLogger("dhtypes")
log.addHandler(logging.NullHandler())


def _dhstore(fn_name, *args, **kwargs):
    """Delegate to dhstore if loaded."""
    # Try package-qualified name first, then bare name for compatibility
    m = sys.modules.get('dhtrader.dhstore')
    if m is None:
        m = sys.modules.get('dhstore')
    if m is None:
        raise RuntimeError(
            f"dhstore not imported. Import dhstore before "
            f"calling {fn_name}()."
        )
    return getattr(m, fn_name)(*args, **kwargs)


def get_symbol_by_ticker(*args, **kwargs):
    """Delegate to dhstore.get_symbol_by_ticker."""
    return _dhstore('get_symbol_by_ticker', *args, **kwargs)


def get_candles(*args, **kwargs):
    """Delegate to dhstore.get_candles."""
    return _dhstore('get_candles', *args, **kwargs)


def get_events(*args, **kwargs):
    """Delegate to dhstore.get_events."""
    return _dhstore('get_events', *args, **kwargs)


def get_indicator_datapoints(*args, **kwargs):
    """Delegate to dhstore.get_indicator_datapoints."""
    return _dhstore(
        'get_indicator_datapoints', *args, **kwargs)


def store_indicator_datapoints(*args, **kwargs):
    """Delegate to dhstore.store_indicator_datapoints."""
    return _dhstore(
        'store_indicator_datapoints', *args, **kwargs)


def store_indicator(*args, **kwargs):
    """Delegate to dhstore.store_indicator."""
    return _dhstore('store_indicator', *args, **kwargs)


def get_trades_by_field(*args, **kwargs):
    """Delegate to dhstore.get_trades_by_field."""
    return _dhstore('get_trades_by_field', *args, **kwargs)


def get_tradeseries_by_field(*args, **kwargs):
    """Delegate to dhstore.get_tradeseries_by_field."""
    return _dhstore(
        'get_tradeseries_by_field', *args, **kwargs)


def delete_tradeseries(*args, **kwargs):
    """Delegate to dhstore.delete_tradeseries."""
    return _dhstore('delete_tradeseries', *args, **kwargs)


def delete_tradeseries_by_field(*args, **kwargs):
    """Delegate to dhstore.delete_tradeseries_by_field."""
    return _dhstore('delete_tradeseries_by_field', *args, **kwargs)


def delete_trades_by_field(*args, **kwargs):
    """Delegate to dhstore.delete_trades_by_field."""
    return _dhstore('delete_trades_by_field', *args, **kwargs)


def delete_trades(*args, **kwargs):
    """Delegate to dhstore.delete_trades."""
    return _dhstore('delete_trades', *args, **kwargs)


def delete_backtests(*args, **kwargs):
    """Delegate to dhstore.delete_backtests."""
    return _dhstore('delete_backtests', *args, **kwargs)


def delete_backtests_by_field(*args, **kwargs):
    """Delegate to dhstore.delete_backtests_by_field."""
    return _dhstore('delete_backtests_by_field', *args, **kwargs)


def review_candles(*args, **kwargs):
    """Delegate to dhstore.review_candles."""
    return _dhstore('review_candles', *args, **kwargs)


class Symbol():
    """Represents basic mechanics of a tradeable symbol a.k.a. ticker.

    This might be a specific stock or future, identified by ticker, name,
    leverage_ratio, and tick_size.
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
        """Return True if this symbol equals the other symbol."""
        return (self.ticker == other.ticker
                and self.name == other.name
                and self.leverage_ratio == other.leverage_ratio
                and self.tick_size == other.tick_size
                )

    def __ne__(self, other):
        """Return True if this symbol does not equal the other symbol."""
        return not self.__eq__(other)

    def to_json(self):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        w = deepcopy(self.__dict__)
        w["eth_open_time"] = str(w["eth_open_time"])
        w["eth_close_time"] = str(w["eth_close_time"])
        w["rth_open_time"] = str(w["rth_open_time"])
        w["rth_close_time"] = str(w["rth_close_time"])
        w["eth_week_open"]["time"] = str(w["eth_week_open"]["time"])
        w["eth_week_close"]["time"] = str(w["eth_week_close"]["time"])
        w["rth_week_open"]["time"] = str(w["rth_week_open"]["time"])
        w["rth_week_close"]["time"] = str(w["rth_week_close"]["time"])
        w.pop("_closed_hours_cache", None)
        return json.dumps(w)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json())

    def __str__(self):
        """Return string representation of this Symbol."""
        working = copy(self.__dict__)
        working.pop("_closed_hours_cache", None)
        return str(working)

    def __repr__(self):
        """Return string representation of this Symbol."""
        return str(self)

    def pretty(self):
        """Return a formatted, indented string representation of this object.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def set_times(self):
        """Sets times for known Symbol objects.

        These are the CURRENT era times (latest era in MARKET_ERAS). Use
        get_times_for_era() with dynamic era detection for historical
        times.
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
        """Returns the next tick available at or above the provided value."""
        return round(ceil(f / self.tick_size) * self.tick_size, 2)

    def get_next_tick_down(self, f: float):
        """Returns the next tick available at or below the provided value."""
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
        d = dt_as_dt(target_dt).date()

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
        """Build the closed hours dict for a given era and trading_hours type.

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
                    "close": dt_as_time(period["close"]),
                    "open": dt_as_time(period["open"])
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
        """Return True if target_dt falls within open market hours.

        Checks that no Events with category 'Closed' overlap.  Uses
        dynamic era detection to apply correct historical market hours.
        """
        # Set vars needed to evaluate
        d = dt_as_dt(target_dt)
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
            if events is None:
                events = []
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
        """Return previous or next market open/close for rth or eth hours.

        Optionally factors in events to skip ahead or back when a market
        closure event would impact the result.  The caller must determine
        event criteria and build the list rather than making assumptions.

        Typically called via wrapper methods below that provide better
        context and auto-include most needed parameters.

        Note: this only skips to the next standard open/close when an event
        overlaps, rather than evaluating event boundaries.  This should
        have minimal impact on backtest results over meaningful periods.
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
            this_date = dt_as_dt(target_dt).date()
            this_time = dt_as_dt(target_dt).time()
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
    """Represents a single OHLCV candlestick for a tradeable symbol.

    Derived size, direction, and wick attributes are computed automatically
    on creation.
    """

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
        # Precalculate datetime for calculating other attributes efficiently
        c_datetime_dt = dt_as_dt(c_datetime)

        # Passable attributes
        self.c_datetime = dt_as_str(c_datetime_dt)
        self.c_timeframe = c_timeframe
        valid_timeframe(self.c_timeframe)
        self.c_open = float(c_open)
        self.c_high = float(c_high)
        self.c_low = float(c_low)
        self.c_close = float(c_close)
        self.c_volume = int(c_volume)
        if isinstance(c_symbol, Symbol):
            self.c_symbol = c_symbol
        else:
            self.c_symbol = get_symbol_by_ticker(ticker=c_symbol)
        if c_tags is None:
            c_tags = []
        self.c_tags = c_tags
        if c_epoch is None:
            c_epoch = dt_to_epoch(c_datetime_dt)
        self.c_epoch = c_epoch
        if c_date is None:
            c_date = self.c_datetime[:10]
        self.c_date = c_date
        if c_time is None:
            c_time = self.c_datetime[11:19]
        self.c_time = c_time

        # Calculated attributes
        delta = timeframe_delta(self.c_timeframe)
        self.c_end_datetime = dt_as_str(c_datetime_dt + delta)
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
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        working = deepcopy(self.__dict__)
        working["c_symbol"] = working["c_symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json())

    def __str__(self):
        """Return string representation of this Candle."""
        return str(self.__dict__)

    def __repr__(self):
        """Return string representation of this Candle."""
        return str(self.__dict__)

    def pretty(self):
        """Return a formatted, indented string representation of this object.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def brief(self):
        """Return a single line summary string of this Candle's vitals."""
        if isinstance(self.c_symbol, str):
            ticker = self.c_symbol
        else:
            ticker = self.c_symbol.ticker
        return (f"{ticker} {self.c_timeframe} {self.c_datetime} | "
                f"O: {self.c_open} H: {self.c_high} L: {self.c_low} "
                f"C: {self.c_close} V: {self.c_volume}")

    def __eq__(self, other):
        """Return True if this candle equals the other candle."""
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
        """Return True if this candle does not equal the other candle."""
        return not self.__eq__(other)

    def contains_datetime(self, d):
        """Return True if the datetime provided occurs in this candle."""
        return (dt_as_dt(self.c_datetime) < dt_as_dt(d) and
                dt_as_dt(d) < dt_as_dt(self.c_end_datetime))

    def contains_price(self, p):
        """Return True if price falls within this candle's high/low range.
        """
        return self.c_low <= p <= self.c_high


class Chart():
    """Collection of Candles for a given symbol, timeframe, and date range.

    Supports both regular and extended trading hours.
    """

    def __init__(self,
                 c_timeframe: str,
                 c_trading_hours: str,
                 c_symbol,
                 c_start: str = None,
                 c_end: str = None,
                 c_candles: list = None,
                 autoload: bool = False,
                 ):
        if valid_timeframe(c_timeframe):
            self.c_timeframe = c_timeframe
        if valid_trading_hours(c_trading_hours):
            self.c_trading_hours = c_trading_hours
        if isinstance(c_symbol, str):
            self.c_symbol = get_symbol_by_ticker(ticker=c_symbol)
        else:
            self.c_symbol = c_symbol
        self.c_start = dt_as_str(c_start)
        self.c_end = dt_as_str(c_end)
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
        """Return True if this Chart equals the other Chart."""
        return (self.c_timeframe == other.c_timeframe
                and self.c_symbol == other.c_symbol
                and self.c_start == other.c_start
                and self.c_end == other.c_end
                and self.c_candles == other.c_candles
                )

    def __ne__(self, other):
        """Return True if this Chart does not equal the other Chart."""
        return not self.__eq__(other)

    def to_json(self,
                suppress_candles: bool = True,
                ):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
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
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json(suppress_candles=suppress_candles))

    def __str__(self):
        """Return string representation of this Chart."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return string representation of this Chart."""
        return str(self)

    def pretty(self,
               suppress_candles: bool = True,
               ):
        """Return a formatted, indented string representation of this object.
        """
        return json.dumps(self.to_clean_dict(
            suppress_candles=suppress_candles),
            indent=4,
            )

    def sort_candles(self):
        """Sort c_candles in ascending order by candle datetime."""
        self.c_candles.sort(key=lambda c: c.c_datetime)

    def add_candle(self, new_candle, sort=False):
        """Add a Candle to this Chart, optionally sorting after insertion."""
        if not isinstance(new_candle, Candle):
            raise TypeError(f"new_candle {type(new_candle)} must be a "
                            "<class dhtypes.Candle> object")
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
        """Load candles from central storage based on current attributes."""
        log.info(f"Loading candles for {self.c_symbol.ticker} "
                 f"{self.c_timeframe} ")
        cans = get_candles(
               start_epoch=dt_to_epoch(self.c_start),
               end_epoch=dt_to_epoch(self.c_end),
               timeframe=self.c_timeframe,
               symbol=self.c_symbol.ticker,
               )
        self.c_candles = []
        log.info("Getting events for market hours filtering...")
        events = get_events(symbol=self.c_symbol.ticker,
                            categories=["Closed"],
                            )
        log.info("Filtering candles for market hours and events...")
        for c in cans:
            if self.c_symbol.market_is_open(target_dt=c.c_datetime,
                                            trading_hours=self.c_trading_hours,
                                            events=events,
                                            ):
                self.c_candles.append(c)
        log.info("Sorting candles")
        self.sort_candles()
        log.info("Reviewing candles")
        self.review_candles()
        log.info("Finished loading candles into Chart")

    def review_candles(self):
        """Update candle summary attributes and return a summary dict."""
        if len(self.c_candles) > 0:
            self.candles_count = len(self.c_candles)
            self.earliest_candle = dt_as_str(self.c_candles[0].c_datetime)
            self.latest_candle = dt_as_str(self.c_candles[-1].c_datetime)
        else:
            self.candles_count = 0
            self.earliest_candle = None
            self.latest_candle = None

        return {"candles_count": self.candles_count,
                "earliest_candle": self.earliest_candle,
                "latest_candle": self.latest_candle,
                }

    def restrict_dates(self, new_start_dt: str, new_end_dt: str):
        """Reduce the Chart's date range and remove out-of-bounds Candles.
        """
        os = dt_as_dt(self.c_start)
        oe = dt_as_dt(self.c_end)
        ns = dt_as_dt(new_start_dt)
        ne = dt_as_dt(new_end_dt)
        ns_epoch = dt_to_epoch(new_start_dt)
        ne_epoch = dt_to_epoch(new_end_dt)
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
    """Classify notable time periods for correlation or exclusion in analysis.

    May include holiday closures, FOMC meetings, or any other substantial
    occurrences that have immediate market impact.  category is context
    dependent and user defined.  The combination of start_dt + category
    is used to prevent duplicate events in storage.
    """

    def __init__(self,
                 start_dt,
                 end_dt,
                 symbol,
                 category: str,
                 tags: list = None,
                 notes: str = "",
                 ):
        self.start_dt = dt_as_str(start_dt)
        self.end_dt = dt_as_str(end_dt)
        if isinstance(symbol, Symbol):
            self.symbol = symbol
        else:
            self.symbol = get_symbol_by_ticker(ticker=symbol)
        self.category = category
        self.tags = tags
        if tags is None:
            tags = []
        self.notes = notes
        self.start_epoch = dt_to_epoch(self.start_dt)
        self.end_epoch = dt_to_epoch(self.end_dt)

    def to_json(self):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        working = deepcopy(self.__dict__)
        working["symbol"] = working["symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json())

    def __str__(self):
        """Return string representation of this Event."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return string representation of this Event."""
        return str(self)

    def pretty(self):
        """Return a formatted, indented string representation of this object.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def contains_datetime(self,
                          dt,
                          ):
        """Return True if the given dt falls within this Event's timeframe.
        """
        start = dt_as_dt(self.start_dt)
        end = dt_as_dt(self.end_dt)
        this = dt_as_dt(dt)
        if start <= this <= end:
            return True
        else:
            return False


class Day():
    """Represent a single trading day with ETH and RTH OHLCV data and charts.

    Combines extended (ETH) and regular (RTH) trading hours along with
    charts at various timeframes.
    """

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
            raise TypeError(f"d_symbol {type(d_symbol)} must be a "
                            "<class dhtypes.Symbol> object")
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
                raise TypeError(f"c {type(c)} must be a "
                                "<class dhtypes.Chart> object")
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
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        return json.dumps(self.__dict__)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json())

    def __str__(self):
        """Return string representation of this Day."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return string representation of this Day."""
        return str(self)

    def pretty(self):
        """Return a formatted, indented string representation of this object.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def recalc_from_1m(self):
        """Recalculate OHLCV attributes from the 1m Chart candle data."""
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
        """Add a Chart to this Day if no chart with that timeframe exists."""
        if not isinstance(new_chart, Chart):
            raise TypeError(f"new_chart {type(new_chart)} must be a "
                            "<class dhtypes.Chart> object")
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
        """Replace existing Chart with the same timeframe with new_chart."""
        if not isinstance(new_chart, Chart):
            raise TypeError(f"new_chart {type(new_chart)} must be a "
                            "<class dhtypes.Chart> object")
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
        """Return the Chart matching the given timeframe, or None."""
        for c in self.d_charts:
            if c.c_timeframe == timeframe:
                return c
        return None


class IndicatorDataPoint():
    """Simple class to handle time series datapoints for indicators.

    I might swap this out for an even more generic TSDataPoint or similar
    if I find more uses for time series beyond this.
    """

    def __init__(self,
                 dt: str,
                 value: float,
                 ind_id: str,
                 epoch: int = None,
                 ):
        self.dt = dt_as_str(dt)
        self.value = value
        self.ind_id = ind_id
        if epoch is None:
            self.epoch = dt_to_epoch(dt)
        else:
            self.epoch = epoch

    def to_json(self):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        return json.dumps(self.__dict__)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json())

    def __str__(self):
        """Return a string representation of this datapoint."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return the canonical string representation of this datapoint."""
        return str(self)

    def pretty(self):
        """Return a formatted, indented string representation of this object.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def __eq__(self, other):
        """Return True if this datapoint equals the other datapoint."""
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
        """Return True if this datapoint does not equal other."""
        return not self.__eq__(other)


class Indicator():
    """Base class for technical indicators such as SMA and EMA.

    Names should be short and simple abbreviations used in tagging and
    storage, e.g. sma, hod, vwap.  This class is not intended to be used
    directly; use its subclasses which provide indicator type-specific
    logic.
    """

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
        self.name = name
        self.description = description
        if not valid_timeframe(timeframe):
            raise ValueError(f"{timeframe} not valid for timeframe")
        self.timeframe = timeframe
        if not valid_trading_hours(trading_hours):
            raise ValueError(f"{trading_hours} not valid for trading_hours")
        self.trading_hours = trading_hours
        if isinstance(symbol, str):
            self.symbol = get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.calc_version = calc_version
        self.calc_details = calc_details
        self.start_dt = start_dt
        self.end_dt = end_dt
        if self.end_dt is None:
            self.end_dt = dt_as_str(dt.datetime.now())
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
        """Return True if all indicator attributes and datapoints match."""
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
        """Return True if any indicator attribute or datapoint differs."""
        return not self.__eq__(other)

    def sub_eq(self, other):
        """Placeholder for subclasses to add extra __eq__ comparison logic.

        Any comparison of parameters should be done here as they are
        subclass specific.
        """
        return self.parameters == other.parameters

    def to_json(self,
                suppress_datapoints: bool = True,
                suppress_chart_candles: bool = True,
                ):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
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
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json(
                          suppress_datapoints=suppress_datapoints,
                          suppress_chart_candles=suppress_chart_candles,
                          ))

    def pretty(self,
               suppress_datapoints: bool = True,
               suppress_chart_candles: bool = True,
               ):
        """Return a formatted, indented string representation of this object.

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
        """Return a string representation of this indicator."""
        return str(self.get_info())

    def __repr__(self):
        """Return the canonical string representation of this indicator."""
        return str(self)

    def get_info(self,
                 pretty: bool = False,
                 ):
        """Provide a basic overview of this object."""
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
        """Load the underlying candle chart from central storage."""
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
        """Load datapoints from storage by ind_id, start_dt, and end_dt.
        """
        self.datapoints = get_indicator_datapoints(ind_id=self.ind_id,
                                                   earliest_dt=self.start_dt,
                                                   latest_dt=self.end_dt)
        self.sort_datapoints()

    def sort_datapoints(self):
        """Sort attached datapoints in chronological order."""
        if len(self.datapoints) == 0:
            return False
        self.datapoints.sort(key=lambda dp: dp.epoch)

    def datapoint_indexes_by_epoch(self):
        """Return a dict mapping each datapoint's epoch to its list index."""
        result = {}
        for i, dp in enumerate(self.datapoints):
            result[dp.epoch] = i
        return result

    def datapoint_indexes_by_dt(self):
        """Return a dict mapping each datapoint's dt to its list index."""
        result = {}
        for i, dp in enumerate(self.datapoints):
            result[dp.dt] = i
        return result

    def calculate(self):
        """This method will be specific to each type of indicator.

        It should accept only a list of Candles, sort it, and calculate new
        indicator datapoints from the candles.  Copy and modify this method
        as needed in subclasses.
        """
        if self.candle_chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhtypes.Chart> object")
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
        prev_day = dt_as_dt("1900-01-01 00:00:00").date()
        for c in self.candle_chart.c_candles:
            today = dt_as_dt(c.c_datetime).date()
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

    def get_datapoint(self,
                      dt,
                      offset: int = 0,
                      ):
        """Returns a single datapoint based on datetime provided.

        Because this is typically based on candle close, we often want the
        previous datapoint from the candle we are working through in a
        backtest so offset is allowed to go back or forward in the list by
        the provided value.  Wrapper methods assist with the most common
        previous and next requests.
        """
        can_dt = this_candle_start(dt=dt, timeframe=self.timeframe)
        index = next((i for i, dp in enumerate(self.datapoints)
                      if dt_as_dt(dp.dt) == dt_as_dt(can_dt)), None)
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
        """Wrapper for get_datapoint."""
        return self.get_datapoint(dt=dt, offset=1)

    def prev_datapoint(self,
                       dt,
                       ):
        """Wrapper for get_datapoint."""
        return self.get_datapoint(dt=dt, offset=-1)


class IndicatorSMA(Indicator):
    """Subclass of Indicator() specifically used for simple moving avg.

    Requires 'length' (int) in parameters. Optionally accepts 'method'
    (default: 'close') to specify which candle value to average.
    """

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
        """Calculate a simple moving average over time.

        Defaults to using the 'close' value of each candle.
        """
        if self.candle_chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhtypes.Chart> object")
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
                dp = IndicatorDataPoint(dt=dt_as_str(c.c_datetime),
                                        value=round(fmean(values), 2),
                                        ind_id=self.ind_id,
                                        )
                self.datapoints.append(dp)
            counter += 1

        return True


class IndicatorEMA(Indicator):
    """Subclass of Indicator() used for exponential moving averages.

    Requires a length, method (default: close) and smoothing (default: 2).
    The first 4*length calculated values will not be kept as the early part
    of the calculation has to start with a simple average and does not
    reach true EMA values until it has substantial history to effectively
    factor out the initial non-EMA baseline. Spot testing found roughly 4
    times the length is when accuracy reaches +/- $0.01 vs the same
    timestamp datapoints when the series is started at a much earlier point
    in time.
    """

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
        """Calculate an exponential simple moving average over time.

        Defaults to using the 'close' value of each candle and a smoothing
        factor of 2.
        """
        if self.candle_chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhtypes.Chart> object")
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
                    dp = IndicatorDataPoint(dt=dt_as_str(c.c_datetime),
                                            value=round(this_ema, 2),
                                            ind_id=self.ind_id,
                                            )
                    self.datapoints.append(dp)
                # Update vars for next candle
                prior_ema = this_ema
                counter += 1


class Trade():
    """Represents a single trade that could have been made.

    Attributes:
        open_dt (str): datetime trade was initiated
        open_epoch (int): epoch time of open_dt for sorting (autocalculated)
        direction (str): 'long' or 'short'
        timeframe (str): timeframe of the chart the Trade was created from
        trading_hours (str): regular trading hours (rth)
            or extended/globex (eth) of the underlying chart
        entry_price (float): price at which trade was initiated
        close_dt (str): datetime trade was closed
        created_dt (str): datetime this object was created
        high_price (float): highest price seen during trade
        low_price (float): lowest price seen during trade
        exit_price (float): price at which trade was closed
        stop_target (float): Target price at which trade should exit at a loss
        prof_target (float): Target price at which trade should exit at a gain
        stop_ticks (float): number of ticks to reach stop_target from entry,
            primarily used as trade rules identification during analysis
        prof_ticks (float): number of ticks to reach prof_target from entry,
            primarily used as trade rules identification during analysis
        offset_ticks (float): number of ticks away from target price that
            this trade made entry, primarily used as trade rules
            identification during analysis
        symbol (str): ticker being traded
        is_open (bool): True if trade has not yet been closed via .close()
        profitable (bool): True if trade made money
        name (str): Label for identifying groups of similar trades
            such as those run by the same backtester
        version (str): Version of trade (for future use)
        ts_id (str): unique id of associated TradeSeries this was created by
        bt_id (str): unique id of associated Backtest this was created by
    """

    def __init__(self,
                 open_dt: str,
                 direction: str,
                 timeframe: str,
                 trading_hours: str,
                 entry_price: float,
                 close_dt: str = None,
                 created_dt: str = None,
                 open_epoch: int = None,
                 high_price: float = None,
                 low_price: float = None,
                 exit_price: float = None,
                 stop_target: float = None,
                 prof_target: float = None,
                 stop_ticks: int = None,
                 prof_ticks: int = None,
                 offset_ticks: int = 0,
                 symbol="ES",
                 is_open: bool = True,
                 profitable: bool = None,
                 name: str = None,
                 version: str = "1.0.0",
                 ts_id: str = None,
                 bt_id: str = None,
                 tags: list = None,
                 ):
        # Passable attributes
        self.open_dt = open_dt
        self.close_dt = close_dt
        if created_dt is None:
            self.created_dt = dt_as_str(dt.datetime.now())
        else:
            self.created_dt = created_dt
        if direction in ['long', 'short']:
            self.direction = direction
        else:
            raise ValueError(f"invalid value for direction of {direction} "
                             "received, must be in ['long', 'short'] only."
                             )
        if valid_timeframe(timeframe):
            self.timeframe = timeframe
        if valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        self.entry_price = entry_price
        self.stop_target = stop_target
        self.prof_target = prof_target
        if high_price is None:
            self.high_price = entry_price
        else:
            self.high_price = high_price
        if low_price is None:
            self.low_price = entry_price
        else:
            self.low_price = low_price
        self.exit_price = exit_price
        self.stop_ticks = stop_ticks
        self.prof_ticks = prof_ticks
        self.offset_ticks = offset_ticks
        if isinstance(symbol, str):
            self.symbol = get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.is_open = is_open
        self.profitable = profitable
        self.name = name
        self.version = version
        self.ts_id = ts_id
        self.bt_id = bt_id
        if tags is None:
            self.tags = []
        else:
            self.tags = deepcopy(tags)

        # Calculated attributes
        if self.direction == "long":
            self.flipper = 1
        elif self.direction == "short":
            self.flipper = -1
        else:
            self.flipper = 0  # If this happens there's a bug somewhere
        self.open_epoch = dt_to_epoch(self.open_dt)
        self.open_date = str(dt_as_dt(self.open_dt).date())
        self.open_time = str(dt_as_dt(self.open_dt).time())
        if self.close_dt is not None:
            self.close_date = str(dt_as_dt(self.close_dt).date())
            self.close_time = str(dt_as_dt(self.close_dt).time())
            self.close_time = self.close_time.split(".")[0]
        else:
            self.close_date = None
            self.close_time = None
        # Mark Trades that open in the first minute of their timeframe bar
        # These can be difficult to trade quickly against changing indicators
        if self.timeframe == "5m":
            start_mins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        elif self.timeframe == "15m":
            start_mins = [0, 15, 30, 45]
        elif self.timeframe == "e1h":
            start_mins = [0]
        elif self.timeframe == "r1h":
            start_mins = [30]
        else:
            start_mins = None
            self.first_min_open = False
        if start_mins is not None:
            if dt_as_dt(self.open_dt).minute in start_mins:
                self.first_min_open = True
            else:
                self.first_min_open = False
        # Calc or confirm ticks and targets to prevent later innaccuracies
        if self.prof_ticks is None:
            if self.prof_target is None:
                # If neither provided we can't continue
                raise ValueError("Must provide either prof_ticks or "
                                 "prof_target (or both).  Neither was passed")
            else:
                # Need to calculate prof_ticks
                self.prof_ticks = ((self.prof_target - self.entry_price)
                                   * self.flipper
                                   / self.symbol.tick_size)
        else:
            if self.prof_target is None:
                # Need to calculate prof_target
                self.prof_target = self.entry_price + ((self.prof_ticks
                                                       * self.flipper)
                                                       * self.symbol.tick_size)
            else:
                # Both provided, make sure they math out correctly
                pt = self.entry_price + ((self.prof_ticks
                                         * self.flipper)
                                         * self.symbol.tick_size)
                if not pt == self.prof_target:
                    msg = (f"Provided prof_target does not match prof_ticks "
                           "calculation against entry_price.  These numbers "
                           "cannot be trusted for later calculations.  "
                           f"entry_price={self.entry_price} "
                           f"prof_ticks={self.prof_ticks} "
                           f"direction={self.direction} "
                           f"should get prof_target of {pt} but we got "
                           f"prof_target={self.prof_target} instead."
                           )
                    raise ValueError(msg)
        if self.stop_ticks is None:
            if self.stop_target is None:
                # If neither provided we can't continue
                raise ValueError("Must provide either stop_ticks or "
                                 "stop_target (or both).  Neither was passed")
            else:
                # Need to calculate stop_ticks
                self.stop_ticks = ((self.entry_price - self.stop_target)
                                   * self.flipper
                                   / self.symbol.tick_size)
        else:
            if self.stop_target is None:
                # Need to calculate stop_target
                self.stop_target = self.entry_price - ((self.stop_ticks
                                                       * self.flipper)
                                                       * self.symbol.tick_size)
            else:
                # Both provided, make sure they math out correctly
                st = self.entry_price - ((self.stop_ticks
                                         * self.flipper)
                                         * self.symbol.tick_size)
                if not st == self.stop_target:
                    msg = (f"Provided stop_target does not match stop_ticks "
                           "calculation against entry_price.  These numbers "
                           "cannot be trusted for later calculations.  "
                           f"entry_price={self.entry_price} "
                           f"stop_ticks={self.stop_ticks} "
                           f"direction={self.direction} "
                           f"should get stop_target of {st} but we got "
                           f"stop_target={self.stop_target} instead."
                           )
                    raise ValueError(msg)
        # Make sure ticks end up integers, no decimals allowed
        if self.prof_ticks == int(self.prof_ticks):
            self.prof_ticks = int(self.prof_ticks)
        else:
            raise ValueError("prof_ticks must be an integer but we got "
                             f"{self.prof_ticks}")
        if self.stop_ticks == int(self.stop_ticks):
            self.stop_ticks = int(self.stop_ticks)
        else:
            raise ValueError("stop_ticks must be an integer but we got "
                             f"{self.stop_ticks}")

        # If closing attributes were passed, run close() to ensure all
        # related attributes that may not have been passed in are finalized.
        if self.exit_price is not None:
            self.close(price=self.exit_price,
                       dt=self.close_dt,
                       )

    def __str__(self):
        """Return the string representation of this Trade."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return the repr string of this Trade."""
        return str(self)

    def __eq__(self, other):
        """Return True if all Trade attributes are equal."""
        return (self.open_dt == other.open_dt
                and self.timeframe == other.timeframe
                and self.trading_hours == other.trading_hours
                and self.direction == other.direction
                and self.entry_price == other.entry_price
                and self.high_price == other.high_price
                and self.low_price == other.low_price
                and self.stop_target == other.stop_target
                and self.prof_target == other.prof_target
                and self.close_dt == other.close_dt
                and self.created_dt == other.created_dt
                and self.open_epoch == other.open_epoch
                and self.exit_price == other.exit_price
                and self.stop_ticks == other.stop_ticks
                and self.prof_ticks == other.prof_ticks
                and self.offset_ticks == other.offset_ticks
                and self.symbol == other.symbol
                and self.is_open == other.is_open
                and self.profitable == other.profitable
                and self.name == other.name
                and self.version == other.version
                and self.ts_id == other.ts_id
                and self.bt_id == other.bt_id
                )

    def __ne__(self, other):
        """Return True if any Trade attribute differs."""
        return not self.__eq__(other)

    def to_json(self):
        """Return a JSON dict of this Trade with datetimes as strings.
        """
        # Make sure dates are strings not datetimes
        working = deepcopy(self.__dict__)
        if self.open_dt is not None:
            working["open_dt"] = dt_as_str(self.open_dt)
        if self.close_dt is not None:
            working["close_dt"] = dt_as_str(self.close_dt)
        if self.created_dt is not None:
            working["created_dt"] = dt_as_str(self.created_dt)
        # Change symbol to string of ticker
        working["symbol"] = working["symbol"].ticker

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
        return json.dumps(self.to_clean_dict(), indent=4)

    def brief(self):
        """Return a single line summary string of this Trade's vitals."""
        return (f"{self.open_dt} - {self.close_dt} | "
                f"{dt_as_dt(self.open_dt).strftime('%A')} | "
                f"{self.direction} | "
                f"entry={self.entry_price} | exit={self.exit_price} | "
                f"profitable={self.profitable}")

    def parent_bar_dt(self):
        """Return the opening datetime of the timeframe-specific parent bar.

        The parent bar is the bar in the configured timeframe within which
        this Trade opened.
        """
        return this_candle_start(self.open_dt,
                                 timeframe=self.timeframe)

    def parent_bar_secs(self):
        """Return seconds elapsed between the parent bar open and Trade open.

        The parent bar is the timeframe-specific bar within which this
        Trade opened.
        """
        start = dt_to_epoch(self.parent_bar_dt())
        return self.open_epoch - start

    def closed_intraday(self):
        """Return True if this Trade closes on the same day it was opened.
        """
        # Return False if the trade is still open
        if self.is_open:
            return False
        else:
            next_close = self.symbol.get_next_close(
                            target_dt=self.open_dt,
                            trading_hours=self.trading_hours)
            if dt_as_dt(self.close_dt) <= next_close:
                return True
            else:
                return False

    def candle_update(self,
                      candle,
                      ):
        """Update an open Trade with a new candle, checking for close triggers.

        Updates low and high price attributes and checks for trade closing
        triggers.
        """
        if not self.is_open:
            raise Exception("Cannot run update() on a closed Trade, this "
                            "would break reality.")
        if not isinstance(candle, Candle):
            raise TypeError(
                f"candle {candle} must be a dhtypes.Candle object, "
                f"we got a {type(candle)} instead"
            )
        # Set short var names for calcs in this method
        tick = self.symbol.tick_size
        close_price = None
        # Calculate updates based on direction
        if self.direction == "long":
            # Close on stop to be conservative even if profit_target hit
            if candle.c_low <= self.stop_target:
                close_price = self.stop_target
                self.low_price = min(self.low_price, self.stop_target)
            else:
                self.low_price = min(self.low_price, candle.c_low)
            # Now if we passed the profit target we can close at a gain
            # Exact tag doesn't guarantee fill so we must see a tick past
            if dt_as_dt(self.open_dt) == dt_as_dt(candle.c_datetime):
                # if we're still in the entry minute, we need to compare the
                # closing price to be sure it tagged profit after the entry
                if candle.c_close >= (self.prof_target + tick):
                    if close_price is None:
                        close_price = self.prof_target
                    self.high_price = max(self.high_price, self.prof_target)
                else:
                    self.high_price = max(self.high_price, candle.c_close)
            else:
                # Otherwise we compare against the candle high
                if candle.c_high >= (self.prof_target + tick):
                    if close_price is None:
                        close_price = self.prof_target
                    # Even if we closed on a stop, assume the worst case that
                    # we got to profit_target but did not get filled before
                    # falling to stop_target.  This has the most conservative
                    # /worst case effect on drawdown impacts.
                    self.high_price = max(self.high_price, self.prof_target)
                else:
                    self.high_price = max(self.high_price, candle.c_high)
        elif self.direction == "short":
            # Close on stop to be conservative even if profit_target hit
            if candle.c_high >= self.stop_target:
                close_price = self.stop_target
                self.high_price = max(self.high_price, self.stop_target)
            else:
                self.high_price = max(self.high_price, candle.c_high)
            # Now if we passed the profit target we can close at a gain
            # Exact tag doesn't guarantee fill so we must see a tick past
            if dt_as_dt(self.open_dt) == dt_as_dt(candle.c_datetime):
                # if we're still in the entry minute, we need to compare the
                # closing price to be sure it tagged profit after the entry
                if candle.c_close <= (self.prof_target - tick):
                    if close_price is None:
                        close_price = self.prof_target
                    self.low_price = min(self.low_price, self.prof_target)
                else:
                    self.low_price = min(self.low_price, candle.c_close)
            else:
                # Otherwise we compare against the candle low
                if candle.c_low <= (self.prof_target - tick):
                    if close_price is None:
                        close_price = self.prof_target
                    # Even if we closed on a stop_target, assume worst case
                    # that we got to profit_target but did not get filled
                    # before falling to stop_target.  This has the most
                    # conservative/worst case effect on drawdown impacts.
                    self.low_price = min(self.low_price, self.prof_target)
                else:
                    self.low_price = min(self.low_price, candle.c_low)

        # Close the trade if we hit a target
        if close_price is not None:
            self.close(close_price, candle.c_datetime)
            return {"closed": True}
        else:
            return {"closed": False}

    def drawdown_impact(self,
                        drawdown_open: float,
                        drawdown_limit: float,
                        contracts: int,
                        contract_value: float,
                        contract_fee: float
                        ):
        """Calculate drawdown range and ending values for the series inputs.

        Primarily used by TradeSeries when checking for drawdown liquidations.
        """
        # Until I find a reasonable need, assume we don't care about drawdowns
        # until the trade is closed.  Returning results could cause mistakes.
        if self.is_open:
            return None
        fees = contracts * contract_fee
        # Determine max gain and loss prices seen during trade
        if self.direction == "long":
            max_gain = self.high_price - self.entry_price
            max_loss = self.entry_price - self.low_price
        elif self.direction == "short":
            max_gain = self.entry_price - self.low_price
            max_loss = self.high_price - self.entry_price
        # Use these to calculate highest and lowest drawdown distances seen
        cmult = contracts * contract_value
        drawdown_high = drawdown_open + (max_gain * cmult)
        drawdown_low = drawdown_open - (max_loss * cmult)
        if drawdown_high > drawdown_limit:
            drawdown_trail_increase = drawdown_high - drawdown_limit
        else:
            drawdown_trail_increase = 0
        # Calculate the closing drawdown level i.e. where it will be after
        # the trade is finished including any adjustment for trailing increase
        drawdown_close = (((self.exit_price - self.entry_price)
                          * self.flipper
                          * cmult)
                          + drawdown_open
                          - drawdown_trail_increase
                          - fees)
        # Closing drawdown cannot exceed drawdown limit on account
        drawdown_close = min(drawdown_close, drawdown_limit)
        # Round results because float math anomalies create trailing decimals
        drawdown_open = round(drawdown_open, 2)
        drawdown_close = round(drawdown_close, 2)
        drawdown_high = round(drawdown_high, 2)
        drawdown_low = round(drawdown_low, 2)
        drawdown_trail_increase = round(drawdown_trail_increase, 2)

        return {"drawdown_open": drawdown_open,
                "drawdown_close": drawdown_close,
                "drawdown_trail_increase": drawdown_trail_increase,
                "drawdown_high": drawdown_high,
                "drawdown_low": drawdown_low,
                }

    def balance_impact(self,
                       balance_open: float,
                       contracts: int,
                       contract_value: float,
                       contract_fee: float,
                       ):
        """Calculate ending balance and gain/loss for series-specific inputs.

        Primarily used by TradeSeries when evaluating ending gains and
        balances.
        """
        # Cannot return balance impact until trade is closed
        if self.is_open:
            return None
        fees = contracts * contract_fee
        # Determine max gain and loss prices seen during trade
        if self.direction == "long":
            max_gain = self.high_price - self.entry_price
            max_loss = self.entry_price - self.low_price
        elif self.direction == "short":
            max_gain = self.entry_price - self.low_price
            max_loss = self.high_price - self.entry_price
        # Use these to calculate highest and lowest balances seen
        cmult = contracts * contract_value
        balance_high = balance_open + (max_gain * cmult) - fees
        balance_low = balance_open - (max_loss * cmult) - fees
        # Calculate gain/loss of the trade
        gain_loss = (((self.exit_price - self.entry_price)
                     * contracts * contract_value * self.flipper)
                     - fees)
        # Closing balance is just the difference from opening balance
        balance_close = round((balance_open + gain_loss), 2)
        # Round results because float math anomalies create trailing decimals
        balance_close = round(balance_close, 2)
        balance_high = round(balance_high, 2)
        balance_low = round(balance_low, 2)
        gain_loss = round(gain_loss, 2)

        return {"balance_open": balance_open,
                "balance_close": balance_close,
                "balance_high": balance_high,
                "balance_low": balance_low,
                "gain_loss": gain_loss,
                }

    def close(self,
              price: float,
              dt,
              ):
        """Close the trade at the given price, finalizing related attributes.
        """
        self.is_open = False
        self.close_dt = dt_as_str(dt)
        self.close_date = str(dt_as_dt(self.close_dt).date())
        self.close_time = str(dt_as_dt(self.close_dt).time()).split(".")[0]
        self.exit_price = price
        if (self.exit_price - self.entry_price) * self.flipper > 0:
            self.profitable = True
        else:
            self.profitable = False

    def gain_loss(self,
                  contracts: int = 1):
        """Return gross gain/loss for a given number of contracts before fees.
        """
        if self.is_open:
            return None
        return ((self.exit_price - self.entry_price) * self.flipper
                * contracts * self.symbol.leverage_ratio)

    def duration(self):
        """Return a timedelta for this Trade's duration (open_dt to close_dt).
        """
        if self.is_open:
            return None
        else:
            return (dt_to_epoch(self.close_dt)
                    - dt_to_epoch(self.open_dt))


class TradeSeries():
    """Represents a series of trades presumably following the same rules.

    Attributes:
        start_dt (str or datetime): Beginning of time period evaluated which
            may be earlier than the first trade datetime
        end_dt (str or datetime): End of time period evaluated
        timeframe (str): timeframe of underlying chart trades were evaluated
            from
        trading_hours (str): trading_hours of underlying chart trades were
            evaluated from
        symbol (str): The symbol or "ticker" being evaluated
        name (str): Human friendly label representing this object
        params_str (str): Represents Backtest or trade specific parameters
            to be passed in by creating objects and functions.  This is
            used to generate a unique ts_id in backtesting strategies so that
            multiple similar and related TradeSeries can be differentiated,
            stored, and retrieved effectively where needed even if created
            simultaneously with the same epoch value.
        ts_id (str): unique ID used for storage and analysis purposes,
            can be used to link related Trade() and Backtest() objects
        bt_id (str): bt_id of the Backtest() object that created this if any
        trades (list): list of trades in the series
    """

    def __init__(self,
                 start_dt,
                 end_dt,
                 timeframe: str,
                 trading_hours: str,
                 symbol="ES",
                 name: str = "",
                 params_str: str = "",
                 ts_id: str = None,
                 bt_id: str = None,
                 trades: list = None,
                 tags: list = None,
                 ):
        self.start_dt = dt_as_str(start_dt)
        self.end_dt = dt_as_str(end_dt)
        if valid_timeframe(timeframe):
            self.timeframe = timeframe
        if valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        if isinstance(symbol, str):
            self.symbol = get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.name = name
        self.params_str = params_str
        if ts_id is None:
            self.ts_id = "_".join([self.name,
                                   self.params_str,
                                   ])
        else:
            self.ts_id = ts_id
        self.bt_id = bt_id
        if trades is None:
            self.trades = []
        else:
            self.trades = trades.copy()
            for t in self.trades:
                t.ts_id = self.ts_id
                t.bt_id = self.bt_id
        if tags is None:
            self.tags = []
        else:
            self.tags = deepcopy(tags)

    def __eq__(self, other):
        """Return True if all TradeSeries attributes are equal."""
        return (self.start_dt == other.start_dt
                and self.end_dt == other.end_dt
                and self.timeframe == other.timeframe
                and self.symbol == other.symbol
                and self.name == other.name
                and self.params_str == other.params_str
                and self.ts_id == other.ts_id
                and self.bt_id == other.bt_id
                and self.trades == other.trades
                )

    def __ne__(self, other):
        """Return True if any TradeSeries attribute differs."""
        return not self.__eq__(other)

    def to_json(self,
                suppress_trades: bool = True,
                ):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        working = deepcopy(self.__dict__)
        clean_trades = []
        if suppress_trades:
            num = len(self.trades)
            clean_trades = [f"{num} Trades suppressed for output sanity"]
        else:
            for t in self.trades:
                clean_trades.append(t.to_clean_dict())
        working["trades"] = clean_trades
        working["symbol"] = working["symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self,
                      suppress_trades: bool = True,
                      ):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json(suppress_trades=suppress_trades))

    def __str__(self):
        """Return the string representation of this TradeSeries."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return the repr string of this TradeSeries."""
        return str(self)

    def pretty(self,
               suppress_trades: bool = True,
               ):
        """Return a formatted, indented string representation of this object.

        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(suppress_trades=suppress_trades),
                          indent=4,
                          )

    def trades_brief(self):
        """Return one-line summary strings for all Trades in this TradeSeries.
        """
        result = []
        for t in self.trades:
            result.append(t.brief())

        return result

    def list_trades(self):
        """Prints the results of trades_brief()"""
        for t in self.trades_brief():
            print(t)

    def update_bt_id(self, bt_id):
        """Update bt_id on this and any attached Trade objects.

        Typicaly called by a Backtest when adding this object to it's list.
        """
        self.bt_id = bt_id
        for t in self.trades:
            t.bt_id = bt_id

    def load_trades(self):
        """Clear and reload all Trades matching this ts_id from storage.
        """
        self.trades = get_trades_by_field(field="ts_id",
                                          value=self.ts_id)
        self.sort_trades()

    def delete_from_storage(self,
                            include_trades: bool = True,
                            ):
        """Delete this object from central storage if it exists by ts_id.

        Optionally (default=True) also remove all Trade objects matching
        this object's ts_id.
        """
        result = {"tradeseries": None, "trades": []}

        result["tradeseries"] = delete_tradeseries_by_field(
                symbol=self.symbol,
                field="ts_id",
                value=self.ts_id,
                )
        if include_trades:
            result["trades"] = delete_trades_by_field(symbol=self.symbol,
                                                      field="ts_id",
                                                      value=self.ts_id,
                                                      )

        return result

    def add_trade(self,
                  trade,
                  ):
        """Append a Trade to this TradeSeries, setting its ts_id."""
        # Associate this Trade with this Backtest
        trade.ts_id = self.ts_id
        self.trades.append(trade)

    def sort_trades(self):
        """Sort attached Trades in ascending open_epoch order."""
        self.trades.sort(key=lambda t: t.open_epoch)

    def get_trade_by_open_dt(self, dt):
        """Return the first Trade matching the given open_dt, or None.
        """
        for t in self.trades:
            if dt_as_dt(t.open_dt) == dt_as_dt(dt):
                return t

        return None

    def count_trades(self):
        """Return the number of Trades currently attached."""
        if self.trades is None:
            return 0
        else:
            return len(self.trades)

    def restrict_dates(self,
                       new_start_dt: str,
                       new_end_dt: str,
                       update_storage: bool = False,
                       ):
        """Reduce the TradeSeries date range and remove out-of-bounds Trades.
        """
        os = dt_as_dt(self.start_dt)
        oe = dt_as_dt(self.end_dt)
        ns = dt_as_dt(new_start_dt)
        ne = dt_as_dt(new_end_dt)
        ns_epoch = dt_to_epoch(new_start_dt)
        ne_epoch = dt_to_epoch(new_end_dt)
        # Ensure new dates don't expand the daterange, they should only reduce
        # or keep unchanged
        if ns < os:
            raise ValueError(f"new_start_dt {new_start_dt} cannot be earlier "
                             f"than the current self.start_dt {self.start_dt}")
        if ne > oe:
            raise ValueError(f"new_end_dt {new_end_dt} cannot be later "
                             f"than the current self.end_dt {self.end_dt}")
        # Update TradeSeries dates
        self.start_dt = new_start_dt
        self.end_dt = new_end_dt
        if update_storage:
            # Store (update) the TradeSeries
            _dhstore('store_tradeseries', series=[self])
            # Remove all trades from storage that are no longer in bounds
            remove_trades = [t for t in self.trades
                             if (t.open_epoch < ns_epoch
                                 or t.open_epoch > ne_epoch)
                             ]
            delete_trades(remove_trades)
        # Remove trades from the current object's list as well by rebuilding
        # with only trades inside the new datetime range
        self.trades = [t for t in self.trades
                       if ns_epoch <= t.open_epoch <= ne_epoch
                       ]

    def balance_impact(self,
                       balance_open: float,
                       contracts: int,
                       contract_value: float,
                       contract_fee: float,
                       include_first_min: bool = True,
                       ):
        """Calculate running account balance changes across all current Trades.

        Returns high, low, and ending balance starting from balance_open.
        """
        # Make sure trades are in order or results can't be trusted
        self.sort_trades()
        # All vars start at the opening balance provided
        balance_close = balance_open
        balance_high = balance_open
        balance_low = balance_open
        liquidated = False
        gain_loss = 0.0
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                r = t.balance_impact(balance_open=balance_close,
                                     contracts=contracts,
                                     contract_value=contract_value,
                                     contract_fee=contract_fee,
                                     )
                balance_high = max(balance_high, r["balance_high"])
                balance_low = min(balance_low, r["balance_low"])
                balance_close = r["balance_close"]
                gain_loss += r["gain_loss"]
        if balance_low <= 0:
            liquidated = True

        return {"balance_open": balance_open,
                "balance_close": balance_close,
                "balance_high": balance_high,
                "balance_low": balance_low,
                "liquidated": liquidated,
                "gain_loss": round(gain_loss, 2),
                }

    def drawdown_impact(self,
                        drawdown_open: float,
                        drawdown_limit: float,
                        contracts: int,
                        contract_value: float,
                        contract_fee: float,
                        include_first_min: bool = True,
                        ):
        """Calculate running drawdown changes across all current Trades.

        Returns high, low, and ending drawdown starting from drawdown_open.
        """
        # Make sure trades are in order or results can't be trusted
        self.sort_trades()
        drawdown_close = drawdown_open
        drawdown_high = drawdown_open
        drawdown_low = drawdown_open
        liquidated = False
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                r = t.drawdown_impact(drawdown_open=drawdown_close,
                                      drawdown_limit=drawdown_limit,
                                      contracts=contracts,
                                      contract_value=contract_value,
                                      contract_fee=contract_fee,
                                      )
                drawdown_high = max(drawdown_high, r["drawdown_high"])
                drawdown_low = min(drawdown_low, r["drawdown_low"])
                drawdown_close = r["drawdown_close"]
        if drawdown_low <= 0:
            liquidated = True

        return {"drawdown_open": drawdown_open,
                "drawdown_close": drawdown_close,
                "drawdown_high": drawdown_high,
                "drawdown_low": drawdown_low,
                "liquidated": liquidated,
                }

    def stats(self, include_first_min=True):
        """Return useful statistics calculated from the attached Trades."""
        sequence = ""
        total_trades = 0
        profits = 0
        losses = 0
        days_traded = set()
        ticks = set()
        rr = {"max": None, "min": None, "total_risk": 0, "total_reward": 0}
        err = {"total_risk": 0, "total_reward": 0}
        durations = []
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                total_trades += 1
                durations.append(t.duration())
                # Risk reward calcs
                ticks.add((("stop", t.stop_ticks),
                          ("prof", t.prof_ticks),
                          ("offset", t.offset_ticks)))
                this_rr = round(t.stop_ticks/t.prof_ticks, 2)
                if rr["max"] is None:
                    rr["max"] = this_rr
                else:
                    rr["max"] = max(rr["max"], this_rr)
                if rr["min"] is None:
                    rr["min"] = this_rr
                else:
                    rr["min"] = min(rr["min"], this_rr)
                rr["total_risk"] += t.stop_ticks
                rr["total_reward"] += t.prof_ticks
                # Add date to days_traded set
                days_traded.add(dt_as_dt(t.open_dt).date())
                # Update profitability
                if t.profitable:
                    profits += 1
                    sequence = "".join([sequence, "g"])
                    err["total_reward"] += t.gain_loss()
                else:
                    losses += 1
                    sequence = "".join([sequence, "L"])
                    err["total_risk"] -= t.gain_loss()
        durations.sort()
        if total_trades > 0:
            success_percent = round(profits/total_trades, 4)*100
            trading_days = len(days_traded)
            # total_days is fuzzy because start_dt and/or end_dt may be at a
            # day boundary and either may or may not have valid trading hours.
            # There are also edge cases around eth vs rth hours.  For
            # simplicity, we assume both dates have valid hours and count them
            # both from midnight to midnight using .date(), then add 1 to
            # include the end date as a full day.  This may overstate
            # total_days by up to 2 days in some cases which is typically not
            # statistically meaningful for analysis on long time frames.
            total_days = (dt_as_dt(self.end_dt).date()
                          - dt_as_dt(self.start_dt).date()).days + 1
            total_weeks = round(total_days/7, 2)
            trades_per_day = round(total_trades/total_days, 2)
            trades_per_trading_day = round(total_trades/trading_days, 2)
            trades_per_week = round(total_trades/total_weeks, 2)
            duration_sec_p20 = round(np.percentile(durations, 20))
            duration_sec_median = round(np.median(durations))
            duration_sec_p80 = round(np.percentile(durations, 80))
        else:
            success_percent = 0
            trading_days = 0
            total_days = 0
            total_weeks = 0
            trades_per_day = 0
            trades_per_trading_day = 0
            trades_per_week = 0
            duration_sec_p20 = 0
            duration_sec_median = 0
            duration_sec_p80 = 0

        if rr["total_reward"] > 0:
            risk_reward = round(rr["total_risk"] / rr["total_reward"], 2)
        else:
            risk_reward = None
        min_risk_reward = rr["min"]
        max_risk_reward = rr["max"]
        if profits == 0:
            avg_gain_per_con = None
        else:
            avg_gain_per_con = err["total_reward"] / profits
        if losses == 0:
            avg_loss_per_con = None
        else:
            avg_loss_per_con = err["total_risk"] / losses
        if avg_gain_per_con is None or avg_loss_per_con is None:
            eff_risk_reward = None
        else:
            eff_risk_reward = round(avg_loss_per_con / avg_gain_per_con, 2)
        if avg_gain_per_con is not None:
            avg_gain_per_con = round(avg_gain_per_con, 2)
        if avg_loss_per_con is not None:
            avg_loss_per_con = round(avg_loss_per_con, 2)
        # Convert ticks to a sorted list of dictionaries to aid unittests
        # and json conversions
        ticks_list = sorted([dict(x) for x in list(ticks)],
                            key=lambda y: (y['stop'], y['prof'], y['offset']))

        return {"gl_sequence": sequence,
                "profitable_trades": profits,
                "avg_gain_per_con": avg_gain_per_con,
                "losing_trades": losses,
                "avg_loss_per_con": avg_loss_per_con,
                "total_trades": total_trades,
                "success_percent": success_percent,
                "duration_sec_p20": duration_sec_p20,
                "duration_sec_median": duration_sec_median,
                "duration_sec_p80": duration_sec_p80,
                "setup_risk_reward": risk_reward,
                "effective_risk_reward": eff_risk_reward,
                "min_risk_reward": min_risk_reward,
                "max_risk_reward": max_risk_reward,
                "trading_days": trading_days,
                "total_days": total_days,
                "total_weeks": total_weeks,
                "trades_per_week": trades_per_week,
                "trades_per_day": trades_per_day,
                "trades_per_trading_day": trades_per_trading_day,
                "trade_ticks": ticks_list,
                }

    def weekly_stats(self, include_first_min: bool = True):
        """Return trade statistics grouped into weekly Sunday-keyed buckets.

        Uses Sunday as the start of each week, with Sunday's date as the
        bucket name.
        """
        # Build a dict of weeks with zeroes as default values to ensure we
        # represent non-traded weeks in the result rather than leave gaps
        template = {"total_trades": 0,
                    "profitable_trades": 0,
                    "losing_trades": 0,
                    "gl_in_ticks": 0,
                    "success_rate": "nil",
                    }
        # If no Trades, no stats can be gleaned
        if len(self.trades) == 0:
            return {}
        self.sort_trades()
        w_start = self.trades[0].open_dt
        w_end = self.trades[-1].open_dt
        result = dict_of_weeks(start_dt=w_start,
                               end_dt=w_end,
                               template=template)
        # Loop through trades to aggregate stats
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                d = dt_as_dt(t.open_dt)
                w = str(start_of_week_date(dt=d))
                result[w]["total_trades"] += 1
                if t.profitable:
                    result[w]["profitable_trades"] += 1
                else:
                    result[w]["losing_trades"] += 1
                result[w]["gl_in_ticks"] += ((t.exit_price - t.entry_price)
                                             / self.symbol.tick_size
                                             * t.flipper)
        # Calculate success rates
        for k in result.keys():
            if result[k]["total_trades"] > 0:
                srate = round(result[k]["profitable_trades"]
                              / result[k]["total_trades"]
                              * 100, 0)
                result[k]["success_rate"] = srate

        return result


class Backtest():
    """Base class for backtests; subclass to implement specific strategy rules.

    Contains core functionality but is not intended to run backtests
    directly.  Create subclasses with methods and parameters representing
    the specific rules of the backtests being performed.

    Attributes:
        start_dt (str or datetime): Beginning of time period evaluated which
            may be earlier than the first trade datetime
        end_dt (str or datetime): End of time period evaluated
        timeframe (str): timeframe of underlying chart trades were evaluated
            on
        trading_hours (str): whether to run trades during regular trading
            hours only ('rth') or include extended/globex hours ('eth')
        symbol (str or Symbol): The symbol or "ticker" being
            evaluated.  If passed as a str, the object will fetch the symbol
            with a matching 'name'.
        name (str): Human friendly label representing this object
        class_name (str): attrib to identify subclass, primarily used by
            storage functions to reassemble retrieved data into the correct
            object type
        parameters (dict): Backtest specific parameters needed to evaluate.
            These will vary and be handled by subclases typically.
        bt_id (str): unique ID used for storage and analysis purposes,
            can be used to link related TradeSeries() and Analyzer() objects
        chart_tf (Chart): Underlying chart used for evaluation at
            same tf as Backtest object.  This is typically used to evaluate
            timeframe specific candle patterns and attributes that may be
            needed by the specific backtest rules.
        chart_1m (Chart): underlying 1m chart is tyipcally used in
            combination with chart_tf to find specific entries and exits and
            build Trade() objects.
        autoload_charts (bool): Whether to automatically load chart_tf and
            chart_1m from central storage at creation
            at creation (default True)
        prefer_stored (bool): If a backtest with the same bt_id is in storage,
            configure this object with it's configuration rather than creating
            a new backtest (default True)
        tradeseries (list): List of TradeSeries() objects which will be
            created when the Backtest is run
    """

    def __init__(self,
                 start_dt,
                 end_dt,
                 timeframe: str,
                 trading_hours: str,
                 symbol,
                 name: str,
                 parameters: dict,
                 bt_id: str = None,
                 class_name: str = "Backtest",
                 chart_tf=None,
                 chart_1m=None,
                 autoload_charts: bool = False,
                 prefer_stored: bool = True,
                 tradeseries: list = None,
                 ):
        self.start_dt = dt_as_str(start_dt)
        self.end_dt = dt_as_str(end_dt)
        if valid_timeframe(timeframe):
            self.timeframe = timeframe
        if valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        check_tf_th_compatibility(tf=timeframe, th=trading_hours)
        if isinstance(symbol, str):
            self.symbol = get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.name = name
        if bt_id is None:
            self.bt_id = name
        else:
            self.bt_id = bt_id
        self.class_name = class_name
        self.parameters = deepcopy(parameters)
        self.chart_tf = chart_tf
        self.chart_1m = chart_1m
        if tradeseries is None:
            self.tradeseries = []
        else:
            self.tradeseries = tradeseries.copy()
            for ts in self.tradeseries:
                ts.update_bt_id(self.bt_id)
        self.prefer_stored = prefer_stored
        if self.prefer_stored:
            from_store = self.config_from_storage()
        else:
            from_store = False
        self.autoload_charts = autoload_charts
        # Only load charts if this copy wasn't configured from storage
        # as config_from_storage will load charts via rerunning __init__
        if self.autoload_charts and not from_store:
            self.load_charts()

    def __eq__(self, other):
        """Return True if all Backtest attributes are equal."""
        return (self.start_dt == other.start_dt
                and self.end_dt == other.end_dt
                and self.timeframe == other.timeframe
                and self.trading_hours == other.trading_hours
                and self.symbol == other.symbol
                and self.name == other.name
                and self.bt_id == other.bt_id
                and self.class_name == other.class_name
                and self.chart_tf == other.chart_tf
                and self.chart_1m == other.chart_1m
                and self.tradeseries == other.tradeseries
                and self.sub_eq(other)
                )

    def __ne__(self, other):
        """Return True if any Backtest attribute differs."""
        return not self.__eq__(other)

    def sub_eq(self, other):
        """Placeholder for subclasses to add extra __eq__ comparison logic.

        Any comparison of parameters should be done here as they are
        subclass specific.
        """
        return self.parameters == other.parameters

    def sub_to_json(self, working):
        """Placeholder for subclasses to normalize extra attributes for JSON.
        """
        return working

    def to_json(self,
                suppress_tradeseries: bool = True,
                suppress_trades: bool = True,
                suppress_charts: bool = True,
                suppress_chart_candles: bool = True,
                ):
        """Return a JSON representation with custom types normalized.

        Converts datetime and other non-serializable types to
        strings for portability.
        """
        working = deepcopy(self.__dict__)
        working["symbol"] = working["symbol"].ticker
        if suppress_charts:
            working["chart_tf"] = "Chart suppressed for output sanity"
            working["chart_1m"] = "Chart suppressed for output sanity"
        else:
            if self.chart_tf is not None:
                working["chart_tf"] = self.chart_tf.to_clean_dict(
                        suppress_candles=suppress_chart_candles,
                        )
            if self.chart_1m is not None:
                working["chart_1m"] = self.chart_1m.to_clean_dict(
                        suppress_candles=suppress_chart_candles,
                        )
        if suppress_tradeseries:
            num = len(self.tradeseries)
            m = f"{num} Tradeseries suppressed for output sanity"
            clean_tradeseries = [m]
        else:
            clean_tradeseries = []
            for t in self.tradeseries:
                clean_tradeseries.append(t.to_clean_dict(
                    suppress_trades=suppress_trades,
                    ))
        working["tradeseries"] = clean_tradeseries

        return json.dumps(self.sub_to_json(working))

    def to_clean_dict(self,
                      suppress_tradeseries: bool = True,
                      suppress_trades: bool = True,
                      suppress_charts: bool = True,
                      suppress_chart_candles: bool = True,
                      ):
        """Converts to JSON string then back to a python dict.

        This helps to normalize types (I'm looking at YOU datetime) while
        ensuring a portable python data structure
        """
        return json.loads(self.to_json(
            suppress_tradeseries=suppress_tradeseries,
            suppress_trades=suppress_trades,
            suppress_charts=suppress_charts,
            suppress_chart_candles=suppress_chart_candles,
            ))

    def __str__(self):
        """Return the string representation of this Backtest."""
        return str(self.to_clean_dict())

    def __repr__(self):
        """Return the repr string of this Backtest."""
        return str(self)

    def pretty(self,
               suppress_tradeseries: bool = True,
               suppress_trades: bool = True,
               suppress_charts: bool = True,
               suppress_chart_candles: bool = True,
               ):
        """Return a formatted, indented string representation of this object.

        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(
            suppress_tradeseries=suppress_tradeseries,
            suppress_trades=suppress_trades,
            suppress_charts=suppress_charts,
            suppress_chart_candles=suppress_chart_candles,
            ),
            indent=4,
            )

    def load_charts(self):
        """Load the Chart for this Backtest based on its datetimes and symbol.

        This is the base data for calculating trades.  Also restricts the
        Backtest's start_dt/end_dt to match the earliest and latest 1m
        candles available from storage to prevent future calculation gaps.
        """
        # Build candle charts, retrieving candles from storage
        self.chart_tf = Chart(c_timeframe=self.timeframe,
                              c_trading_hours=self.trading_hours,
                              c_symbol=self.symbol,
                              c_start=self.start_dt,
                              c_end=self.end_dt,
                              autoload=True,
                              )
        self.chart_1m = Chart(c_timeframe="1m",
                              c_trading_hours=self.trading_hours,
                              c_symbol=self.symbol,
                              c_start=self.start_dt,
                              c_end=self.end_dt,
                              autoload=True)
        # Limit the timeframe of the Backtest based on existing candles
        self.start_dt = self.chart_1m.c_candles[0].c_datetime
        self.end_dt = self.chart_1m.c_candles[-1].c_datetime
        # And adjust the chart timeframes to match the Backtest
        self.chart_tf.c_start = self.start_dt
        self.chart_tf.c_end = self.end_dt
        self.chart_1m.c_start = self.start_dt
        self.chart_1m.c_end = self.end_dt

    def delete_from_storage(self,
                            include_tradeseries: bool = True,
                            include_trades: bool = True,
                            ):
        """Delete this object from central storage if it exists by bt_id.

        Optionally (default=True) also remove all TradeSeries and Trade
        objects matching this object's bt_id.
        """
        result = {"backtest": None, "tradeseries": []}

        result["backtest"] = delete_backtests_by_field(
                symbol=self.symbol,
                field="bt_id",
                value=self.bt_id,
                )
        if include_tradeseries:
            for ts in self.tradeseries:
                result["tradeseries"].append(ts.delete_from_storage(
                    include_trades=include_trades,
                    ))

        return result

    def count_tradeseries(self):
        """Return the number of TradeSeries currently attached."""
        if self.tradeseries is None:
            return 0
        else:
            return len(self.tradeseries)

    def count_trades(self):
        """Return the number of Trades currently attached."""
        count = 0
        for ts in self.tradeseries:
            count += ts.count_trades()
        return count

    def update_tradeseries(self,
                           ts,
                           clear_storage: bool = True):
        """Add an existing TradeSeries to this Backtest.

        Typically used for pulling results from previous runs to update
        with new data.  If a TradeSeries with the same ts_id is already
        attached, this will replace it.  clear_storage controls whether
        the replaced TradeSeries is deleted from storage.
        """
        if self.tradeseries is None:
            self.tradeseries = []
        # Associate this TradeSeries with this Backtest
        ts.update_bt_id(self.bt_id)
        # Remove any existing TradeSeries with the same ts_id in case this
        # is an update rather than new addition
        self.remove_tradeseries(ts.ts_id, clear_storage=clear_storage)
        # Add the tradeseries to the Backtest
        self.tradeseries.append(ts)
        self.sort_tradeseries()

    def remove_tradeseries(self,
                           ts_id,
                           clear_storage: bool = True):
        """Removes any TradeSeries with the given ts_id from the Backtest.

        Optionally remove TradeSeries and it's Trades from storage as well
        (default = True).
        """
        # Delete TradeSeries and associated Trades from storage
        if clear_storage:
            delete_tradeseries_by_field(symbol="ES",
                                        field="ts_id",
                                        value=ts_id,
                                        )
            delete_trades_by_field(symbol="ES",
                                   field="ts_id",
                                   value=ts_id,
                                   )
        # Rebuild Backtest's list of TradeSeries, excluding any matching ts_id
        self.tradeseries = [ts for ts in self.tradeseries if ts.ts_id != ts_id]
        return True

    def sort_tradeseries(self):
        """Sort attached TradeSeries in ascending ts_id order."""
        self.tradeseries.sort(key=lambda t: t.ts_id)

    def load_tradeseries(self):
        """Load and attach all TradeSeries matching this bt_id from storage.

        This will replace any currently attached tradeseries.
        """
        self.tradeseries = get_tradeseries_by_field(field="bt_id",
                                                    value=self.bt_id,
                                                    include_trades=True,
                                                    )
        self.sort_tradeseries()

    def restrict_dates(self,
                       new_start_dt: str,
                       new_end_dt: str,
                       update_storage: bool = False,
                       ):
        """Trim the Backtest's datetime range and all attached TradeSeries.

        Removes Trades outside the new range.  Pass update_storage=True to
        also persist the changes (destructive).  Typically used to clean up
        failed partial runs or to analyze a targeted sub-range.
        """
        os = dt_as_dt(self.start_dt)
        oe = dt_as_dt(self.end_dt)
        ns = dt_as_dt(new_start_dt)
        ne = dt_as_dt(new_end_dt)
        # Ensure new dates don't expand the daterange, they should only reduce
        # or keep unchanged
        if ns < os:
            raise ValueError(f"new_start_dt {new_start_dt} cannot be earlier "
                             f"than the current self.start_dt {self.start_dt}")
        if ne > oe:
            raise ValueError(f"new_end_dt {new_end_dt} cannot be later "
                             f"than the current self.end_dt {self.end_dt}")
        # Update Backtest start and end dates and optionally store
        self.start_dt = dt_as_str(new_start_dt)
        self.end_dt = dt_as_str(new_end_dt)
        if update_storage:
            self.delete_from_storage(
                include_tradeseries=False, include_trades=False)
            _dhstore('store_backtests', backtests=[self])
        # Update the attached Charts for the new dates as well if loaded
        if self.chart_tf is not None:
            self.chart_tf.restrict_dates(new_start_dt=new_start_dt,
                                         new_end_dt=new_end_dt,
                                         )
        if self.chart_1m is not None:
            self.chart_1m.restrict_dates(new_start_dt=new_start_dt,
                                         new_end_dt=new_end_dt,
                                         )
        # Update all attached TradeSeries
        for ts in self.tradeseries:
            ts.restrict_dates(new_start_dt=new_start_dt,
                              new_end_dt=new_end_dt,
                              update_storage=update_storage,
                              )

    def get_autoclose_time_by_date(self,
                                   candle_date,
                                   closed_events,
                                   default_autoclose):
        """Determine autoclose time for a date, accounting for early closures.

        Returns adjusted time (5 min before close) only when earlier than
        the default autoclose.  Autoclose may not be used in all subclasses.

        Args:
            candle_date (datetime.date): Date to check (must be datetime.date)
            closed_events (list): List of Event objects with Closed category
            default_autoclose: Default autoclose time, can be datetime.time or
                               str in "HH:MM:SS" format

        Returns:
            datetime.time: Adjusted autoclose time (5 minutes before actual
                          close) if closure is before default, or
                          default_autoclose if no early close event found or
                          if closure time is after the default autoclose.
        """
        # Ensure default_autoclose is a datetime.time object
        if isinstance(default_autoclose, str):
            default_autoclose = dt_as_dt(
                f"2000-01-01 {default_autoclose}").time()

        # Ensure candle_date is a datetime.date object
        if not isinstance(candle_date, dt.date):
            raise TypeError(f"candle_date must be datetime.date, got "
                            f"{type(candle_date).__name__}")

        # Find events that start on this date with Closed category
        for event in closed_events:
            event_start_date = dt_as_dt(event.start_dt).date()
            # Check if event starts on this date and is a Closed event
            if (event_start_date == candle_date and
                    event.category == "Closed"):
                close_time = dt_as_dt(event.start_dt).time()
                # Only apply early close if it's actually before default
                if close_time < default_autoclose:
                    # Calculate autoclose as 5 minutes before
                    close_dt = dt.datetime.combine(
                        candle_date, close_time
                    ) - timedelta(minutes=5)
                    autoclose_time = close_dt.time()
                    log.info(f"Early close event detected for {candle_date}:"
                             f" market closes at {close_time}.  Setting "
                             f"autoclose to {autoclose_time}")
                    return autoclose_time
        # If no early close event found for this date, or if closure is after
        # default autoclose, return default autoclose time
        return default_autoclose

    def close_if_past_autoclose(self,
                                trade,
                                current_candle,
                                prev_candle,
                                autoclose_time,
                                current_time,
                                log_id=""):
        """Close an open trade if the current candle exceeds autoclose_time.

        Failsafe to catch edge cases where trades extend past autoclose
        (e.g., from overnight sessions).  Uses prev candle's open as exit.

        Args:
            trade: Trade object to potentially close
            current_candle: Current Candle object being evaluated
            prev_candle: Previous Candle object (or None on first iteration)
            autoclose_time (datetime.time): Expected autoclose time
            current_time (datetime.time): Current candle time
            log_id (str): Backtest ID for logging

        Returns:
            bool: True if trade was closed by this method, False otherwise
        """
        if current_time > autoclose_time and trade.is_open:
            cnow = f"candle={dt_as_str(prev_candle.c_datetime)}"
            log.warn(f"{log_id} {cnow} action=failsafe_autoclose msg='"
                     f"Forcing close: current time {current_time} exceeds"
                     f" autoclose {autoclose_time}. Closing at previous"
                     f" candle open: {prev_candle.c_open}'")
            trade.close(price=prev_candle.c_open, dt=prev_candle.c_datetime)
            trade.tags.append("autoclosed")
            trade.tags.append("failsafe_close")
            return True
        return False

    def config_from_storage(self):
        """Placeholder to load this Backtest's configuration from storage.

        Subclasses should implement this to retrieve stored configuration.
        By default does nothing since __init__ has already configured the
        object.  Should also load any stored TradeSeries and their Trades.
        """
        self.sort_tradeseries()
        # Subclass copies of this method should return True if configuration
        # from storage was successful (a stored version was found and applied)
        # and False if it was not reconfigured from storage.
        return False

    def incorporate_parameters(self):
        """Placeholder for subclasses to validate and set up custom parameters.

        This will vary greatly from one type of backtest to another.
        """
        pass

    def calculate(self):
        """Placeholder for subclasses to calculate TradeSeries from chart data.

        This will vary greatly from one type of backtest to another.  At
        the end of the run it will likely need to store the Backtest along
        with its child TradeSeries and grandchild Trades.
        """
        pass


__all__ = [
    "CANDLE_TIMEFRAMES",
    "BEGINNING_OF_TIME",
    "MARKET_ERAS",
    "bot",
    "get_symbol_by_ticker",
    "get_candles",
    "get_events",
    "get_indicator_datapoints",
    "store_indicator_datapoints",
    "store_indicator",
    "get_trades_by_field",
    "get_tradeseries_by_field",
    "delete_tradeseries_by_field",
    "delete_tradeseries",
    "delete_trades_by_field",
    "delete_trades",
    "delete_backtests_by_field",
    "delete_backtests",
    "Symbol",
    "Candle",
    "Chart",
    "Event",
    "Day",
    "IndicatorDataPoint",
    "Indicator",
    "IndicatorSMA",
    "IndicatorEMA",
    "Trade",
    "TradeSeries",
    "Backtest",
]
