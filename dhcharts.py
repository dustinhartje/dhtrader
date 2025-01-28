import datetime as dt
import sys
import json
import dhutil as dhu
import dhstore as dhs
from statistics import fmean

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
    # TODO LOWPRI - go through all module files and convert everything to use
    #      the Symbol() class.  There will be cases where I measure tick
    #      sizes and other things that should also reference this class instead
    #      of hardcoded values.
    def __init__(self,
                 ticker: str,
                 name: str,
                 leverage_ratio: float,
                 ):

        self.ticker = ticker
        self.name = name
        self.leverage_ratio = leverage_ratio


class Candle():
    def __init__(self,
                 c_datetime,
                 c_timeframe: str,
                 c_open: float,
                 c_high: float,
                 c_low: float,
                 c_close: float,
                 c_volume: int,
                 c_symbol: str,
                 c_tags: list = None,
                 c_epoch: int = None,
                 c_date: str = None,
                 c_time: str = None
                 ):

        self.c_datetime = dhu.dt_as_str(c_datetime)
        self.c_timeframe = c_timeframe
        dhu.valid_timeframe(self.c_timeframe)
        self.c_open = float(c_open)
        self.c_high = float(c_high)
        self.c_low = float(c_low)
        self.c_close = float(c_close)
        self.c_volume = int(c_volume)
        self.c_symbol = c_symbol
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
        return json.dumps(self.__dict__)

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
        if not isinstance(other, Candle):
            print("wrong type")
            return False
        return (self.c_datetime == other.c_datetime and
                self.c_timeframe == other.c_timeframe and
                self.c_open == other.c_open and
                self.c_high == other.c_high and
                self.c_low == other.c_low and
                self.c_close == other.c_close and
                self.c_volume == other.c_volume and
                self.c_symbol == other.c_symbol)

    def __ne__(self, other):
        return not self.__eq__(other)

    def store(self):
        dhs.store_candle(self)


class Chart():
    def __init__(self,
                 c_timeframe: str,
                 c_symbol: str,
                 c_start: str = None,
                 c_end: str = None,
                 c_candles: list = None,
                 autoload: bool = False,
                 ):

        self.c_timeframe = c_timeframe
        if self.c_timeframe not in CANDLE_TIMEFRAMES:
            raise ValueError(f"c_timeframe of {c_timeframe} is not in the "
                             "known list {CANDLE_TIMEFRAMES}")
        self.c_symbol = c_symbol
        if not self.c_symbol == 'ES':
            raise ValueError(f"c_symbol {self.c_symbol} is not supported, "
                             "only 'ES' is currently allowed.")
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

    def to_json(self,
                suppress_candles: bool = True,
                ):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = self.__dict__.copy()
        if suppress_candles:
            clean_cans = ["Candles suppressed for output sanity"]
        else:
            clean_cans = []
            for c in working["c_candles"]:
                clean_cans.append(c.to_clean_dict())
        working["c_candles"] = clean_cans
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
        self.c_candles = dhs.get_candles(
                start_epoch=dhu.dt_to_epoch(self.c_start),
                end_epoch=dhu.dt_to_epoch(self.c_end),
                timeframe=self.c_timeframe,
                symbol=self.c_symbol,
                )
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
                 symbol: str,
                 category: str,
                 tags: list = None,
                 notes: str = "",
                 ):
        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        self.symbol = symbol
        if self.symbol != "ES":
            raise ValueError("Only ES is currently supported for Event.symbol")
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

    def store(self):
        return dhs.store_event(self)


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
                 epoch: int = None
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
        # if not isinstance(other, IndicatorDataPoint):
        #     print("wrong type")
        #     return False
        try:
            return (self.dt == other.dt and
                    self.value == other.value and
                    self.ind_id == other.ind_id and
                    self.epoch == other.epoch)
        except Exception:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def store(self):
        return dhs.store_indicator_datapoints(datapoints=[self])


class Indicator():
    def __init__(self,
                 name: str,
                 description: str,
                 timeframe: str,
                 trading_hours: str,
                 symbol: str,
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
        self.name = name
        self.description = description
        if not dhu.valid_timeframe(timeframe):
            raise ValueError(f"{timeframe} not valid for timeframe")
        self.timeframe = timeframe
        if not dhu.valid_trading_hours(trading_hours):
            raise ValueError(f"{trading_hours} not valid for trading_hours")
        self.trading_hours = trading_hours
        self.symbol = symbol
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
            self.ind_id = (f"{self.symbol}{self.timeframe}{self.name}")
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
        working = self.__dict__.copy()
        working["candle_chart"] = working["candle_chart"].to_clean_dict(
                suppress_candles=suppress_chart_candles,
                )
        if suppress_datapoints:
            clean_dps = ["Datapoints suppressed for output sanity"]
        else:
            clean_dps = []
            for d in working["datapoints"]:
                clean_dps.append(d.to_clean_dict())
        working["datapoints"] = clean_dps

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
        # TODO add earliest and latest datapoints info to this
        #      see dhmongo review_candles() for example

        output = {"ind_id": self.ind_id,
                  "name": self.name,
                  "description": self.description,
                  "timeframe": self.timeframe,
                  "trading_hours": self.trading_hours,
                  "symbol": self.symbol,
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
        if self.chart is None:
            self.load_underlying_chart()
        if not isinstance(self.candle_chart, Chart):
            raise TypeError(f"candle_chart {type(self.candle_chart)} must be a"
                            " <class dhcharts.Chart> object")
        self.chart.sort()

        # Subclass specific functionality starts here
        result = "No calculations can be done on parent class Indicator()"
        print(result)

        return result

    def store(self,
              store_datapoints: bool = True,
              ):
        """uses DHStore functionality to store metadata and time series
        datapoints into central storage
        """
        return dhs.store_indicator(self,
                                   store_datapoints=store_datapoints,
                                   )

        return result
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
        # Only update ind_id if it still matches the Parent class format
        # otherwise we likely loaded from storage and already have this
        if self.ind_id == (f"{self.symbol}{self.timeframe}{self.name}"):
            self.ind_id += str(self.length)
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
        # Only update ind_id if it still matches the Parent class format
        # otherwise we likely loaded from storage and already have this
        if self.ind_id == (f"{self.symbol}{self.timeframe}{self.name}"):
            self.ind_id += str(self.length)
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
# TODO create a script and possibly .json file that creates/updates all the
#      indicators I'm going to maintain (starting with 9 and 20 sma
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


if __name__ == '__main__':
    # TODO LOWPRI these should be unit tests or something similar eventually
    # Run a few tests to confirm desired functionality

    # Test pretty output which also confirms json and clean_dict working

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
    # Indicators
    print("\n######################### INDICATORS ###########################")
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
    print("\n------------------------------------------------")
    print("Validate candles as expected")
    print(f"length of candle_chart: {len(itest.candle_chart.c_candles)}")
    expected = [{'dt': '2025-01-10 15:00:00', 'value': 5890.25,
                 'ind_id': 'ESethe1hTestEMA-DELETEME', 'epoch': 1736539200},
                {'dt': '2025-01-10 16:00:00', 'value': 5884.5,
                 'ind_id': 'ESethe1hTestEMA-DELETEME', 'epoch': 1736542800},
                {'dt': '2025-01-12 18:00:00', 'value': 5879.6,
                 'ind_id': 'ESethe1hTestEMA-DELETEME', 'epoch': 1736722800},
                {'dt': '2025-01-12 19:00:00', 'value': 5875.38,
                 'ind_id': 'ESethe1hTestEMA-DELETEME', 'epoch': 1736726400},
                {'dt': '2025-01-12 20:00:00', 'value': 5869.3,
                 'ind_id': 'ESethe1hTestEMA-DELETEME', 'epoch': 1736730000},
                ]
    calculated = itest.datapoints[-5:]
    print("(E)xpected vs (C)alculated last 5 datapoints:")
    for i in range(5):
        print(f"E: {expected[i]}")
        print(f"C: {calculated[i]}")
    print("If expected and calculated don't match above, something is broken")
    print("\n################################################")

    # Testing storage and retrieval
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
