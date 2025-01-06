import datetime as dt
import sys
import json
import dhutil as dhu
import dhstore as dhs

CANDLE_TIMEFRAMES = ['1m', '5m', '15m', '1h', '4h', '1d', '1w']


class Symbol():
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

        self.c_datetime = dhu.dt_as_dt(c_datetime)
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

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

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
                 c_start=None,
                 c_end=None,
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
        self.c_start = c_start
        if not isinstance(self.c_start, dt.datetime) and c_start is not None:
            raise TypeError(f"c_start {type(c_start)} must be a"
                            "<class datetime.datetime> object")
        self.c_end = c_end
        if not isinstance(self.c_end, dt.datetime) and c_end is not None:
            raise TypeError(f"c_end {type(c_end)} must be a"
                            "<class datetime.datetime> object")
        if c_candles is None:
            self.c_candles = []
        else:
            self.c_candles = c_candles
        self.autoload = autoload
        if self.autoload:
            self.load_candles()

    def __repr__(self):
        if self.c_candles is not None:
            earliest_candle = self.c_candles[0].c_datetime
            latest_candle = self.c_candles[-1].c_datetime
            candles_count = len(self.c_candles)
        else:
            earliest_candle = None
            latest_candle = None
            candles_count = 0
        this = {"c_timeframe": self.c_timeframe,
                "c_symbol": self.c_symbol,
                "c_start": self.c_start,
                "c_end": self.c_end,
                "autoload": self.autoload,
                "candles_count": candles_count,
                "earliest_candle": earliest_candle,
                "latest_candle": latest_candle,
                }
        return str(this)

    def __str__(self):
        return str(self.__repr__)

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

    def load_candles(self):
        """Load candles from central storage based on current attributes"""
        self.c_candles = dhs.get_candles(
                start_epoch=dhu.dt_to_epoch(self.c_start),
                end_epoch=dhu.dt_to_epoch(self.c_end),
                timeframe=self.c_timeframe,
                symbol=self.c_symbol,
                )
        self.sort_candles()


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
        self.start_dt = dhu.dt_as_dt(start_dt)
        self.end_dt = dhu.dt_as_dt(end_dt)
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

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)

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
                 indicator_id: str,
                 ):
        self.dt = dt
        self.value = value
        self.indicator_id = indicator_id

    def to_json(self):
        """returns a json version of this Trade object while normalizing
        custom types (like datetime to string)"""

        return json.dumps(self.__dict__)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())


class Indicator():
    def __init__(self,
                 short_name: str,
                 long_name: str,
                 description: str,
                 timeframe: str,
                 trading_hours: str,
                 symbol: str,
                 calc_version: str,
                 calc_details: str,
                 ):

        self.short_name = short_name
        self.long_name = long_name
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
        self.datapoints = None
        self.indicator_id = f"{self.short_name}.{self.calc_version}"

    def get_info(self):
        # TODO add earliest and latest datapoints info to this
        return {"indicator_id": self.indicator_id,
                "short_name": self.short_name,
                "long_name": self.long_name,
                "description": self.description,
                "timeframe": self.timeframe,
                "trading_hours": self.trading_hours,
                "symbol": self.symbol,
                "calc_version": self.calc_version,
                "calc_details": self.calc_details,
                }

    def calculate(self, chart: object):
        """This method will be specific to each type of indicator.  It should
        accpet only a list of Candles, sort it, and calculate new indicator
        datapoints from the candles."""
        if not isinstance(chart, Chart):
            raise TypeError(f"chart {type(chart)} must be a "
                            "<class dhcharts.Chart> object")
        # TODO In real class, make sure the chart is sorted unless the Chart
        # object already covers this out of the box
        print("No calculations can be done on parent class Indicator()")

        return False

    def store(self):
        """uses DHStore functionality to store metadata and time series
        datapoints in the default storage system (probably mongo)"""
        result = dhs.store_indicator(indicator_id=self.indicator_id,
                                     short_name=self.short_name,
                                     long_name=self.long_name,
                                     description=self.description,
                                     timeframe=self.timeframe,
                                     trading_hours=self.trading_hours,
                                     symbol=self.symbol,
                                     calc_version=self.calc_version,
                                     calc_details=self.calc_details,
                                     datapoints=self.datapoints,
                                     )

        return result


class IndicatorEMA(Indicator):
    def __init__(self,
                 short_name,
                 long_name,
                 description,
                 timeframe,
                 trading_hours,
                 symbol,
                 calc_version,
                 calc_details,
                 ):
        super().__init__(short_name,
                         long_name,
                         description,
                         timeframe,
                         trading_hours,
                         symbol,
                         calc_version,
                         calc_details,
                         )
    # TODO Next I'll need to retrieve the candles for any given timeframe
    #      into a Chart()
    # TODO Then add functionality somewhere (Chart() method?) to build higher
    #      timeframes from a 1m Chart().  This way I can start with the chart
    #      matching the timeframe of the indicator to make indicator calcs
    #      less tedious.  The calcs should always check the chart being input
    #      to ensure it's timeframe matches theirs.  They should probably also
    #      confirm the chart doesn't have gaps.
    #      --This might not be necessary at all.  I think I might just give
    #      each indicator it's own timeframe, with things like HOD/GXLO all
    #      using 1min charts.  then each indicator has a method that takes
    #      a datetime and returns the appropriate IndicatorDatapoint that
    #      closed during that candle or the candle before (need to think on
    #      which makes more sense.  maybe separate methods for each?  like
    #      get_prior_close_value() and get_current_close_value()?)
    #      ----I might be mixing up two different ideas here, read it again
    #      when I'm not falling asleep
    # TODO flesh out indicatorEMA to be able to build on any tf and hours
    #      with whatever params make sense for calculating it
    # TODO method to retrieve results time series from mongo
    #      basic form was written in dhstore, needs to be able to limit time
    #      window though and then the calling method added here
    # TODO method to check time series for missing data points

# TODO add subclass of Inidicators class for each type of indicator I want

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
    # TODO delete all this, just using it for quick testing
    #      or turn it into a test_basics function like elsewhere
    #      Or or move a version of it into dhstore and dhmongo

    # Testing subclass stuff
    i = IndicatorEMA(short_name="shorty",
                     long_name="blah",
                     description="yadda",
                     calc_version="yoda",
                     calc_details="yeeta"
                     )
    print(i.get_info())

    sys.exit()
    # Testing storage and retrieval

    dps = [IndicatorDataPoint(dt='12/10/2024 01:30:00',
                              value=1.24,
                              indicator_id='test_indicator.0.1'),
           IndicatorDataPoint(dt='12/10/2024 02:30:00',
                              value=2.1,
                              indicator_id='test_indicator.0.1')]
    print(dps)
    i = Indicator(short_name='test_ind',
                  long_name='Indicator made for testing',
                  description='this is a description',
                  calc_version='0.1',
                  calc_details='no details yet just testing thigns',
                  )
    i.datapoints = dps
    result = i.store()
    print(f"dhindicator received {result}")
    print("################################################")
    print("Now let's see what's in mongo...")
    indicators = dhs.list_indicators()
    for i in indicators:
        print(i)
    print("And as for actual datapoints...")
    datapoints = dhs.get_indicator_datapoints(
            indicator_id="test_indicator.0.1")
    for d in datapoints:
        print(d)
