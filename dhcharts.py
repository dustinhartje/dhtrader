import datetime as dt
import sys
import dhutil as dhu
import dhstore as dhs

# TODO at this point I think it's time to start building classes/investigating
#      backtest libraries to use because I think I want to go ahead and read
#      the CSV data into Candle & Day objects from here
#      Then those objects (probably Day objects) will build out the smaller
#      timeframe candles as lists/attributes (Charts?)
#
#      I want to be able to pass data into these objects at this point in the
#      script in a way that can be swapped out later if I switch to an api or
#      other data source.  i.e. firstrate.py should only contain stuff that is
#      specific to reading firstrate data, it should not do anything with it
#      beyond passing it to another module which can then take care of how I'm
#      going to organize and store it long term

#     TODO I need to decide how I'm going to want to store stuff for sure.  I
#          was originally thinking a structure like:
#          FuturesHistoricalData/ES/<DATE>/1m.csv/15m.csv/etc but after
#          thinking through classes below a little I think I'd like them to be
#          organized primarily as Days with the smaller timeframes as
#          subclasses or whatever the right term would be attached to each day
#          since I'll mostly use this for intraday stuff.
#
#          I'll create Chart objects which can pull specific times from Days
#          attributes or combine multiple Days when working on higher
#          timeframes when I'm actually working with the data, these don't
#          necessarily need to get stored but might if I decide to later
#
#          As I think about the objects I want, I'm leaning more and more
#          towards storing each Day as a json file for now and seeing how that
#          works out.  At some point I do want to explore putting them in
#          Mongo or some other such datastore but the amount of data I have is
#          manageable in files I think and all of it will be rebuildable.
#
#          I should definitely build it with a reusable idempotent script that
#          can be rerun at will so I don't have to worry about backups
#
#          TODO think a bit more about how this works if a Day changes and has
#               to be rewritten but some of it's data was used in a Chart or
#               higher timeframe that was also saved.  Does it somehow trigger
#               a recalc on those things?  I at least need to leave a note to
#               explore this later / maybe a github issue?
#
#               I definitely will want each of those other objects to have
#               rebuild() methods and maybe just need to write a master
#               rebuild_all script to run periodically which starts with
#               recalcing/rebuilding Days then moves on to other objects in
#               whatever order makes sense
#
#      candle
#        attributes: start and end times, symbol, timeframe (i.e. 1m/5m),
#                    o, h, l, c, direction/color
#          to think about / research:
#             what happens if a candle closes unchanged?  it's not red or green
#             what is it?  do I default it to green?
#             type/shape i.e. doji, buy signal, inside/outside/outsideup.
#             maybe this is None unless it matches some particular criteria
#             sizes: body size in pts, wick sizes, proportion of body to wicks
#             (helps to determine patterns)
#             for starters I can create these attributes and just set them all
#             to None, then if I want to use them later it'll easy to adjust.
#
#      day
#        attributes: symbol, date, list of candles for each timeframe, list of
#                    timeframes?)
#        OHLCV data / it's own candle object as an attribute?  I think that
#            makes the most sense
#        methods: check_integrity, rebuild
#      week / month
#        like a day, but for a week/month
#      pattern?
#          this would be an object with a specific type and some other info
#          attributes like
#          maybe a description, how to play it, etc.  it would have multiple
#          sequential candles
#      chart?  (i.e. multiple candles with defined timeframe/market hours)
#      play?  now we're getting into how to trade, I probably need to table
#          this until I get the rest built out

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
            # TODO revisit this later as I'm not sure how they'll be used yet
            #      and what the implications are for perfectly flat candles
            #      as sometimes seen especially in the depths of the overnight
            #      Should these be None?  0?  100?
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
            earliest_candle = self.c_candles[0]
            latest_candle = self.c_candles[-1]
            candles_count = len(self.c_candles)
        else:
            earliest_candle = None
            latest_candle = None
            candles_count = 0
        return {"c_timeframe": self.c_timeframe,
                "c_symbol": self.c_symbol,
                "c_start": self.c_start,
                "c_end": self.c_end,
                "autoload": self.autoload,
                "candles_count": candles_count,
                "earliest_candle": earliest_candle,
                "latest_candle": latest_candle,
                }

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

        # TODO do I need to check for gaps in candles?  what if I find them?
        # maybe this should be a separate method to call on demand?

    def load_candles(self):
        """Load candles from central storage based on current attributes"""
        self.c_candles = dhs.get_candles(
                start_epoch=dhu.dt_as_epoch(self.c_start),
                end_epoch=dhu.dt_as_epoch(self.c_end),
                timeframe=self.c_timeframe,
                symbol=self.c_symbol,
                )
        self.sort_candles()


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
            # print('------------------------')
            # print('Precalc daily candles and attributes')
            # print(vars(self))
            # print('------------------------')
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
            # Everything up to this point has been validated using 10/9 data
            # TODO calculate higher timeframe charts i.e. 5m, 15m, 1h..
            #      for each timeframe, build a new chart here
            #      then self.add_chart()
            #      can probably build each higher chart from the prev
            #      for speed vs using 1m as base for all?
            #      TODO make sure 1m chart is correctly sorted going into this

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

    def write_file(self, output_file: str):
        # TODO DEFERRING THIS STEP FOR THE NEAR TERM
        #      I want to get some actual trades running before I come back
        #      to trying to get a more robust storage system going and using
        #      long timeframe backtesting which is going to require me to
        #      store the processed daily data and then store the calculated
        #      results as well.  More notes below on initial findings and
        #      possible paths to explore (most disk vs sql)

        # TODO write this function to save to disk initially with the path
        #      to be provided by the caller.  Consider adding a set path
        #      as an attribute to the class maybe later instead?
        # TODO I will also need some way to read from disk eventually
        #      ideally without repeating a bunch of the __init__ code...

        # TODO figure out how to deal with the non-serializable issues here
        #      https://stackoverflow.com/questions/3768895/
        #             how-to-make-a-class-json-serializable
        #      has some ideas I can explore.

        #      Alternatively, try just loading everything
        #      This was very slow trying to load the full 15y file I think
        #      because the dataframe gets so big the lookups take a long time
        #      to get each individual day probably.  It was very slow per day
        #      Using only 6 weeks of data though it goes pretty quick, loads
        #      that up in probably 10-15 seconds which is managable for now.

        #      TODO consider SQL options
        #           https://stackoverflow.com/questions/2047814/is-it-possible
        #               -to-store-python-class-objects-in-sqlite
        # with open(output_file, 'w') as f:
        #     f.write(json.dumps(self))
        return True
