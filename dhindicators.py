import dhcharts as dhc
import dhstore as dhs
import dhutil as dhu
import json
import sys


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
        if not isinstance(chart, dhc.Chart):
            raise TypeError(f"chart {type(chart)} must be a "
                            "<class dhc.Chart> object")
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
    # TODO I'll also want some quick scripts to make daily/weekly updates easy
    #      These might accept date ranges or at least earliest date so it
    #      doesn't try to add everything.
    # TODO Next I'll need to retrieve the candles for any given timeframe
    #      into a Chart()
    # TODO Then add functionality somewhere (Chart() method?) to build higher
    #      timeframes from a 1m Chart().  This way I can start with the chart
    #      matching the timeframe of the indicator to make indicator calcs
    #      less tedious.  The calcs should always check the chart being input
    #      to ensure it's timeframe matches theirs.  They should probably also
    #      confirm the chart doesn't have gaps.
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


if __name__ == '__main__':
    # TODO delete all this, just using it for quick testing
    #      or turn it into a test_basics function like elsewhere

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
