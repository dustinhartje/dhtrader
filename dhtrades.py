import json
from datetime import datetime as dt

class Trade():
    def __init__(self,
                 open_dt: str,
                 direction: str,
                 entry_price: float,
                 stop_target: float,
                 prof_target: float,
                 open_drawdown: float,
                 close_drawdown: float,
                 max_drawdown: float,
                 close_dt: str = None,
                 evaluation_dt: str = None,
                 exit_price: float = None,
                 gain_loss: float = None,
                 stop_ticks: float = 0,
                 prof_ticks: float = 0,
                 offset_ticks: float = 0,
                 drawdown_impact: float = 0,
                 ticker: str = 'ES',
                 contracts: int = 1,
                 contract_value: float = 50,
                 is_open: bool = True,
                 successful: bool = None,
                 trade_name: str = None,
                 trade_version: str = None,
                 ):

        self.open_dt = open_dt
        self.close_dt = close_dt
        self.evaluation_dt = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        self.direction = direction
        self.entry_price: float = entry_price
        self.stop_target = stop_target
        self.prof_target = prof_target
        self.exit_price = exit_price
        self.open_drawdown = open_drawdown
        self.close_drawdown = close_drawdown
        self.max_drawdown = max_drawdown
        self.gain_loss = gain_loss
        self.stop_ticks = stop_ticks
        self.prof_ticks = prof_ticks
        self.offset_ticks = offset_ticks
        self.drawdown_impact = drawdown_impact
        self.ticker = ticker
        self.contracts = contracts
        self.contract_value = contract_value
        self.is_open = is_open
        self.successful = successful
        self.trade_name = trade_name
        self.trade_version = trade_version

    def csv_list(self):
        return [self.open_dt,
                self.close_dt,
                self.direction,
                self.entry_price,
                self.stop_target,
                self.prof_target,
                self.exit_price,
                self.open_drawdown,
                self.close_drawdown,
                self.max_drawdown,
                self.gain_loss,
                self.stop_ticks,
                self.prof_ticks,
                self.offset_ticks,
                self.drawdown_impact,
                self.ticker,
                self.contracts,
                self.contract_value,
                self.is_open,
                self.successful,
                self.evaluation_dt,
                self.trade_name,
                self.trade_version,
                ]

    def csv_header(self):
        return ['open_dt',
                'close_dt',
                'direction',
                'entry_price',
                'stop_target',
                'prof_target',
                'exit_price',
                'open_drawdown',
                'close_drawdown',
                'max_drawdown',
                'gain_loss',
                'stop_ticks',
                'prof_ticks',
                'offset_ticks',
                'drawdown_impact',
                'ticker',
                'contracts',
                'contract_value',
                'is_open',
                'successful',
                'evaluation_dt',
                'trade_name',
                'trade_version',
                ]

    def from_json(self, json_str: str = None, file_path: str = None):
        """Loads data into this Trade object from a json string or file"""
        if json_str is None and file_path is None:
            raise Exception('Must provide either json_str or file_path')
        if json_str is not None and file_path is not None:
            raise Exception('Provide json_str OR file_path, not both')

        if json_str is not None:
            d = json.loads(json_str)
        else:
            with open(file_path) as f:
                d = json.load(f)

        self.open_dt = d['open_dt']
        self.close_dt = d['close_dt']
        self.evaluation_dt = d['evaluation_dt']
        self.direction = d['direction']
        self.entry_price = d['entry_price']
        self.stop_target = d['stop_target']
        self.prof_target = d['prof_target']
        self.exit_price = d['exit_price']
        self.open_drawdown = d['open_drawdown']
        self.close_drawdown = d['close_drawdown']
        self.max_drawdown = d['max_drawdown']
        self.gain_loss = d['gain_loss']
        self.stop_ticks = d['stop_ticks']
        self.prof_ticks = d['prof_ticks']
        self.offset_ticks = d['offset_ticks']
        self.drawdown_impact = d['drawdown_impact']
        self.ticker = d['ticker']
        self.contracts = d['contracts']
        self.contract_value = d['contract_value']
        self.is_open = d['is_open']
        self.successful = d['successful']
        return True

    def to_json(self, file_path: str = None):
        """returns a json version of this Trade object while normalizing
        custom types (like datetime to string) and optionally writing the
        result to a file as well"""
        if not isinstance(self.open_dt, str):
            self.open_dt = dt.strftime(
                    self.open_dt,
                    "%Y-%m-%d %H:%M:%S")
        if not isinstance(self.close_dt, str):
            self.close_dt = dt.strftime(
                    self.close_dt,
                    "%Y-%m-%d %H:%M:%S")
        if not isinstance(self.evaluation_dt, str):
            self.evaluation_dt = dt.strftime(
                    self.evaluation_dt,
                    "%Y-%m-%d %H:%M:%S")
        if file_path is not None:
            with open(file_path, 'w', newline='') as f:
                json.dump(self.__dict__,
                          f,
                          indent=2,
                          )
        return json.dumps(self.__dict__)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def update_drawdown(self,
                        price_seen: float):
        """Takes a price, typically the candle high or low, and updates
           the drawdown impact accordingly"""
        # NOTE This only updates the drawdown_impact and is meant for
        # running trade updates.  Final drawdown calculations are done in the
        # close() method.

        # Represent drawdown_impact as negative unless/until the trade closes
        # in profit.  price_diff will be used to calculate it momentarily.
        price_diff = self.entry_price - price_seen
        # In case of a short trade this is reversed
        if self.direction == 'short':
            price_diff = price_diff * -1
        # print(f"drawdown_impact={self.drawdown_impact}")
        # Use the worst of the current and prior worse drawdown impact
        self.drawdown_impact = min(self.drawdown_impact, price_diff)
        # print(f"price_seen={price_seen} | "
        #       f"price_diff={price_diff} | entry_price={self.entry_price} "
        #       f"drawdown_impact={self.drawdown_impact}")

    def close(self,
              price: float,
              time: str,
              ):
        self.is_open = False
        self.close_dt = time
        self.exit_price = price
        contract_multiplier = self.contracts * self.contract_value
        self.gain_loss = ((self.exit_price - self.entry_price)
                          * contract_multiplier)
        # Invert if the trade was short rather than long
        if self.direction == 'short':
            self.gain_loss = self.gain_loss * -1
        # Close as a profitable trade if we made money
        if self.gain_loss > 0:
            self.successful = True
            # On a successful trade, our drawdown_impact is equal to the
            # profit made on the trade.
            self.drawdown_impact = self.gain_loss
        # Close as a losing trade if we lost money
        else:
            self.successful = False
            # If the trade was a loss, our drawdown is negatively impacted
            # by the delta from the entry to the max profit it reached
            # (drawdown_impact) plus the stop target / amount lost in the trade
            # (gain_loss).  This amount will be negative for a loss.
            self.drawdown_impact = ((self.drawdown_impact
                                     * contract_multiplier)
                                    + self.gain_loss)
        self.close_drawdown = min(self.open_drawdown + self.drawdown_impact,
                                  self.max_drawdown)
        # print(f"CLOSED: entry={self.entry_price} exit={self.exit_price} "
        #       f"drawdown_impact={self.drawdown_impact}")
