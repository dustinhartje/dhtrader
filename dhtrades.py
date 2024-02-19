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
                 ):

        self.open_dt = open_dt
        self.close_dt = close_dt
        self.evaluation_dt = dt.now()
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
                ]

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
