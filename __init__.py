"""
dhtrader: Trading analysis module

Public API exports domain classes and storage functions.
"""
from .dhtypes import (
    Backtest,
    Candle,
    Chart,
    Day,
    Event,
    Indicator,
    IndicatorDataPoint,
    IndicatorEMA,
    IndicatorSMA,
    Symbol,
    Trade,
    TradeSeries,
)

from .dhcommon import (
    dt_as_dt,
    dt_as_epoch,
    epoch_as_dt,
    epoch_as_str,
    str_as_dt,
    str_as_epoch,
)

from .dhutil import (
    compare_candles_vs_csv,
)

try:
    from .dhstore import (
        delete_backtests,
        delete_backtests_by_field,
        delete_trades,
        delete_trades_by_field,
        delete_tradeseries,
        delete_tradeseries_by_field,
        get_backtests_by_field,
        get_candles,
        get_events,
        get_indicator,
        get_indicator_datapoints,
        get_symbol_by_ticker,
        get_trades_by_field,
        get_tradeseries_by_field,
        review_candles,
        review_trades,
        review_tradeseries,
        store_backtests,
        store_candle,
        store_candles,
        store_event,
        store_trades,
        store_tradeseries,
    )
except Exception:
    # dhstore requires MongoDB configuration, skip if not available
    pass

__all__ = [
    'Backtest',
    'Candle',
    'Chart',
    'compare_candles_vs_csv',
    'Day',
    'delete_backtests',
    'delete_backtests_by_field',
    'delete_trades',
    'delete_trades_by_field',
    'delete_tradeseries',
    'delete_tradeseries_by_field',
    'dt_as_dt',
    'dt_as_epoch',
    'epoch_as_dt',
    'epoch_as_str',
    'Event',
    'get_backtests_by_field',
    'get_candles',
    'get_events',
    'get_indicator',
    'get_indicator_datapoints',
    'get_symbol_by_ticker',
    'get_trades_by_field',
    'get_tradeseries_by_field',
    'Indicator',
    'IndicatorDataPoint',
    'IndicatorEMA',
    'IndicatorSMA',
    'review_candles',
    'review_trades',
    'review_tradeseries',
    'store_backtests',
    'store_candle',
    'store_candles',
    'store_event',
    'store_trades',
    'store_tradeseries',
    'str_as_dt',
    'str_as_epoch',
    'Symbol',
    'Trade',
    'TradeSeries',
]

__version__ = '1.0.0'
