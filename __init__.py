"""
dhtrader: Trading analysis module

Public API exports domain classes and storage functions.
"""
from .dhtypes import (
    Candle,
    Event,
    Symbol,
    IndicatorDataPoint,
    Indicator,
    IndicatorSMA,
    IndicatorEMA,
    Chart,
    Day,
    Trade,
    TradeSeries,
    Backtest,
)

try:
    from .dhstore import (
        store_candle,
        store_candles,
        store_trade,
        store_backtests,
        store_event,
        get_candles,
        get_trades_by_field,
        get_backtests_by_field,
        get_events,
        get_symbol_by_ticker,
    )
except Exception:
    # dhstore requires MongoDB configuration, skip if not available
    pass

__all__ = [
    'Candle', 'Event', 'Symbol', 'IndicatorDataPoint', 'Indicator',
    'IndicatorSMA', 'IndicatorEMA', 'Chart', 'Day', 'Trade',
    'TradeSeries', 'Backtest',
    'store_candle', 'store_candles', 'store_trade', 'store_backtests',
    'store_event', 'get_candles', 'get_trades_by_field',
    'get_backtests_by_field', 'get_events', 'get_symbol_by_ticker',
]

__version__ = '2.0.0'
