"""Tests for Symbol market hours, serialization, and era detection."""
import datetime as dt
import json
import pytest
from dhtrader import (
    dt_as_dt, dt_as_str, dt_to_epoch, Event, MARKET_ERAS, Symbol)


@pytest.fixture
def symbol():
    """Create and return a default ES Symbol fixture."""
    return Symbol(ticker="ES", name="ES", leverage_ratio=50,
                  tick_size=0.25)


def test_build_market_hours_context_metadata(symbol):
    """Verify context shape and schedule-day tracking for date spans."""
    context = symbol.build_market_hours_context(
        trading_hours="eth",
        start_dt="2099-01-05 00:00:00",
        end_dt="2099-01-11 23:59:59",
    )

    assert context["trading_hours"] == "eth"
    assert context["source_event_count"] == 0
    assert context["source_schedule_range_days"] == 7
    assert context["closed_ranges"]
    assert len(context["closed_ranges"]) == len(context["closed_range_starts"])


def test_is_open_dt_matches_market_is_open_across_eras(symbol):
    """Verify is_open_dt preserves market_is_open semantics by era."""
    test_times = [
        "00:00:00",
        "09:30:00",
        "16:14:00",
        "16:30:00",
        "17:00:00",
        "17:59:00",
        "18:00:00",
        "23:59:00",
    ]

    for era in MARKET_ERAS:
        probe_date = era["start_date"] + dt.timedelta(days=2)
        day_start = dt.datetime.combine(probe_date, dt.time(0, 0, 0))
        day_end = dt.datetime.combine(probe_date, dt.time(23, 59, 59))
        context = symbol.build_market_hours_context(
            trading_hours="eth",
            start_dt=day_start,
            end_dt=day_end,
        )

        for time_str in test_times:
            probe_dt = f"{probe_date} {time_str}"
            assert symbol.is_open_dt(target_dt=probe_dt, context=context) == (
                symbol.market_is_open(
                    trading_hours="eth",
                    target_dt=probe_dt,
                )
            )


def test_is_open_dt_matches_market_is_open_for_event_boundaries(symbol):
    """Verify event inclusivity boundaries are preserved by helpers."""
    events = [
        Event(
            start_dt="2099-01-06 12:00:00",
            end_dt="2099-01-06 13:00:00",
            symbol="ES",
            category="Closed",
            tags=["test"],
            notes="event-boundary-check",
        ),
    ]
    context = symbol.build_market_hours_context(
        trading_hours="eth",
        events=events,
        start_dt="2099-01-06 00:00:00",
        end_dt="2099-01-06 23:59:59",
    )

    for probe_dt in [
            "2099-01-06 11:59:00",
            "2099-01-06 12:00:00",
            "2099-01-06 12:30:00",
            "2099-01-06 13:00:00",
            "2099-01-06 13:01:00",
    ]:
        assert symbol.is_open_dt(target_dt=probe_dt, context=context) == (
            symbol.market_is_open(
                trading_hours="eth",
                target_dt=probe_dt,
                events=events,
            )
        )


def test_filter_open_datetimes_matches_market_is_open(symbol):
    """Verify bulk datetime filtering matches per-datetime checks."""
    events = [
        Event(
            start_dt="2099-01-05 12:00:00",
            end_dt="2099-01-05 13:00:00",
            symbol="ES",
            category="Closed",
            tags=["test"],
            notes="bulk-datetime-check",
        ),
    ]
    target_dts = [
        "2099-01-05 11:59:00",
        "2099-01-05 12:00:00",
        "2099-01-05 12:30:00",
        "2099-01-05 13:01:00",
        "2099-01-05 17:00:00",
        "2099-01-05 18:00:00",
    ]

    filtered = symbol.filter_open_datetimes(
        target_dts=target_dts,
        trading_hours="eth",
        events=events,
    )
    expected = [
        d for d in target_dts
        if symbol.market_is_open(
            trading_hours="eth",
            target_dt=d,
            events=events,
        )
    ]

    assert filtered == expected


def test_filter_open_candles_matches_market_is_open(symbol):
    """Verify bulk candle filtering matches per-candle checks."""

    class TestCandle:
        def __init__(self, c_datetime):
            self.c_datetime = c_datetime
            self.c_epoch = dt_to_epoch(c_datetime)

    events = [
        Event(
            start_dt="2099-01-05 12:00:00",
            end_dt="2099-01-05 13:00:00",
            symbol="ES",
            category="Closed",
            tags=["test"],
            notes="bulk-candle-check",
        ),
    ]
    candles = [
        TestCandle("2099-01-05 11:59:00"),
        TestCandle("2099-01-05 12:00:00"),
        TestCandle("2099-01-05 12:30:00"),
        TestCandle("2099-01-05 13:01:00"),
        TestCandle("2099-01-05 17:00:00"),
        TestCandle("2099-01-05 18:00:00"),
    ]

    filtered = symbol.filter_open_candles(
        candles=candles,
        trading_hours="eth",
        events=events,
    )
    expected = [
        c for c in candles
        if symbol.market_is_open(
            trading_hours="eth",
            target_dt=c.c_datetime,
            events=events,
        )
    ]

    assert [c.c_datetime for c in filtered] == [c.c_datetime for c in expected]


def test_Symbol_market_is_open(symbol):
    """Verify Symbol.market_is_open() for eth and rth across various times."""
    # ETH
    # Monday through Thursday
    for day in ["05", "06", "07", "08"]:
        date = f"2099-01-{day}"

        # Open at midnight
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00")
        # Open at 12:01am
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00")
        # Open at 9:29am
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00")
        # Open at 9:30am
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00")
        # Open at 4:00pm
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00")
        # Open at 4:59pm
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:59:00")
        # Closed at 5:00pm
        assert not symbol.market_is_open(trading_hours="eth",
                                         target_dt=f"{date} 17:00:00")
        # Closed at 5:59pm
        assert not symbol.market_is_open(trading_hours="eth",
                                         target_dt=f"{date} 17:59:00")
        # Open at 6:00pm
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00")
        # Open at 6:01pm
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00")
        # Open at 11:59pm
        assert symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00")
    # Friday
    date = "2099-01-09"
    # Open at midnight
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:00:00")
    # Open at 12:01am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:01:00")
    # Open at 9:29am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:29:00")
    # Open at 9:30am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:30:00")
    # Open at 4:00pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 16:00:00")
    # Open at 4:59pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 16:59:00")
    # Closed at 5:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:00:00")
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00")
    # Closed at 6:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00")
    # Closed at 6:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00")
    # Closed at 11:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00")

    # Saturday
    date = "2099-01-10"
    # Closed at midnight and midnight
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00")
    # Closed at 12:01am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00")
    # Closed at 9:29am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00")
    # Closed at 9:30am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00")
    # Closed at 4:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00")
    # Closed at 4:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:59:00")
    # Closed at 5:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:00:00")
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00")
    # Closed at 6:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00")
    # Closed at 6:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00")
    # Closed at 11:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00")

    # Sunday
    date = "2099-01-11"
    # Closed at midnight
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00")
    # Closed at 12:01am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00")
    # Closed at 9:29am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00")
    # Closed at 9:30am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00")
    # Closed at 4:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00")
    # Closed at 4:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:59:00")
    # Closed at 5:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:00:00")
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00")
    # Opend at 6:00pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:00:00")
    # Opend at 6:01pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:01:00")
    # Opend at 11:59pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 23:59:00")

    # RTH
    # Monday through Friday
    for day in ["05", "06", "07", "08", "09"]:
        date = f"2099-01-{day}"
        # Closed at midnight
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:00:00")
        # Closed at 12:01am
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:01:00")
        # Closed at 9:29am
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:29:00")
        # Open at 9:30am
        assert symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:30:00")
        # Open at 9:31am
        assert symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:31:00")
        # Open at 12:30pm
        assert symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 12:30:00")
        # Open at 3:59pm
        assert symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 15:59:00")
        # Closed at 4:00pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:00:00")
        # Closed at 4:01pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:01:00")
        # Closed at 5:59pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 17:59:00")
        # Closed at 6:00pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:00:00")
        # Closed at 6:01pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:01:00")
        # Closed at 11:59pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 23:59:00")

    # Saturday & Sunday
    for day in ["10", "11"]:
        date = f"2099-01-{day}"
        # Closed at midnight
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:00:00")
        # Closed at 12:01am
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:01:00")
        # Closed at 9:29am
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:29:00")
        # Closed at 9:30am
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:30:00")
        # Closed at 9:31am
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:31:00")
        # Closed at 12:30pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 12:30:00")
        # Closed at 3:59pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 15:59:00")
        # Closed at 4:00pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:00:00")
        # Closed at 4:01pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:01:00")
        # Closed at 5:59pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 17:59:00")
        # Closed at 6:00pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:00:00")
        # Closed at 6:01pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:01:00")
        # Closed at 11:59pm
        assert not symbol.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 23:59:00")

    # Holidays (will need to pass events for this borred from known)
    events = [Event(start_dt="2099-01-19 13:00:00",
                    end_dt="2099-01-19 17:59:00",
                    symbol="ES",
                    category="Closed",
                    tags=["holiday"],
                    notes="Presidents Day early close"),
              Event(start_dt="2099-01-22 17:00:00",
                    end_dt="2099-01-25 17:59:00",
                    symbol="ES",
                    category="Closed",
                    tags=["holiday"],
                    notes="Good Friday closed")
              ]

    # Check a full closure holiday for several times
    date = "2099-01-23"  # Good Friday

    # ETH
    # Closed at midnight
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00",
                                     events=events,
                                     )
    # Closed at 12:01am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00",
                                     events=events,
                                     )
    # Closed at 9:29am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00",
                                     events=events,
                                     )
    # Closed at 9:30am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00",
                                     events=events,
                                     )
    # Closed at 9:31am
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:31:00",
                                     events=events,
                                     )
    # Closed at 12:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 12:59:00",
                                     events=events,
                                     )
    # Closed at 1:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Closed at 6:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00",
                                     events=events,
                                     )
    # Closed at 6:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00",
                                     events=events,
                                     )
    # Closed at 11:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00",
                                     events=events,
                                     )

    # RTH
    # Closed at midnight
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:00:00",
                                     events=events,
                                     )
    # Closed at 12:01am
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:01:00",
                                     events=events,
                                     )
    # Closed at 9:29am
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:29:00",
                                     events=events,
                                     )
    # Closed at 9:30am
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:30:00",
                                     events=events,
                                     )
    # Closed at 9:31am
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:31:00",
                                     events=events,
                                     )
    # Closed at 12:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 12:59:00",
                                     events=events,
                                     )
    # Closed at 1:00pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Closed at 6:00pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:00:00",
                                     events=events,
                                     )
    # Closed at 6:01pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:01:00",
                                     events=events,
                                     )
    # Closed at 11:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 23:59:00",
                                     events=events,
                                     )

    # Check an early close holiday for several times
    date = "2099-01-19"

    # ETH
    # Open at midnight
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:00:00",
                                 events=events,
                                 )
    # Open at 12:01am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:01:00",
                                 events=events,
                                 )
    # Open at 9:29am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:29:00",
                                 events=events,
                                 )
    # Open at 9:30am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:30:00",
                                 events=events,
                                 )
    # Open at 9:31am
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:31:00",
                                 events=events,
                                 )
    # Open at 12:59pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 12:59:00",
                                 events=events,
                                 )
    # Closed at 1:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Open at 6:00pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:00:00",
                                 events=events,
                                 )
    # Open at 6:01pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:01:00",
                                 events=events,
                                 )
    # Open at 11:59pm
    assert symbol.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 23:59:00",
                                 events=events,
                                 )

    # RTH
    # Closed at midnight
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:00:00",
                                     events=events,
                                     )
    # Closed at 12:01am
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:01:00",
                                     events=events,
                                     )
    # Closed at 9:29am
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:29:00",
                                     events=events,
                                     )
    # Open at 9:30am
    assert symbol.market_is_open(trading_hours="rth",
                                 target_dt=f"{date} 09:30:00",
                                 events=events,
                                 )
    # Open at 9:31am
    assert symbol.market_is_open(trading_hours="rth",
                                 target_dt=f"{date} 09:31:00",
                                 events=events,
                                 )
    # Open at 12:59pm
    assert symbol.market_is_open(trading_hours="rth",
                                 target_dt=f"{date} 12:59:00",
                                 events=events,
                                 )
    # Closed at 1:00pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Closed at 6:00pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:00:00",
                                     events=events,
                                     )
    # Closed at 6:01pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:01:00",
                                     events=events,
                                     )
    # Closed at 11:59pm
    assert not symbol.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 23:59:00",
                                     events=events,
                                     )


def test_Symbol_get_market_boundary(symbol):
    """Verify Symbol.get_next/previous_open/close for eth and rth."""
    sym = symbol

    # Testing All boundaries mid-week Wednesday noon datetime
    # 2099-01-07 12:00:00.  This confirms non-weekend mechanics are working
    t = dt_as_dt("2099-01-07 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth")) == "2099-01-07 18:00:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth")) == "2099-01-07 16:59:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth")) == "2099-01-06 18:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth")) == "2099-01-06 16:59:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth")) == "2099-01-08 09:30:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth")) == "2099-01-07 16:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2099-01-07 09:30:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2099-01-06 16:00:00"

    # Testing Next boundaries from Thursday noon 2099-01-08 12:00:00
    # (should hit Thursday/Friday)")
    # Confirms we don't slip into or over the weekend due to miscalculations
    t = dt_as_dt("2099-01-08 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth")) == "2099-01-08 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth")) == "2099-01-09 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth")) == "2099-01-08 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth")) == "2099-01-08 16:00:00"

    # Testing Next boundaries from Friday noon 2099-01-09 12:00:00
    # (should hit Sunday/Monday)
    # This confirms we span the weekend as expected when appropriate
    t = dt_as_dt("2099-01-09 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth")) == "2099-01-11 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth")) == "2099-01-12 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth")) == "2099-01-09 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth")) == "2099-01-09 16:00:00"

    # Testing Previous boundaries from Tuesday noon 2099-01-06 12:00:00
    # (should hit Monday/Tuesday)
    # Confirms we don't slip into or over the weekend due to miscalculations
    t = dt_as_dt("2099-01-06 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth")) == "2099-01-05 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2099-01-06 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth")) == "2099-01-05 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2099-01-05 16:00:00"

    # Testing Previous boundaries from Monday noon 2099-01-05 12:00:00
    # (should hit Friday/Sunday)
    # This confirms we span the weekend as expected when appropriate
    t = dt_as_dt("2099-01-05 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth")) == "2099-01-04 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2099-01-05 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth")) == "2099-01-02 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2099-01-02 16:00:00"

    # Setting up a few events to test that boundary mechanics respect
    events = [Event(start_dt="2099-02-12 17:00:00",
                    end_dt="2099-02-15 17:59:00",
                    symbol="ES",
                    category="Closed",
                    notes="Good Friday Closed",
                    ),
              Event(start_dt="2099-02-23 00:00:00",
                    end_dt="2099-02-24 23:59:00",
                    symbol="ES",
                    category="Closed",
                    notes="Tues-Wed Full days closure",
                    ),
              Event(start_dt="2099-02-23 13:00:00",
                    end_dt="2099-02-23 17:59:00",
                    symbol="ES",
                    category="Closed",
                    notes="Tues early closure",
                    ),
              ]

    # Testing Next against Good Friday closure running Thursday
    # 2099-02-12 17:00:00 through Sunday 2099-02-15 17:59:00

    # Checking from noon on Thursday 2099-02-12 12:00:00
    # This confirms we cross the event and weekend where appropriate.
    t = dt_as_dt("2099-02-12 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-15 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-16 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-12 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-12 16:00:00"

    # Testing same closure window from within using Friday at Noon
    # 2099-02-13 12:00:00
    # Confirms times inside a closure are moved outside of it in both direction
    t = dt_as_dt("2099-02-13 12:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-16 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-16 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-12 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-12 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-15 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-16 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-11 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-12 09:30:00"

    # Testing same closure window from the following Monday at Noon
    # 2099-02-16 12:00:00
    # This confirms Previous crosses the event to the prior week.
    t = dt_as_dt("2099-02-16 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-15 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-16 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-12 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-12 16:00:00"
    # Testing nested closure window from within both using
    # 2099-02-23 14:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2099-02-23 14:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-25 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-25 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-20 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-20 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-25 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-25 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-22 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-20 09:30:00"

    # Testing nested closure window from within outer only using
    # 2099-02-23 10:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2099-02-23 10:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-25 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-25 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-20 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-20 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-25 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-25 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-22 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-20 09:30:00"

    # Testing nested closure window from within outer only using
    # 2099-02-24 06:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2099-02-24 06:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-25 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-25 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-20 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-20 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-25 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-25 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2099-02-22 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2099-02-20 09:30:00"


def test_Symbol_get_next_tick_up(symbol):
    """Verify Symbol.get_next_tick_up() rounds to tick_size."""
    sym = symbol  # ES with tick_size=0.25

    # Test exact tick boundary - should return same value
    assert sym.get_next_tick_up(100.0) == 100.0
    assert sym.get_next_tick_up(100.25) == 100.25
    assert sym.get_next_tick_up(100.5) == 100.5
    assert sym.get_next_tick_up(100.75) == 100.75

    # Test rounding up to next tick
    assert sym.get_next_tick_up(100.01) == 100.25
    assert sym.get_next_tick_up(100.1) == 100.25
    assert sym.get_next_tick_up(100.2) == 100.25
    assert sym.get_next_tick_up(100.24) == 100.25

    # Test rounding across multiples
    assert sym.get_next_tick_up(100.26) == 100.5
    assert sym.get_next_tick_up(100.51) == 100.75
    assert sym.get_next_tick_up(100.76) == 101.0

    # Test negative values
    assert sym.get_next_tick_up(-100.0) == -100.0
    assert sym.get_next_tick_up(-100.1) == -100.0
    assert sym.get_next_tick_up(-100.24) == -100.0
    assert sym.get_next_tick_up(-100.26) == -100.25

    # Test values near zero
    assert sym.get_next_tick_up(0.0) == 0.0
    assert sym.get_next_tick_up(0.01) == 0.25
    assert sym.get_next_tick_up(0.24) == 0.25
    assert sym.get_next_tick_up(-0.01) == 0.0
    assert sym.get_next_tick_up(-0.24) == 0.0

    # Test decimal precision preservation (should round to 2 decimals)
    assert sym.get_next_tick_up(4987.23) == 4987.25
    assert sym.get_next_tick_up(1000.01) == 1000.25

    # Test large values
    assert sym.get_next_tick_up(9999.99) == 10000.0
    assert sym.get_next_tick_up(5000.01) == 5000.25


def test_Symbol_get_next_tick_down(symbol):
    """Verify Symbol.get_next_tick_down() rounds to tick_size."""
    """Test get_next_tick_down() rounds down to nearest 0.25 (ES tick size)"""
    sym = symbol  # ES with tick_size=0.25

    # Test exact tick boundary - should return same value
    assert sym.get_next_tick_down(100.0) == 100.0
    assert sym.get_next_tick_down(100.25) == 100.25
    assert sym.get_next_tick_down(100.5) == 100.5
    assert sym.get_next_tick_down(100.75) == 100.75

    # Test rounding down to previous tick
    assert sym.get_next_tick_down(100.01) == 100.0
    assert sym.get_next_tick_down(100.1) == 100.0
    assert sym.get_next_tick_down(100.2) == 100.0
    assert sym.get_next_tick_down(100.24) == 100.0

    # Test rounding across multiples
    assert sym.get_next_tick_down(100.26) == 100.25
    assert sym.get_next_tick_down(100.51) == 100.5
    assert sym.get_next_tick_down(100.76) == 100.75

    # Test negative values
    assert sym.get_next_tick_down(-100.0) == -100.0
    assert sym.get_next_tick_down(-100.1) == -100.25
    assert sym.get_next_tick_down(-100.24) == -100.25
    assert sym.get_next_tick_down(-100.26) == -100.5

    # Test values near zero
    assert sym.get_next_tick_down(0.0) == 0.0
    assert sym.get_next_tick_down(0.01) == 0.0
    assert sym.get_next_tick_down(0.24) == 0.0
    assert sym.get_next_tick_down(-0.01) == -0.25
    assert sym.get_next_tick_down(-0.24) == -0.25

    # Test decimal precision preservation (should round to 2 decimals)
    assert sym.get_next_tick_down(4987.23) == 4987.0
    assert sym.get_next_tick_down(1000.01) == 1000.0

    # Test large values
    assert sym.get_next_tick_down(9999.99) == 9999.75
    assert sym.get_next_tick_down(5000.01) == 5000.0


def test_Symbol_tick_methods_roundtrip(symbol):
    """Verify get_next_tick_up and down roundtrip correctly."""
    sym = symbol

    # Test that values already on boundaries stay the same
    for tick in [0.0, 0.25, 0.5, 0.75, 1.0, 100.25, 5000.75]:
        assert sym.get_next_tick_up(tick) == tick
        assert sym.get_next_tick_down(tick) == tick

    # Test that rounding up then down from a boundary returns boundary
    value = 100.1
    rounded_up = sym.get_next_tick_up(value)
    assert rounded_up == 100.25
    # Rounding down an exact boundary stays at that boundary
    rounded_down = sym.get_next_tick_down(rounded_up)
    assert rounded_down == 100.25

    # Test that rounding involves correct nearest boundaries
    # 100.1 should round up to 100.25, and down to 100.0
    rounded_down_direct = sym.get_next_tick_down(100.1)
    assert rounded_down_direct == 100.0

    # 100.6 should round down to 100.5, round up stays at 100.75
    value = 100.6
    rounded_down = sym.get_next_tick_down(value)
    assert rounded_down == 100.5
    rounded_up = sym.get_next_tick_up(value)
    assert rounded_up == 100.75

    # Test idempotent property: rounding twice returns same value
    test_values = [100.1, 100.5, 100.99, 50.23, -100.15]
    for val in test_values:
        # Rounding up twice should equal rounding up once
        up_once = sym.get_next_tick_up(val)
        up_twice = sym.get_next_tick_up(up_once)
        assert up_once == up_twice
        # Same for rounding down
        down_once = sym.get_next_tick_down(val)
        down_twice = sym.get_next_tick_down(down_once)
        assert down_once == down_twice


def test_Symbol_get_era(symbol):
    """Verify Symbol.get_era() identifies correct market hour eras."""
    sym = symbol
    import datetime as dt

    # Test Era 1: 2008_thru_2012 (2008-01-01 to 2012-11-16)
    era = sym.get_era("2008-01-01 12:00:00")
    assert era["name"] == "2008_thru_2012"
    assert era["times"]["eth_close"] == dt.time(17, 29, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)

    assert "2008_thru_2012" == sym.get_era("2010-06-15 12:00:00")["name"]
    assert "2008_thru_2012" == sym.get_era("2012-11-16 12:00:00")["name"]

    # Test Era 2: 2012holidays_thru_2015holidays (2012-11-17 to 2015-09-18)
    era = sym.get_era("2012-11-17 12:00:00")
    assert era["name"] == "2012holidays_thru_2015holidays"
    assert era["times"]["eth_close"] == dt.time(17, 15, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure on weekdays
    assert era["closed_hours"]["eth"][0] == [
        {"close": "16:15:00", "open": "16:30:00"},
        {"close": "17:16:00", "open": "17:59:59"}
    ]

    era = sym.get_era("2012-11-18 12:00:00")
    assert era["name"] == "2012holidays_thru_2015holidays"
    era = sym.get_era("2015-09-18 12:00:00")
    assert era["name"] == "2012holidays_thru_2015holidays"

    # Test Era 3: 2015holidays_thru_2020 (2015-09-19 to 2020-12-31)
    era = sym.get_era("2015-09-19 12:00:00")
    assert era["name"] == "2015holidays_thru_2020"
    assert era["times"]["eth_close"] == dt.time(16, 59, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure on weekdays
    assert era["closed_hours"]["eth"][0] == [
        {"close": "16:15:00", "open": "16:30:00"},
        {"close": "17:00:00", "open": "17:59:59"}
    ]

    era = sym.get_era("2018-06-15 12:00:00")
    assert era["name"] == "2015holidays_thru_2020"
    era = sym.get_era("2020-12-31 12:00:00")
    assert era["name"] == "2015holidays_thru_2020"

    # Test Era 4: 2021-01_thru_2021-06 (2021-01-01 to 2021-06-25)
    era = sym.get_era("2021-01-01 12:00:00")
    assert era["name"] == "2021-01_thru_2021-06"
    assert era["times"]["eth_close"] == dt.time(16, 59, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure on weekdays (WITH weekday closure)
    assert era["closed_hours"]["eth"][0] == [
        {"close": "16:15:00", "open": "16:30:00"},
        {"close": "17:00:00", "open": "17:59:59"}
    ]

    assert "2021-01_thru_2021-06" == sym.get_era("2021-03-15 12:00:00")["name"]
    assert "2021-01_thru_2021-06" == sym.get_era("2021-06-25 12:00:00")["name"]

    # Test Era 5: 2021-06_thru_present (2021-06-26 onwards)
    # Critical boundary: 2021-06-26 is where 16:15-16:29 closure was removed
    era = sym.get_era("2021-06-26 12:00:00")
    assert era["name"] == "2021-06_thru_present"
    assert era["times"]["eth_close"] == dt.time(16, 59, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure REMOVED (only 17:00-17:59:59 close)
    assert era["closed_hours"]["eth"][0] == [
        {"close": "17:00:00", "open": "17:59:59"}
    ]

    assert "2021-06_thru_present" == sym.get_era("2021-09-15 12:00:00")["name"]
    assert "2099_test_era" == sym.get_era("2099-06-15 12:00:00")["name"]
    assert "2021-06_thru_present" == sym.get_era("2025-12-31 12:00:00")["name"]

    # Test with datetime objects (not just strings)
    era = sym.get_era(dt.datetime(2010, 5, 1))
    assert era["name"] == "2008_thru_2012"

    era = sym.get_era(dt.datetime(2013, 6, 1))
    assert era["name"] == "2012holidays_thru_2015holidays"

    era = sym.get_era(dt.datetime(2018, 3, 1))
    assert era["name"] == "2015holidays_thru_2020"

    era = sym.get_era(dt.datetime(2021, 3, 1))
    assert era["name"] == "2021-01_thru_2021-06"

    era = sym.get_era(dt.datetime(2099, 5, 1))
    assert era["name"] == "2099_test_era"

    # Test error for date before any era
    with pytest.raises(ValueError, match="No market era defined"):
        sym.get_era("2007-12-31 12:00:00")


def test_Symbol_get_times_for_era(symbol):
    """Verify Symbol.get_times_for_era() returns correct hours."""
    sym = symbol
    import datetime as dt

    # Test 2008_thru_2012 era
    times = sym.get_times_for_era("2008_thru_2012")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(17, 29, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2012holidays_thru_2015holidays era
    times = sym.get_times_for_era("2012holidays_thru_2015holidays")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(17, 15, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2015holidays_thru_2020 era
    times = sym.get_times_for_era("2015holidays_thru_2020")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(16, 59, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2021-01_thru_2021-06 era (WITH 16:15-16:30 closure)
    times = sym.get_times_for_era("2021-01_thru_2021-06")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(16, 59, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2021-06_thru_present era (latest, no 16:15-16:30 closure)
    times = sym.get_times_for_era("2021-06_thru_present")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(16, 59, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test by era dict (2012holidays_thru_2015holidays)
    era = sym.get_era("2014-06-15 12:00:00")
    times = sym.get_times_for_era(era)
    assert times["eth_close"] == dt.time(17, 15, 0)

    # Test error for unknown era name
    with pytest.raises(ValueError, match="Unknown era name"):
        sym.get_times_for_era("nonexistent_era")


def test_Symbol_get_closed_hours_for_era(symbol):
    """Verify Symbol.get_closed_hours_for_era() returns correct gaps."""
    sym = symbol
    import datetime as dt

    # Test 2008_thru_2012 era
    closed = sym.get_closed_hours_for_era("2010-06-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 16, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 30, 0)

    # Test 2012holidays_thru_2015holidays era
    closed = sym.get_closed_hours_for_era("2014-06-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 15, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 16, 0)
    assert closed[0][1]["open"] == dt.time(17, 59, 59)

    # Test 2015holidays_thru_2020 era
    closed = sym.get_closed_hours_for_era("2018-06-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 15, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 0, 0)

    # Test 2021-01_thru_2021-06 era (WITH 16:15-16:30 closure)
    closed = sym.get_closed_hours_for_era("2021-03-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 15, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 0, 0)

    # Test 2021-06_thru_present era (NO 16:15-16:30 closure)
    closed = sym.get_closed_hours_for_era("2099-06-15 12:00:00", "eth")
    assert len(closed[0]) == 1
    assert closed[0][0]["close"] == dt.time(17, 0, 0)

    # Verify RTH closed consistently across all eras at 16:00
    closed2008 = sym.get_closed_hours_for_era(
        "2010-06-15 12:00:00", "rth")
    closed2099 = sym.get_closed_hours_for_era(
        "2099-06-15 12:00:00", "rth")
    assert closed2008[0][1]["close"] == dt.time(16, 0, 0)
    assert closed2099[0][1]["close"] == dt.time(16, 0, 0)

    # Test caching - call twice and verify same object returned
    closed1 = sym.get_closed_hours_for_era("2099-06-15 12:00:00", "eth")
    closed2 = sym.get_closed_hours_for_era("2099-06-20 12:00:00", "eth")
    assert closed1 is closed2  # Should be same cached object

    # Different era should return different object
    closed3 = sym.get_closed_hours_for_era("2010-06-15 12:00:00", "eth")
    assert closed1 is not closed3

    # Test error for invalid trading_hours
    with pytest.raises(ValueError, match="trading_hours.*not defined"):
        sym.get_closed_hours_for_era("2099-06-15 12:00:00", "invalid")


def test_Symbol_market_is_open_historical_eras(symbol):
    """Verify Symbol.market_is_open() for historical hour changes."""
    sym = symbol

    # Test 2008-2012 era
    date = "2010-03-10"
    assert sym.market_is_open(trading_hours="rth",
                              target_dt=f"{date} 15:59:00",
                              check_closed_events=False)
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt=f"{date} 16:00:00",
                                  check_closed_events=False)
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 16:16:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:30:00",
                              check_closed_events=False)

    # Test 2015holidays_thru_2020 era (same 16:15 closure)
    date = "2018-03-14"
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 16:15:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:30:00",
                              check_closed_events=False)

    # Test 2021-01_thru_2021-06 era (HAS 16:15-16:30 closure)
    date = "2021-03-10"
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 16:15:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:30:00",
                              check_closed_events=False)

    # Critical transition: 2021-06-21 (with closure) vs 2021-06-28 (no)
    # Both are Mondays (day_of_week=0) for fair comparison
    # 2021-06-21 is last Monday of 2021-01_thru_2021-06 era
    date_with = "2021-06-21"
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date_with} 16:15:00",
                                  check_closed_events=False)

    # 2021-06-28 is first Monday of 2021-06_thru_present era
    date_without = "2021-06-28"
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date_without} 16:15:00",
                              check_closed_events=False)

    # Test 2021-06_thru_present era (NO 16:15-16:30 closure)
    date = "2099-03-13"
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt=f"{date} 16:00:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="rth",
                              target_dt=f"{date} 15:59:00",
                              check_closed_events=False)
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 17:00:00",
                                  check_closed_events=False)
    # ETH should be open at 4:15pm in current era (no closure)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:15:00",
                              check_closed_events=False)

    # ETH should be open at 4:15pm in current era (no closure)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:15:00",
                              check_closed_events=False)


def test_Symbol_get_market_boundary_historical_eras(symbol):
    """Verify Symbol boundary methods for historical hour changes."""
    sym = symbol

    # Test 2008-2012 era: RTH closes at 16:00
    # Checking from noon on Wednesday 2010-03-10 12:00:00
    t = dt_as_dt("2010-03-10 12:00:00")

    # Next RTH Close should be at 16:00
    result = sym.get_next_close(target_dt=t,
                                trading_hours="rth",
                                adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-10 16:00:00"

    # Previous RTH Close should be at 16:00 previous day
    result = sym.get_previous_close(target_dt=t,
                                    trading_hours="rth",
                                    adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-09 16:00:00"

    # Test 2008-2012 era: ETH closes at 17:29
    # Next ETH Close should be at 17:29 (not 16:59 like current era)
    result = sym.get_next_close(target_dt=t,
                                trading_hours="eth",
                                adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-10 17:29:00"

    # Previous ETH Close should be at 17:29 previous day
    result = sym.get_previous_close(target_dt=t,
                                    trading_hours="eth",
                                    adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-09 17:29:00"

    # Test era transition: date right before change (2012-11-16 is Friday)
    t = dt_as_dt("2012-11-16 12:00:00")
    result = sym.get_next_close(target_dt=t,
                                trading_hours="rth",
                                adjust_for_events=False)
    # Should use old era times (16:00)
    assert dt_as_str(result) == "2012-11-16 16:00:00"

    # Test era transition: Monday after change (2012-11-19 is Monday,
    # 11/17 is Saturday, 11/18 is Sunday)
    # Use Monday to avoid weekend complications
    t = dt_as_dt("2012-11-19 12:00:00")
    result = sym.get_next_close(target_dt=t,
                                trading_hours="rth",
                                adjust_for_events=False)
    # Should use new era times (16:00, same as previous era)
    assert dt_as_str(result) == "2012-11-19 16:00:00"

    # Test era transition: RTH open stays the same across eras
    t1 = dt_as_dt("2012-11-16 08:00:00")
    result1 = sym.get_next_open(target_dt=t1,
                                trading_hours="rth",
                                adjust_for_events=False)
    assert dt_as_str(result1) == "2012-11-16 09:30:00"

    t2 = dt_as_dt("2012-11-19 08:00:00")  # Monday after era change
    result2 = sym.get_next_open(target_dt=t2,
                                trading_hours="rth",
                                adjust_for_events=False)
    # RTH open unchanged across eras (both 9:30)
    assert dt_as_str(result2) == "2012-11-19 09:30:00"


def test_Symbol_init(symbol):
    """Verify Symbol initialization and attributes."""
    sym = symbol

    assert sym.ticker == "ES"
    assert sym.name == "ES"
    assert sym.leverage_ratio == 50.0
    assert sym.tick_size == 0.25

    # Verify set_times() was called during init
    assert hasattr(sym, "eth_open_time")
    assert hasattr(sym, "eth_close_time")
    assert hasattr(sym, "rth_open_time")
    assert hasattr(sym, "rth_close_time")
    assert hasattr(sym, "eth_week_open")
    assert hasattr(sym, "eth_week_close")
    assert hasattr(sym, "rth_week_open")
    assert hasattr(sym, "rth_week_close")
    assert hasattr(sym, "_closed_hours_cache")

    # Test that leverage_ratio and tick_size are converted to float
    # and alternate ticker and name values apply correctly
    sym2 = Symbol(ticker="DELETEME", name="Test",
                  leverage_ratio="100", tick_size="1.5")
    assert isinstance(sym2.leverage_ratio, float)
    assert sym2.leverage_ratio == 100.0
    assert isinstance(sym2.tick_size, float)
    assert sym2.tick_size == 1.5
    assert sym2.ticker == "DELETEME"
    assert sym2.name == "Test"


def test_Symbol_equality(symbol):
    """Verify Symbol equality comparison."""
    sym1 = symbol
    sym2 = Symbol(ticker="ES", name="ES",
                  leverage_ratio=50, tick_size=0.25)
    sym3 = Symbol(ticker="ES", name="ES",
                  leverage_ratio=50, tick_size=0.5)
    sym4 = Symbol(ticker="DELETEME", name="DELETEME",
                  leverage_ratio=20, tick_size=0.25)

    # Test __eq__
    assert sym1 == sym2
    assert not (sym1 == sym3)
    assert not (sym1 == sym4)

    # Test __ne__
    assert not (sym1 != sym2)
    assert sym1 != sym3
    assert sym1 != sym4

    # Test different attributes affect equality
    sym5 = Symbol(ticker="ES", name="Different Name",
                  leverage_ratio=50, tick_size=0.25)
    assert sym1 != sym5


def test_Symbol_string_representations(symbol):
    """Verify Symbol.__str__, __repr__, and pretty() methods."""
    sym = symbol

    # Test __str__ returns a string
    str_result = str(sym)
    assert isinstance(str_result, str)
    assert "ticker" in str_result
    assert "ES" in str_result

    # Test __repr__ returns a string
    repr_result = repr(sym)
    assert isinstance(repr_result, str)
    assert "ticker" in repr_result

    # Test __str__ and __repr__ are the same
    assert str_result == repr_result

    # Test pretty() returns formatted JSON string
    pretty_result = sym.pretty()
    assert isinstance(pretty_result, str)
    # Should have indentation (newlines and spaces)
    assert "\n" in pretty_result
    assert "    " in pretty_result
    # Should contain key attributes
    assert '"ticker"' in pretty_result
    assert '"ES"' in pretty_result
    assert '"leverage_ratio"' in pretty_result
    assert '"tick_size"' in pretty_result


def test_Symbol_serialization(symbol):
    """Verify Symbol to_json() and to_clean_dict() methods."""
    import json
    sym = symbol

    # Test to_json returns valid JSON string
    json_str = sym.to_json()
    assert isinstance(json_str, str)

    # Should be valid JSON
    parsed = json.loads(json_str)
    assert isinstance(parsed, dict)
    assert parsed["ticker"] == "ES"
    assert parsed["name"] == "ES"
    assert parsed["leverage_ratio"] == 50.0
    assert parsed["tick_size"] == 0.25

    # Time attributes should be strings
    assert isinstance(parsed["eth_open_time"], str)
    assert isinstance(parsed["eth_close_time"], str)
    assert isinstance(parsed["rth_open_time"], str)
    assert isinstance(parsed["rth_close_time"], str)

    # Test to_clean_dict returns dict
    clean_dict = sym.to_clean_dict()
    assert isinstance(clean_dict, dict)
    assert clean_dict["ticker"] == "ES"
    assert clean_dict["leverage_ratio"] == 50.0

    # Should match parsed JSON
    assert clean_dict == parsed


def test_Symbol_set_times(symbol):
    """Verify Symbol.set_times() updates trading hours correctly."""
    import datetime as dt
    sym = symbol

    # Should use latest era times (2021-06_thru_present)
    assert sym.eth_open_time == dt.time(18, 0, 0)
    assert sym.eth_close_time == dt.time(16, 59, 0)
    assert sym.rth_open_time == dt.time(9, 30, 0)
    assert sym.rth_close_time == dt.time(16, 0, 0)

    # Check week schedules
    assert sym.eth_week_open["day_of_week"] == 6  # Sunday
    assert sym.eth_week_open["time"] == dt.time(18, 0, 0)
    assert sym.eth_week_close["day_of_week"] == 4  # Friday
    assert sym.eth_week_close["time"] == dt.time(16, 59, 0)

    assert sym.rth_week_open["day_of_week"] == 0  # Monday
    assert sym.rth_week_open["time"] == dt.time(9, 30, 0)
    assert sym.rth_week_close["day_of_week"] == 4  # Friday
    assert sym.rth_week_close["time"] == dt.time(16, 0, 0)

    # Test unknown ticker raises error
    with pytest.raises(ValueError, match="times have not yet been defined"):
        Symbol(ticker="UNKNOWN", name="Unknown",
               leverage_ratio=1, tick_size=0.01)


def test_Symbol_create_and_verify_common_methods(symbol):
    """Test Symbol __init__ values, __eq__, __ne__, __str__, __repr__,
    to_clean_dict, to_json, and pretty.

    Symbol does not define brief.
    """
    sym = symbol
    assert isinstance(sym, Symbol)
    sym2 = Symbol(ticker="ES", name="ES",
                  leverage_ratio=50, tick_size=0.25)
    diff = Symbol(ticker="ES", name="ES",
                  leverage_ratio=50, tick_size=0.5)
    # __init__
    assert sym.ticker == "ES"
    assert sym.name == "ES"
    assert sym.leverage_ratio == 50.0
    assert sym.tick_size == 0.25
    assert sym.eth_open_time == dt.time(18, 0)
    assert sym.eth_close_time == dt.time(16, 59)
    assert sym.rth_open_time == dt.time(9, 30)
    assert sym.rth_close_time == dt.time(16, 0)
    assert sym.eth_week_open == {
        "day_of_week": 6, "time": dt.time(18, 0)}
    assert sym.eth_week_close == {
        "day_of_week": 4, "time": dt.time(16, 59)}
    assert sym.rth_week_open == {
        "day_of_week": 0, "time": dt.time(9, 30)}
    assert sym.rth_week_close == {
        "day_of_week": 4, "time": dt.time(16, 0)}
    assert sym._closed_hours_cache == {}
    expected_attrs = {
        "_closed_hours_cache", "eth_close_time", "eth_open_time",
        "eth_week_close", "eth_week_open", "leverage_ratio", "name",
        "rth_close_time", "rth_open_time", "rth_week_close",
        "rth_week_open", "tick_size", "ticker",
    }
    actual_attrs = set(vars(sym).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "Symbol attributes changed. Update this test's "
        "__init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    # __eq__
    assert sym == sym2
    assert not (sym == diff)
    # __ne__
    assert not (sym != sym2)
    assert sym != diff
    # __str__
    assert isinstance(str(sym), str)
    assert len(str(sym)) > 0
    # __repr__
    assert isinstance(repr(sym), str)
    assert str(sym) == repr(sym)
    # to_clean_dict
    d = sym.to_clean_dict()
    assert isinstance(d, dict)
    assert d["ticker"] == "ES"
    assert d["name"] == "ES"
    assert d["leverage_ratio"] == 50.0
    assert d["tick_size"] == 0.25
    # to_json
    j = sym.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["ticker"] == "ES"
    assert parsed["leverage_ratio"] == 50.0
    # pretty
    p = sym.pretty()
    assert isinstance(p, str)
    assert "\n" in p
    assert '"ticker"' in p
    assert '"ES"' in p
