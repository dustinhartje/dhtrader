"""Tests for session-end aware aggregate candle completeness."""
from datetime import datetime, timedelta

import pytest

from dhtrader import Event, Symbol
from dhtrader.dhcommon import (
    candle_is_complete,
    expected_open_minutes_in_candle,
    last_open_minute_in_candle,
    next_candle_boundary,
)


@pytest.fixture
def symbol():
    """Return a representative ES futures symbol without storage access."""
    return Symbol(ticker="ES", name="ES", leverage_ratio=50, tick_size=0.25)


class OpenMinuteSymbol:
    """Symbol fixture whose open minutes are supplied by a predicate."""

    def __init__(self, is_open):
        self.is_open = is_open
        self.context_build_count = 0

    def build_market_hours_context(self, **kwargs):
        """Return the requested bounds as a test context."""
        self.context_build_count += 1
        return kwargs

    def is_open_dt(self, target_dt, context):
        """Return whether the fixture declares this minute open."""
        return self.is_open(target_dt)


def test_last_open_minute_normal_weekday_e1d(symbol):
    """A normal ETH daily session ends on its expected final minute."""

    actual = last_open_minute_in_candle(
        "2026-09-10 18:00:00",
        "e1d",
        symbol,
        "eth",
    )

    assert actual == datetime(2026, 9, 11, 16, 59)


def test_last_open_minute_sunday_open_e1d(symbol):
    """Sunday's ETH session starts at 18:00 and ends Monday afternoon."""
    actual = last_open_minute_in_candle(
        "2026-09-13 18:00:00",
        "e1d",
        symbol,
        "eth",
    )

    assert actual == datetime(2026, 9, 14, 16, 59)


def test_last_open_minute_friday_session_e1d(symbol):
    """Thursday's daily candle reaches Friday's normal session end."""
    actual = last_open_minute_in_candle(
        "2026-09-10 18:00:00",
        "e1d",
        symbol,
        "eth",
    )

    assert actual == datetime(2026, 9, 11, 16, 59)


def test_last_open_minute_returns_none_for_closed_candle():
    """A completely closed candle can never be sealed."""
    symbol = OpenMinuteSymbol(lambda _: False)

    actual = last_open_minute_in_candle(
        "2026-09-11 16:55:00",
        "5m",
        symbol,
        "eth",
    )

    assert actual is None


@pytest.mark.parametrize(
    ("candle_start", "event_end", "expected"),
    [
        (
            "2026-11-26 18:00:00",
            "2026-11-27 17:00:00",
            "2026-11-27 13:14:00",
        ),
        (
            "2026-12-23 18:00:00",
            "2026-12-24 17:00:00",
            "2026-12-24 13:14:00",
        ),
    ],
)
def test_last_open_minute_respects_early_close(
        symbol, candle_start, event_end, expected):
    """An event beginning at 13:15 leaves 13:14 as the last open minute."""
    event = Event(
        start_dt=f"{expected[:10]} 13:15:00",
        end_dt=event_end,
        symbol=symbol,
        category="Closed",
    )

    actual = last_open_minute_in_candle(
        candle_start,
        "e1d",
        symbol,
        "eth",
        events=[event],
    )

    assert actual == datetime.fromisoformat(expected)


def test_last_open_minute_returns_none_for_full_holiday(symbol):
    """A fully closed daily session has no minute that permits sealing."""
    event = Event(
        start_dt="2026-12-24 18:00:00",
        end_dt="2026-12-25 17:59:00",
        symbol=symbol,
        category="Closed",
    )

    actual = last_open_minute_in_candle(
        "2026-12-24 18:00:00",
        "e1d",
        symbol,
        "eth",
        events=[event],
    )

    assert actual is None


def test_candle_is_complete_requires_final_open_minute():
    """One minute before the final open minute is insufficient to seal."""
    symbol = OpenMinuteSymbol(lambda _: True)
    candle_start = "2026-09-11 16:55:00"

    assert not candle_is_complete(
        candle_start,
        "5m",
        "2026-09-11 16:58:00",
        symbol,
        "eth",
    )
    assert candle_is_complete(
        candle_start,
        "5m",
        "2026-09-11 16:59:00",
        symbol,
        "eth",
    )


def test_candle_is_complete_returns_false_for_fully_closed_candle(symbol):
    """No latest timestamp can seal a candle without an open minute."""
    event = Event(
        start_dt="2026-12-24 18:00:00",
        end_dt="2026-12-25 17:59:00",
        symbol=symbol,
        category="Closed",
    )

    assert not candle_is_complete(
        "2026-12-24 18:00:00",
        "e1d",
        "2026-12-25 17:59:00",
        symbol,
        "eth",
        events=[event],
    )


@pytest.mark.parametrize(
    ("timeframe", "candle_start", "expected"),
    [
        ("e1w", "2026-09-06 18:00:00", "2026-09-11 16:59:00"),
        ("e1mo", "2026-08-31 18:00:00", "2026-09-30 16:59:00"),
        ("e1y", "2026-01-01 18:00:00", "2027-01-01 16:59:00"),
        ("e1h", "2026-09-11 15:00:00", "2026-09-11 15:59:00"),
        ("5m", "2026-09-11 16:55:00", "2026-09-11 16:59:00"),
    ],
)
def test_last_open_minute_uses_calendar_aware_boundaries(
        symbol, timeframe, candle_start, expected):
    """Weekly through intraday candles end on their final expected minute."""
    actual = last_open_minute_in_candle(
        candle_start,
        timeframe,
        symbol,
        "eth",
    )

    assert actual == datetime.fromisoformat(expected)


def test_last_open_minute_month_end_on_weekend(symbol):
    """A weekend calendar boundary resolves to the prior Friday close."""
    actual = last_open_minute_in_candle(
        "2025-12-31 18:00:00",
        "e1mo",
        symbol,
        "eth",
    )

    assert actual == datetime(2026, 1, 30, 16, 59)


def test_last_open_minute_holiday_shortened_week(symbol):
    """A Friday closure causes the weekly candle to end Thursday instead."""
    event = Event(
        start_dt="2026-11-27 13:15:00",
        end_dt="2026-11-27 17:59:00",
        symbol=symbol,
        category="Closed",
    )

    actual = last_open_minute_in_candle(
        "2026-11-22 18:00:00",
        "e1w",
        symbol,
        "eth",
        events=[event],
    )

    assert actual == datetime(2026, 11, 27, 13, 14)


def test_midsession_closure_does_not_truncate_candle_end(symbol):
    """A closure that reopens before end of day leaves the end unchanged."""
    event = Event(
        start_dt="2008-09-18 11:00:00",
        end_dt="2008-09-18 12:00:00",
        symbol=symbol,
        category="Closed",
    )

    actual = last_open_minute_in_candle(
        "2008-09-17 18:00:00",
        "e1d",
        symbol,
        "eth",
        events=[event],
    )

    assert actual == datetime(2008, 9, 18, 17, 29)


def test_missing_early_close_event_does_not_seal_early(symbol):
    """Absent closure data retains the later expected minute requirement."""
    assert not candle_is_complete(
        "2026-11-26 18:00:00",
        "e1d",
        "2026-11-27 13:14:00",
        symbol,
        "eth",
    )


@pytest.mark.parametrize(
    "candle_start",
    ["2026-03-08 18:00:00", "2026-11-01 18:00:00"],
)
def test_last_open_minute_preserves_session_end_through_dst(
        symbol, candle_start):
    """Spring and fall DST transitions retain the expected ETH close."""
    actual = last_open_minute_in_candle(
        candle_start,
        "e1d",
        symbol,
        "eth",
    )

    assert actual == datetime.fromisoformat(candle_start) + timedelta(
        hours=22,
        minutes=59,
    )


def test_last_open_minute_is_open_and_within_open_2026_daily_candles(symbol):
    """Every 2026 sealing candidate resolves to an in-candle open minute."""
    candle_start = datetime(2026, 1, 4, 18)
    final_candle_start = datetime(2027, 1, 3, 18)
    while candle_start < final_candle_start:
        candle_end = next_candle_boundary(candle_start, "e1d")
        context = symbol.build_market_hours_context(
            trading_hours="eth",
            start_dt=candle_start,
            end_dt=candle_end,
        )
        last_open_dt = last_open_minute_in_candle(
            candle_start,
            "e1d",
            symbol,
            "eth",
            context=context,
        )

        if last_open_dt is not None:
            assert candle_start <= last_open_dt < candle_end
            assert symbol.is_open_dt(target_dt=last_open_dt, context=context)
        candle_start = candle_end


def test_last_open_minute_reuses_supplied_context():
    """A supplied context avoids rebuilding market-hours bounds."""
    symbol = OpenMinuteSymbol(lambda _: True)
    context = {"existing": True}

    last_open_minute_in_candle(
        "2026-09-11 16:55:00",
        "5m",
        symbol,
        "eth",
        context=context,
    )

    assert symbol.context_build_count == 0


def test_expected_open_minutes_lists_every_open_minute(symbol):
    """A normal five-minute candle expects each of its five minutes."""
    actual = expected_open_minutes_in_candle(
        "2026-09-11 16:55:00",
        "5m",
        symbol,
        "eth",
    )

    assert actual == [
        datetime(2026, 9, 11, 16, 55) + timedelta(minutes=offset)
        for offset in range(5)
    ]


def test_expected_open_minutes_excludes_closed_minutes(symbol):
    """A holiday early close shortens the expected minute list."""
    event = Event(
        start_dt="2026-11-27 13:15:00",
        end_dt="2026-11-27 17:59:00",
        symbol=symbol,
        category="Closed",
    )

    actual = expected_open_minutes_in_candle(
        "2026-11-27 13:00:00",
        "e1h",
        symbol,
        "eth",
        events=[event],
    )

    assert actual == [
        datetime(2026, 11, 27, 13) + timedelta(minutes=offset)
        for offset in range(15)
    ]


def test_expected_open_minutes_empty_for_fully_closed_candle(symbol):
    """A candle without any open minute expects no source data."""
    event = Event(
        start_dt="2026-12-24 18:00:00",
        end_dt="2026-12-25 17:59:00",
        symbol=symbol,
        category="Closed",
    )

    assert expected_open_minutes_in_candle(
        "2026-12-24 18:00:00",
        "e1d",
        symbol,
        "eth",
        events=[event],
    ) == []


def test_expected_open_minutes_ends_at_last_open_minute(symbol):
    """The final expected minute agrees with last_open_minute_in_candle."""
    minutes = expected_open_minutes_in_candle(
        "2026-09-10 18:00:00",
        "e1d",
        symbol,
        "eth",
    )

    assert minutes[-1] == last_open_minute_in_candle(
        "2026-09-10 18:00:00",
        "e1d",
        symbol,
        "eth",
    )


def test_expected_open_minutes_reuses_supplied_context():
    """A supplied context avoids rebuilding market-hours bounds."""
    symbol = OpenMinuteSymbol(lambda _: True)

    expected_open_minutes_in_candle(
        "2026-09-11 16:55:00",
        "5m",
        symbol,
        "eth",
        context={"existing": True},
    )

    assert symbol.context_build_count == 0
