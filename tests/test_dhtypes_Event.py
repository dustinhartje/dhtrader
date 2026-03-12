"""Tests for Event creation and datetime containment logic."""
import pytest
from dhtrader import (
    Event)


@pytest.fixture
def event():
    """Create and return a default ES Closed Event fixture."""
    return Event(start_dt="2025-01-02 12:00:00",
                 end_dt="2025-01-02 18:00:00",
                 symbol="ES",
                 category="Closed",
                 tags=["holiday"],
                 notes="Test Holiday",
                 )


def test_Event_create_and_verify_pretty():
    """Verify Event.pretty() output line count."""
    # Check line counts of pretty output, won't change unless class changes
    out_event = Event(start_dt="2025-01-02 12:00:00",
                      end_dt="2025-01-02 18:00:00",
                      symbol="ES",
                      category="Closed",
                      tags=["holiday"],
                      notes="Test Holiday",
                      )
    assert isinstance(out_event, Event)
    assert len(out_event.pretty().splitlines()) == 12


def test_Event_contains_datetime(event):
    """Verify contains_datetime uses inclusive start/end boundaries."""
    # Datetime inside the range returns True
    assert event.contains_datetime("2025-01-02 14:00:00")
    # At start boundary is True (inclusive)
    assert event.contains_datetime("2025-01-02 12:00:00")
    # At end boundary is True (inclusive)
    assert event.contains_datetime("2025-01-02 18:00:00")
    # Just before start is False
    assert not event.contains_datetime("2025-01-02 11:59:59")
    assert not event.contains_datetime("2025-01-01 00:00:00")
    # Just after end is False
    assert not event.contains_datetime("2025-01-02 18:00:01")
    assert not event.contains_datetime("2025-01-03 00:00:00")


def test_Event_to_clean_dict(event):
    """Verify Event.to_clean_dict returns a dict with expected keys."""
    d = event.to_clean_dict()
    assert isinstance(d, dict)
    assert d["start_dt"] == "2025-01-02 12:00:00"
    assert d["end_dt"] == "2025-01-02 18:00:00"
    assert d["symbol"] == "ES"
    assert d["category"] == "Closed"
    assert d["tags"] == ["holiday"]
    assert d["notes"] == "Test Holiday"


def test_Event_str_and_repr(event):
    """Verify Event __str__ and __repr__ return non-empty strings."""
    assert isinstance(str(event), str)
    assert len(str(event)) > 0
    assert isinstance(repr(event), str)
    assert str(event) == repr(event)


def test_Event_init(event):
    """Verify Event stores all attributes and computes epoch values."""
    assert event.start_dt == "2025-01-02 12:00:00"
    assert event.end_dt == "2025-01-02 18:00:00"
    assert event.symbol.ticker == "ES"
    assert event.category == "Closed"
    assert event.notes == "Test Holiday"
    assert isinstance(event.start_epoch, int)
    assert isinstance(event.end_epoch, int)
    assert event.start_epoch < event.end_epoch
