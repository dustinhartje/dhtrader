"""Tests for Event creation and datetime containment logic."""
import json
import pytest
from dhtrader import (
    Event)


@pytest.fixture
def event():
    """Create and return a default ES Closed Event fixture."""
    return Event(start_dt="2099-01-02 12:00:00",
                 end_dt="2099-01-02 18:00:00",
                 symbol="ES",
                 category="Closed",
                 tags=["holiday"],
                 notes="Test Holiday",
                 )


def test_Event_create_and_verify_common_methods():
    """Test Event __init__ values, __str__, __repr__, to_clean_dict,
    to_json, and pretty.

    Event does not define __eq__, __ne__, or brief.
    """
    event = Event(start_dt="2099-01-02 12:00:00",
                  end_dt="2099-01-02 18:00:00",
                  symbol="ES",
                  category="Closed",
                  tags=["holiday"],
                  notes="Test Holiday",
                  )
    assert isinstance(event, Event)
    # __init__
    assert event.start_dt == "2099-01-02 12:00:00"
    assert event.end_dt == "2099-01-02 18:00:00"
    assert event.symbol.ticker == "ES"
    assert event.category == "Closed"
    assert event.tags == ["holiday"]
    assert event.notes == "Test Holiday"
    assert isinstance(event.start_epoch, int)
    assert isinstance(event.end_epoch, int)
    assert event.start_epoch < event.end_epoch
    assert event.name == "nameless"
    expected_attrs = {
        "category", "end_dt", "end_epoch", "name", "notes",
        "start_dt", "start_epoch", "symbol", "tags",
    }
    actual_attrs = set(vars(event).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "Event attributes changed. Update this test's "
        "__init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    # __str__
    assert isinstance(str(event), str)
    assert len(str(event)) > 0
    # __repr__
    assert isinstance(repr(event), str)
    assert str(event) == repr(event)
    # to_clean_dict
    d = event.to_clean_dict()
    assert isinstance(d, dict)
    assert d["start_dt"] == "2099-01-02 12:00:00"
    assert d["end_dt"] == "2099-01-02 18:00:00"
    assert d["symbol"] == "ES"
    assert d["category"] == "Closed"
    assert d["tags"] == ["holiday"]
    assert d["notes"] == "Test Holiday"
    # to_json
    j = event.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["start_dt"] == "2099-01-02 12:00:00"
    assert parsed["category"] == "Closed"
    # pretty
    assert isinstance(event.pretty(), str)
    assert len(event.pretty().splitlines()) == 13


def test_Event_contains_datetime(event):
    """Verify contains_datetime uses inclusive start/end boundaries."""
    # Datetime inside the range returns True
    assert event.contains_datetime("2099-01-02 14:00:00")
    # At start boundary is True (inclusive)
    assert event.contains_datetime("2099-01-02 12:00:00")
    # At end boundary is True (inclusive)
    assert event.contains_datetime("2099-01-02 18:00:00")
    # Just before start is False
    assert not event.contains_datetime("2099-01-02 11:59:59")
    assert not event.contains_datetime("2099-01-01 13:00:00")
    # Just after end is False
    assert not event.contains_datetime("2099-01-02 18:00:01")
    assert not event.contains_datetime("2099-01-03 13:00:00")
