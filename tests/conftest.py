"""Pytest helpers for marker-driven output suppression."""

import contextlib
import io

import pytest


def assert_eq_fields_cover_instance(obj):
    """Assert _EQ_FIELDS | _EQ_EXCLUDE exactly covers the instance __dict__.

    Pass any object whose class defines _EQ_FIELDS and _EQ_EXCLUDE.
    Fails with a descriptive message if any instance attribute is unaccounted
    for, or if either frozenset references an attribute that no longer exists.
    """
    cls = type(obj)
    all_attrs = set(obj.__dict__.keys())
    accounted = cls._EQ_FIELDS | cls._EQ_EXCLUDE
    missing = all_attrs - accounted
    phantom = accounted - all_attrs
    assert not missing, (
        f"{cls.__name__}: attrs in __dict__ missing from "
        f"_EQ_FIELDS/_EQ_EXCLUDE: {missing}"
    )
    assert not phantom, (
        f"{cls.__name__}: attrs in _EQ_FIELDS/_EQ_EXCLUDE not in "
        f"__dict__: {phantom}"
    )


@pytest.fixture(name="assert_eq_fields_cover_instance")
def assert_eq_fields_cover_instance_fixture():
    """Fixture exposing assert_eq_fields_cover_instance to test functions."""
    return assert_eq_fields_cover_instance


def run_eq_field_sensitivity(obj, sentinel, sub_eq_fields=None):
    """Run __eq__ field sensitivity assertions on a data class instance.

    Verifies that every _EQ_FIELDS member causes inequality when mutated
    and every truly-excluded _EQ_EXCLUDE member does not.

    obj           -- the instance to test
    sentinel      -- the _EqFieldSentinel from eq_field_sentinel fixture
    sub_eq_fields -- set of field names in _EQ_EXCLUDE that are still
                     compared via sub_eq() (e.g. {"parameters"})
    """
    from copy import deepcopy
    sub_eq_fields = sub_eq_fields or set()
    cls = type(obj)
    # Make an exact copy; the two instances must start out equal.
    cpy = deepcopy(obj)
    assert obj == cpy
    # --- _EQ_FIELDS: every listed field must drive inequality ---
    # Drop the sentinel (guaranteed != anything) into each field one at
    # a time, confirm the objects are now unequal, then restore the
    # original value -- proving __eq__ actually checks that field.
    # object.__setattr__ bypasses custom __setattr__ hooks (e.g. Trade's
    # sync logic when open_dt changes) so only __eq__ is exercised.
    for field in cls._EQ_FIELDS:
        original = getattr(cpy, field)
        object.__setattr__(cpy, field, sentinel)
        assert obj != cpy, (
            f"_EQ_FIELDS {field!r} did not cause inequality"
        )
        object.__setattr__(cpy, field, original)
    # After all fields are restored the pair must be equal again.
    assert obj == cpy
    # --- _EQ_EXCLUDE (truly excluded): each field must NOT matter ---
    # sub_eq_fields are declared as excluded from the _EQ_FIELDS loop
    # but are still compared via sub_eq(); skip them here, test below.
    truly_excluded = cls._EQ_EXCLUDE - sub_eq_fields
    for field in truly_excluded:
        original = getattr(cpy, field)
        object.__setattr__(cpy, field, sentinel)
        assert obj == cpy, (
            f"_EQ_EXCLUDE {field!r} caused unexpected inequality"
        )
        object.__setattr__(cpy, field, original)
    # --- sub_eq_fields: excluded from _EQ_FIELDS loop but still ---
    # compared via sub_eq() -- they DO affect equality.
    for field in sub_eq_fields:
        original = getattr(obj, field)
        object.__setattr__(cpy, field, sentinel)
        assert obj != cpy, (
            f"{field!r} should affect equality via sub_eq()"
        )
        object.__setattr__(cpy, field, original)
        assert obj == cpy


@pytest.fixture(name="run_eq_field_sensitivity")
def run_eq_field_sensitivity_fixture(eq_field_sentinel):
    """Fixture that binds eq_field_sentinel into run_eq_field_sensitivity.

    Tests declare this as a parameter and call it with just the object;
    the sentinel is already bound.  Example:

        def test_Foo_eq_field_sensitivity(run_eq_field_sensitivity):
            obj = Foo(...)
            run_eq_field_sensitivity(obj)

    For classes that compare 'parameters' via sub_eq(), pass the field
    name so it is tested separately:

        run_eq_field_sensitivity(obj, sub_eq_fields={"parameters"})
    """
    def _run(obj, sub_eq_fields=None):
        run_eq_field_sensitivity(obj, eq_field_sentinel, sub_eq_fields)
    return _run


class _EqFieldSentinel:
    """Sentinel for __eq__ field sensitivity tests.

    Compares as not-equal to any value and returns itself for any
    attribute access, so __eq__ methods that call getattr(other, field)
    on a complex field type do not raise AttributeError.
    """

    def __eq__(self, other):
        return False

    def __ne__(self, other):
        return True

    def __getattr__(self, name):
        return self


_EQ_FIELD_SENTINEL = _EqFieldSentinel()


@pytest.fixture(name="eq_field_sentinel")
def eq_field_sentinel_fixture():
    """Fixture returning a sentinel for __eq__ field sensitivity tests."""
    return _EQ_FIELD_SENTINEL


def is_valid_uuid(value):
    """Return True if value is a valid new_uuid() string.

    A valid uuid is a str of exactly 32 lowercase hex characters with
    no hyphens, as produced by new_uuid().
    """
    return (
        isinstance(value, str)
        and len(value) == 32
        and "-" not in value
        and all(c in "0123456789abcdef" for c in value)
    )


@pytest.fixture(name="is_valid_uuid")
def is_valid_uuid_fixture():
    """Fixture exposing is_valid_uuid to test functions."""
    return is_valid_uuid


@pytest.fixture(autouse=True)
def suppress_stdout_for_marked_tests(request):
    """Suppress stdout only for tests marked with suppress_stdout."""
    if request.node.get_closest_marker("suppress_stdout") is None:
        yield
        return

    with contextlib.redirect_stdout(io.StringIO()):
        yield
