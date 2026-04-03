"""Pytest helpers for marker-driven output suppression."""

import contextlib
import io

import pytest


@pytest.fixture(autouse=True)
def suppress_stdout_for_marked_tests(request):
    """Suppress stdout only for tests marked with suppress_stdout."""
    if request.node.get_closest_marker("suppress_stdout") is None:
        yield
        return

    with contextlib.redirect_stdout(io.StringIO()):
        yield
