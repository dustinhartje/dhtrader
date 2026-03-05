"""Test data management module for extracting and rebuilding test data."""


def __getattr__(name):
    """Lazy load Extractor and Rebuilder only when accessed."""
    if name == 'Extractor':
        from .testdata import Extractor
        return Extractor
    elif name == 'Rebuilder':
        from .testdata import Rebuilder
        return Rebuilder
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = ['Extractor', 'Rebuilder']
