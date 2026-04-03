"""Test that total log file size stays within acceptable limits."""
import os
import pytest


LOGS_DIR = os.path.join(
    os.path.dirname(__file__), '..', 'logs'
)
MAX_TOTAL_BYTES = 2147483648  # 2 GB


def _check_log_size():
    """Return (passed, message) for the dhtrader logs size check."""
    log_dir = os.path.abspath(LOGS_DIR)
    if not os.path.isdir(log_dir):
        return True, f"path {log_dir} not found"
    total = sum(
        os.path.getsize(os.path.join(log_dir, f))
        for f in os.listdir(log_dir)
        if os.path.isfile(os.path.join(log_dir, f))
    )
    passed = total <= MAX_TOTAL_BYTES
    limit_gb = MAX_TOTAL_BYTES / (1024 ** 3)
    total_gb = total / (1024 ** 3)
    return passed, (
        f"path {log_dir}, {total_gb:.2f} GB used, "
        f"limit {limit_gb:.2f} GB"
    )


@pytest.mark.suppress_stdout
def test_log_folder_total_size_under_limit():
    """Fail if total size of all files in logs/ exceeds MAX_TOTAL_BYTES."""
    passed, msg = _check_log_size()
    assert passed, f"Total log size exceeds limit: {msg}"
