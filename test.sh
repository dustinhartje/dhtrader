#! /bin/bash
if [ "$1" = "-h" ] || [ "$1" = "--help" ] || [ "$1" = "-?" ]; then
    echo "To exit on the first error: -x"
    echo "To run a single test file: test.sh tests/test_file.py"
    echo "To run a single test: test.sh tests/test_file.py::test_name"
else
    pytest -sv --durations=0 "$@"
fi

ls tests/hide_* &>/dev/null
test $? -ne 0 || echo "Hidden test files detected (./tests/hide_*.py).  Is this intentional?"
