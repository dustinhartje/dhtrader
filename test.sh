#! /bin/bash
# Simple test runner with optional -f flag to exclude slow tests

# Show help if requested anywhere in arguments
for a in "$@"; do
    if [ "$a" = "-h" ] || [ "$a" = "--help" ] || [ "$a" = "-?" ]; then
        echo "To exit on the first error: test.sh -x"
        echo "To run a single test file: test.sh tests/test_file.py"
        echo "To run a single test: test.sh tests/test_file.py::test_name"
        echo "To run only fast tests: test.sh -f"
        echo "To run only specific marks: test.sh -m 'storage'"
        echo "To run only without specific marks: test.sh -m 'not storage'"
        echo ""
        echo "Marks are listed in setup.cfg [tool:pytest] section or grep for '@pytest.mark'"
        exit 0
    fi
done

FILTER_NOT_SLOW=false
ARGS=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        -f)
            FILTER_NOT_SLOW=true
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

EXTRA_MARKS=()
if [ "$FILTER_NOT_SLOW" = true ]; then
    EXTRA_MARKS+=("-m" "not slow")
fi

pytest -svv --durations=0 --show-capture=stdout "${EXTRA_MARKS[@]}" "${ARGS[@]}"

ls tests/hide_* &>/dev/null
test $? -ne 0 || echo "Hidden test files detected (./tests/hide_*.py).  Is this intentional?"
