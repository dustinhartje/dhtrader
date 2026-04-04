#!/bin/bash
# Validate file for code quality standards.
# Usage:
#   ./validate-file-quality.sh <filename>
#   ./validate-file-quality.sh --fix <filename>
#
# Checks:
# - Python files only: 79 character max line length
# - All text files: no trailing spaces or tabs
# - All text files: LF line endings (no CRLF)

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

FIX_MODE=false

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: $0 [--fix] <filename>"
    exit 1
fi

if [ $# -eq 2 ]; then
    if [ "$1" != "--fix" ]; then
        echo "Usage: $0 [--fix] <filename>"
        exit 1
    fi
    FIX_MODE=true
    FILENAME="$2"
else
    FILENAME="$1"
fi

if [ ! -f "$FILENAME" ]; then
    echo -e "${RED}ERROR: File not found: $FILENAME${NC}"
    exit 1
fi

if file --mime "$FILENAME" | grep -q 'charset=binary'; then
    echo -e "${YELLOW}SKIPPED${NC} binary file: $FILENAME"
    exit 0
fi

ERRORS=0
IS_PYTHON=false
IS_MARKDOWN=false
case "$FILENAME" in
    *.py)
        IS_PYTHON=true
        ;;
    *.md)
        IS_MARKDOWN=true
        ;;
esac

echo "Validating: $FILENAME"
echo "---"

if [ "$IS_PYTHON" = true ]; then
    echo -n "Checking line length (Python max 79 chars)... "
    LONG_LINES=$(awk 'length > 79 {print NR": "length" chars"}' \
        "$FILENAME" | wc -l)
    if [ "$LONG_LINES" -gt 0 ]; then
        echo -e "${RED}FAILED${NC}"
        awk 'length > 79 {print "  Line "NR": "length" characters"}' \
            "$FILENAME"
        ERRORS=$((ERRORS + 1))
    else
        echo -e "${GREEN}PASSED${NC}"
    fi
else
    echo -e "Checking line length (Python only)... ${YELLOW}SKIPPED${NC}"
fi

if [ "$IS_MARKDOWN" = true ]; then
    echo -e "Checking for trailing whitespace (markdown)... ${YELLOW}SKIPPED${NC}"
else
    echo -n "Checking for trailing whitespace (spaces/tabs)... "
    TRAILING=$(awk '/[[:blank:]]$/ {print NR}' "$FILENAME" | wc -l)
    if [ "$TRAILING" -gt 0 ]; then
        if [ "$FIX_MODE" = true ]; then
            sed -i 's/[[:blank:]]\+$//' "$FILENAME"
            TRAILING=$(awk '/[[:blank:]]$/ {print NR}' "$FILENAME" | wc -l)
        fi

        if [ "$TRAILING" -gt 0 ]; then
            echo -e "${RED}FAILED${NC}"
            echo "  Trailing whitespace found on:"
            awk '/[[:blank:]]$/ {print NR":"$0}' "$FILENAME" | head -5 |
                sed 's/^/    /'
            if [ "$TRAILING" -gt 5 ]; then
                echo "    ... and $((TRAILING - 5)) more lines"
            fi
            ERRORS=$((ERRORS + 1))
        else
            echo -e "${GREEN}PASSED${NC} (fixed)"
        fi
    else
        echo -e "${GREEN}PASSED${NC}"
    fi
fi

echo -n "Checking line endings (LF not CRLF)... "
if file "$FILENAME" | grep -q 'CRLF'; then
    if [ "$FIX_MODE" = true ]; then
        sed -i 's/\r$//' "$FILENAME"
    fi

    if file "$FILENAME" | grep -q 'CRLF'; then
        echo -e "${RED}FAILED${NC}"
        echo "  File has CRLF line endings (should be LF)"
        ERRORS=$((ERRORS + 1))
    else
        echo -e "${GREEN}PASSED${NC} (fixed)"
    fi
else
    echo -e "${GREEN}PASSED${NC}"
fi

echo "---"
if [ "$ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed${NC}"
    exit 0
fi

echo -e "${RED}✗ $ERRORS check(s) failed${NC}"
exit 1
