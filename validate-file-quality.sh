#!/bin/bash
# Validate file for code quality standards
# Usage: ./validate-file-quality.sh <filename>
#
# This script checks that a file adheres to:
# - 79 character line limit
# - No trailing whitespace
# - Proper line endings (LF)

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [ $# -lt 1 ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

FILENAME="$1"

if [ ! -f "$FILENAME" ]; then
    echo -e "${RED}ERROR: File not found: $FILENAME${NC}"
    exit 1
fi

ERRORS=0

echo "Validating: $FILENAME"
echo "---"

# Check 1: Line length (79 characters max)
echo -n "Checking line length (max 79 chars)... "
LONG_LINES=$(awk 'length > 79 {print NR": "length" chars"}' "$FILENAME" | wc -l)
if [ "$LONG_LINES" -gt 0 ]; then
    echo -e "${RED}FAILED${NC}"
    awk 'length > 79 {print "  Line "NR": "length" characters"}' "$FILENAME"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}PASSED${NC}"
fi

# Check 2: Trailing whitespace
echo -n "Checking for trailing whitespace... "
TRAILING=$(grep -n " $" "$FILENAME" 2>/dev/null | wc -l)
if [ "$TRAILING" -gt 0 ]; then
    echo -e "${RED}FAILED${NC}"
    echo "  Trailing whitespace found on:"
    grep -n " $" "$FILENAME" | head -5 | sed 's/^/    /'
    if [ "$TRAILING" -gt 5 ]; then
        echo "    ... and $((TRAILING - 5)) more lines"
    fi
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}PASSED${NC}"
fi

# Check 3: Line endings (LF not CRLF)
echo -n "Checking line endings (LF not CRLF)... "
if file "$FILENAME" | grep -q "CRLF"; then
    echo -e "${RED}FAILED${NC}"
    echo "  File has CRLF line endings (should be LF)"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}PASSED${NC}"
fi

# Summary
echo "---"
if [ "$ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed${NC}"
    exit 0
else
    echo -e "${RED}✗ $ERRORS check(s) failed${NC}"
    exit 1
fi
