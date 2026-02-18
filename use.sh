#!/bin/bash

# Script to manage and display MongoDB environment configuration
# Usage: ./use.sh [atlas|prod|test]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Delegate to Python script for all operations
python3 "$SCRIPT_DIR/use.py" "$@"
