#! /bin/bash
pytest -sv $1

ls tests/hide_* &>/dev/null
test $? -ne 0 || echo "Hidden test files detected (./tests/hide_*.py.  Is this intentional?"

#if [ $? -ne 0 ]; then
#    echo "Hidden test files detected (./tests/hide_*.py.  Is this intentional?"
#fi
