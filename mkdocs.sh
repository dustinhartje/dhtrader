#!/bin/bash
python3 -m pydoc -w dhcharts
python3 -m pydoc -w dhtrades
python3 -m pydoc -w dhutil
python3 -m pydoc -w dhstore
python3 -m pydoc -w dhmongo
mv *.html docs/
