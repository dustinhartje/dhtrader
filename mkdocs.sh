#!/bin/bash
rm -f ./docs/*
python3 -m pydoc -w dhcharts
python3 -m pydoc -w dhtrades
mv *.html docs/
