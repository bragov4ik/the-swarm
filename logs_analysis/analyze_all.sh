#!/bin/bash

prefix="./logs/*.log"

# Check if argument is provided
if [[ $# -ne 0 ]]; then
    prefix=$@
fi

command="python3 ./logs_analysis/analyze.py"

for filename in $prefix; do
    eval "$command $filename" >> "${filename}_analysis"
done
