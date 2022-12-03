#!/bin/bash
for file in *.txt; do
    [ -f "$file" ] || continue
    eval "influx write -b advdb -o ulb -p ns --format=lp --token {token} -f ./$file"
done
