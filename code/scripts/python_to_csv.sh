#!/bin/bash

# eliminate the begining key value from the hadoop output
# and insure we have unique values to prevent duplicate
# primary keys
#
# Steve Roggenkmp

for f in contacts documents entities filings filings_entities owner_rels; do
    echo "Processing $f:"
    sed -n -e "/^$f/s@[^|]*|@@p" $*  | sort -u  >$f.csv
done
