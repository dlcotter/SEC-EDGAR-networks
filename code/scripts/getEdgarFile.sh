#!/bin/bash
#
# getEdgarFile.sh - this script downloads everything in a quarterly
# index file.

# Steve Roggenkamp
#

for f in $(gunzip -c $1 | awk -F \| 'NR>11{print($5);}'); do
    d=$(dirname $f)
   echo "Retrieving $f"
    curl -G https://www.sec.gov/Archives/$f --create-dirs -o $f
   echo curl -G https://www.sec.gov/Archives/$f -o $f
done
