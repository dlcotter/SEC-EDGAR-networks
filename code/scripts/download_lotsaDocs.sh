#!/bin/bash

# download_lotsaDocs.sh - downlod a number of documents simultaneously
#
# This script starts a background job for each item on the command line
# to get around request throughput limitations of the SEC site which is
# typically 5/sec
#
# Steve Roggenkamp
#

for f in $*; do
    echo "Downloading documents from ${f}"
    nohup download_SECdoc.sh "${f}" 
done
