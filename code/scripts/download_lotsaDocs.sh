#!/bin/bash

# download_lotsaDocs.sh - downlod a number of documents simultaneously

for f in $*; do
    echo "Downloading documents from ${f}"
    download_SECdoc.sh "${f}"
done
