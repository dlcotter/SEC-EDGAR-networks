#!/bin/bash

#
# download_SECdocs.sh - download a list of documents from the SEC's archive
#
# arguments:  $1 - file containing paths of documents to download
#
# Author: Steve Roggenkamp


if [[ -r "$1" ]]; then
    echo "Downloading: $1"
    cat "$1" |\
    while read path; do
	if [[ -n "$path" && ! -f "$path" ]]; then
	    echo "$path"
	fi
    done | wget -o download-$(basename $1 .txt).log \
		--base "https://www.sec.gov/Archives/"  \
		-r \
		-nv \
		-i - \
		--continue \
		-nH \
		--cut-dirs=1
fi

