#!/bin/bash

year=$1
q=$2

if [ -z "${year}" ] && [ -z "${q}" ]; then
	echo "usage:  get_form4_qtr.sh year qtr[1-4]"
    exit 1
fi

echo "Getting Form 4 for year: ${year} ${q}"
    filePath=indices/edgar-${year}q${q}-master.gz
    if [[ -e ${filePath} ]]; then
        gunzip -c ${filePath} | \
	    awk -F \| \
		'    NR>11 && $3=="4"{print($5);}' | \
	    while read path; do
		if [[ ! -e "${path}" ]]; then
		    echo "$path"
		fi
	    done | wget -o form4-${year}.log \
			--base "https://www.sec.gov/Archives/"  \
			-r \
			-nv \
			-i - \
			--continue \
			-nH \
			--cut-dirs=1
    fi
