#!/bin/bash
y=$1
q=$2
if [ -z "${year}"] && [ -z "${q}"]; then
	echo "usage get_index_qtr.sh year qtr[1,4]"
	exit
fi
indexPath=edgar/full-index/${y}/QTR${q}/master.gz
filePath=indices/edgar-${y}q${q}-master.gz
echo "${indexPath} => ${filePath}"
curl -G https://www.sec.gov/Archives/${indexPath} --create-dirs -o ${filePath}
