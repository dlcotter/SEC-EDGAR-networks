#!/bin/bash

md5sums=documents.md5
newdocroot=edgar/documents

if [[ ! -d "${newdocroot}" ]]; then
    mkdir "${newdocroot}"
fi

if [[ -e "${md5sums}" ]]; then
    echo "" >"${md5sums}"
fi

find edgar/data -name '*.txt' | \
    while read infile; do
	echo -n "processing ${infile}: "
	docNo=$(echo "${infile}" | \
		    sed -e 's@.*/.*/@@' \
                        -e 's@.txt@@' )
	echo -n "docNo: ${docNo} "
	newdir=$(echo "${docNo}" | \
		 sed -e 's@-@@g' -e 's@\(.*\)@scale=0; \1 % 3163@' | bc -ql)
	echo "newdir=${newdir}"
	if [[ ! -d "${newdocroot}"/${newdir} ]]; then
	    mkdir "${newdocroot}"/${newdir}
	fi
	newfile="${newdocroot}"/"${newdir}"/"${docNo}".txt
	if [[ ! -e "${newfile}" ]]; then
	    cp ${infile} "${newfile}"
	    echo "created ${newfile}"
	    openssl md5 ${newfile} >> "${md5sums}"
	fi
    done
