#!/bin/bash
#
# organize_documents.sh - reorganize our documents so we have 3163 directories
#                         underneath the root directory instead of over
#                         200,000 directories
#
# Steve Roggenkamp
#

md5sums=documents.md5
newdocroot=edgar/documents

if [[ ! -d "${newdocroot}" ]]; then
    mkdir "${newdocroot}"
fi

if [[ -e "${md5sums}" ]]; then
    echo "" >"${md5sums}"
fi

# traverse the old diretory tree getting documents to move
find edgar/data -name '*.txt' | \
    while read infile; do
	echo -n "processing ${infile}: "
	docNo=$(echo "${infile}" | \
		    sed -e 's@.*/.*/@@' \
                        -e 's@.txt@@' )
	# determine the new directory for the document
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
