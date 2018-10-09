#!/bin/bash

for y in 2018 2017 2016 2015 2014 2013 2012 2011 2010 \
	      2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 \
	      1999 1998 1997 \
	      ; do
    for q in 1 2 3 4; do
	filePath=indices/edgar-${y}q${q}-master.gz
	if [[ -e ${filePath} ]]; then
            gunzip -c ${filePath} | \
	        awk -F \| \
               '    NR>11 && $3=="4"{print($5);}' | \
		       while read path; do
		           if [[ ! -e "${path}" ]]; then
			       echo "$path"
		           fi
		       done | wget -o form4-20181004c.log \
				   --base "https://www.sec.gov/Archives/"  \
				   -r \
				   -nv \
				   -i - \
				   --continue \
				   -nH \
				   --cut-dirs=1
	fi
    done
done
