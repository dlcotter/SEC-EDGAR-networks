#!/bin/bash

for y in 1997 1998 1999 \
	 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 \
	 2010 2011 2012 2013 2014 2015 2016; do
    for q in 1 2 3 4; do
	indexPath=edgar/full-index/${y}/QTR${q}/master.gz
	filePath=indices/edgar-${y}q${q}-master.gz
	echo "${indexPath} => ${filePath}"
	curl -G https://www.sec.gov/Archives/${indexPath} --create-dirs -o ${filePath}
    done
done
