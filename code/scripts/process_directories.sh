#!/bin/bash

BINDIR=<CS626 repo directory>/code/sec-wc/src/main/python
SECDIR=<SEC documents root directory>

for d in $(cat $1); do
    python3 ${BINDIR}/SECFiling.py ${SECDIR}/${d}/*.txt >${d}.data 2>${d}.log
done
	 
