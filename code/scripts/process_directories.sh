#!/bin/bash

# process_directories - process documents in the directories contained in
#                       the file given as the first argument.  using this
#                       script we can run it as a background job and have
#                       multiple instances of it running simultaneously
#                       to max out the processor and system
#   
# Steve Roggenkamp
#
BINDIR=<CS626 repo directory>/code/sec-wc/src/main/python
SECDIR=<SEC documents root directory>

for d in $(cat $1); do
    python3 ${BINDIR}/SECFiling.py ${SECDIR}/${d}/*.txt >${d}.data 2>${d}.log
done
	 
