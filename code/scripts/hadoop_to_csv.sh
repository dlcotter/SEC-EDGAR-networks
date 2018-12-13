#!/bin/bash

# eliminate the begining key value from the hadoop output
# and insure we have unique values to prevent duplicate
# primary keys
#
# Early code to change extracted data into a form we
# can load into an RDBMS
#
# Steve Roggenkamp
#

sed -n -e '/^entity\t/s@.*\t@@p' $*        | sort -u  >entities.csv
sed -n -e '/^filing\t/s@.*\t@@p' $*        | sort -u >filings.csv
sed -n -e '/^filing_entity\t/s@.*\t@@p' $* | sort -u >filings_entities.csv
