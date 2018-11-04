#!/bin/bash

# rebuild the PostgreSQL database from scratch
# change CSVDIR to point to your local directory where
# the CSV files are located

CSVDIR=/home/skr/Projects/cs626p1/code

psql sec < rdbms/src/main/sql/sec_schema.sql
if [[ ${?} == 0 ]]; then
    psql sec <<EOF
COPY entities(cik,trading_symbol,entity_name,entity_type,irsnumber,sic,sic_number,state_of_inc,fiscal_year_end) FROM '${CSVDIR}/entities.csv' DELIMITER '|';
COPY filings(accession_number,submission_type,document_count,filing_date,change_date) FROM '${CSVDIR}/filings.csv' DELIMITER '|';
COPY filings_entities(filing,entity) FROM '${CSVDIR}/filings_entities.csv' DELIMITER '|';
EOF
fi
