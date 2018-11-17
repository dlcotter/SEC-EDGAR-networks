#!/bin/bash

# rebuild the PostgreSQL database from scratch
# change CSVDIR to point to your local directory where
# the CSV files are located

CSVDIR=/home/skr/Projects/cs626p1/code

psql sec < rdbms/src/main/sql/sec_schema.sql
if [[ ${?} == 0 ]]; then
    psql sec  <<EOF
COPY entities(cik,trading_symbol,entity_name,entity_type,irsnumber,sic,sic_number,state_of_inc,fiscal_year_end) FROM '${CSVDIR}/entities.csv' DELIMITER '|';
COPY filings(accession_number,submission_type,document_count,filing_date,change_date) FROM '${CSVDIR}/filings.csv' DELIMITER '|';
COPY filings_entities(filing,entity) FROM '${CSVDIR}/filings_entities.csv' DELIMITER '|';
COPY contacts(cik,filing_date,contact_type,street1,stree2,street3,city,state,zip,phone) FROM '${CSVDIR}/contacts.csv' DELIMITER '|';
COPY owner_rels(issuer_cik,owner_cik,filing_date,is_director,is_officer,is_10_percent_owner,is_other,officer_title) FROM '${CSVDIR}/owner_rels.csv' DELIMITER '|';
COPY documents(filing,sequence,type,filname,format,description) FROM '${CSVDIR}/documents.csv' DELIMITER '|';
EOF
fi
