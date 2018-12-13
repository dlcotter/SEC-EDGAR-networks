#!/bin/bash

#ed entities.csv <<< $'H\n0a\ncik|trading_symbol|entity_name|entity_type|irsnumber|sic|sic_number|state_of_inc|fiscal_year_end\n.\nwq'
ed filings.csv <<< $'H\n0a\naccession_number|submission_type|document_count|filing_date|change_date\n.\nwq'
ed filings_entities.csv <<< $'H\n0a\nfiling|entity\n.\nwq'
#ed contacts.csv <<< $'H\n0a\ncik|filing_date|contact_type|street1|stree2|street3|city|state|zip|phone\n.\nwq'
ed owner_rels.csv <<< $'H\n0a\nissuer_cik|owner_cik|filing_date|is_director|is_officer|is_10_percent_owner|is_other|officer_title\n.\nwq'
#ed documents.csv <<< $'H\n0a\nfiling|sequence|type|filname|format|description\n.\nwq'

