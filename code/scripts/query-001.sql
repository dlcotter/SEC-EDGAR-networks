SELECT a.entity_name,a.cik,c.filing_date,c.accession_number
FROM   entities a
INNER JOIN filings_entities b on
     a.cik              = b.entity
INNER JOIN filings c on
 c.accession_number = b.filing
WHERE c.filing_date >= '2018-01-01'
  AND c.filing_date <= '2018-03-31'
ORDER BY c.filing_date
;
