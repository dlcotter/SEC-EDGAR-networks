DROP TABLE IF EXISTS filings_entities CASCADE;
DROP TABLE IF EXISTS documents CASCADE;
DROP TABLE IF EXISTS owner_rels CASCADE;
DROP TABLE IF EXISTS filings CASCADE;
DROP TABLE IF EXISTS contacts CASCADE;
DROP TABLE IF EXISTS entities CASCADE;

DROP TYPE IF EXISTS type_of_format;
DROP TYPE IF EXISTS type_of_contact;
DROP TYPE IF EXISTS type_of_entity;


CREATE TYPE type_of_entity  AS ENUM ('issuer','owner');
CREATE TYPE type_of_contact AS ENUM ('business','mail','other');
CREATE TYPE type_of_format  AS ENUM ('HTML','PDF','TEXT','XML');

CREATE TABLE entities (
    cik             text,
    trading_symbol  text,
    entity_name     text NOT NULL,
    entity_type     type_of_entity NOT NULL,
    irsnumber       text,
    sic             text,
    sic_number      text,
    state_of_inc    char(2),
    fiscal_year_end char(4),
    PRIMARY KEY (cik)
);

CREATE TABLE contacts (
    cik             text REFERENCES entities (cik),
    filing_date     date,
    contact_type    type_of_contact,
    street1         text,
    street2         text,
    street3         text,
    city            text,
    state           text,
    zip             text,
    phone           text,
    PRIMARY KEY (cik,filing_date,contact_type)
);

CREATE UNIQUE INDEX contacts_cik_idx ON contacts (cik);

CREATE TABLE filings (
    accession_number text,
    submission_type  text,
    document_count   int,
    filing_date      date,
    change_date      date,
    PRIMARY KEY (accession_number)
);

CREATE TABLE owner_rels (
    issuer_cik      text REFERENCES entities (cik) NOT NULL,
    owner_cik       text REFERENCES entities (cik) NOT NULL,
    filing_date     date NOT NULL,
    is_director     boolean NOT NULL,
    is_officer      boolean NOT NULL,
    is_10_pct_owner boolean NOT NULL,
    is_other        boolean NOT NULL,
    officer_title   text,
    PRIMARY KEY (issuer_cik,owner_cik,filing_date)
);    

CREATE INDEX owner_rels_issuer_cik_idx ON owner_rels (issuer_cik);
CREATE INDEX owner_rels_owner_cik_idx  ON owner_rels (owner_cik);

CREATE TABLE documents (
    filing          text REFERENCES filings (accession_number) NOT NULL,
    sequence        int,
    type            text,
    filename        text,
    format          type_of_format,
    description     text,
    PRIMARY KEY (filing,sequence)
);

CREATE TABLE filings_entities (
    filing          text REFERENCES filings (accession_number) NOT NULL,
    entity          text REFERENCES entities (cik) NOT NULL,
    PRIMARY kEY (filing,entity)
);

CREATE INDEX filings_entities_filing_idx ON filings_entities (filing);
CREATE INDEX filings_entities_entity_idx ON filings_entities (entity);

