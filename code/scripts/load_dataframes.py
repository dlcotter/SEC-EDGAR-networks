import pyspark.sql
_colnames = ",".join(("c{} STRING".format(i) for i in range(0,10)))
spark = pyspark.sql.SparkSession.builder.getOrBuild()
def load_frames(path_to_input):
    global owner_rels,ents,filings_ents,filings,contacts,spark
    secdata = spark.read.csv(path_to_input,sep="|",schema=_colnames)    
    owner_rels = secdata.filter(secdata.c0 == 'owner_rels').\
    select(secdata.c1.alias('issuer_cik'),
    secdata.c2.alias('owner_cik'),
    secdata.c3.cast('date').alias('filing_date'),
    secdata.c4.cast('boolean').alias('isDirector'),
    secdata.c5.cast('boolean').alias('isOfficer'),
    secdata.c6.cast('boolean').alias('isTenPercentOwner'),
    secdata.c7.cast('boolean').alias('isOther'),
    secdata.c8.alias('owner_officer_title'))

    ents = secdata.filter(secdata.c0 == 'entities').\
    select(secdata.c1.alias('cik'),
    secdata.c2.alias('filing_date').cast('date'),
    secdata.c3.alias('trading_symbol'),
    secdata.c4.alias('entity_name'),
    secdata.c5.alias('irsnumber'),
    secdata.c6.alias('sic'),
    secdata.c7.alias('sic_number'),
    secdata.c8.alias('state_of_inc'),
    secdata.c9.alias('fiscal_year_end'))

    filings_ents = secdata.filter(secdata.c0 == 'filings_entities').\
    select(secdata.c1.alias('accession_number'),
    secdata.c2.alias('cik'),
    secdata.c3.alias('entity_type'))

    filings = secdata.filter(secdata.c0 == 'filings').\
    select(secdata.c1.alias('accession_number'),
    secdata.c2.alias('submission_type'),
    secdata.c3.alias('document_count').cast('int'),
    secdata.c4.cast('date').alias('reporting_period'),
    secdata.c5.cast('date').alias('filing_date'),
    secdata.c6.cast('date').alias('change_date'))

    contacts= secdata.filter(secdata.c0 == 'contacts').\
    select(secdata.c1.alias('cik'),
    secdata.c2.alias('filing_data').cast('date'),
    secdata.c3.alias('entity_type'),
    secdata.c4.alias('street'),
    secdata.c5.alias('extra_1'),
    secdata.c6.alias('extra_2'),
    secdata.c7.alias('city'),
    secdata.c8.alias('state'),
    secdata.c9.alias('zipcode'))
