import load_dataframes.py
import graph_utils.py

v = combine_df(ents,filings).withColumn('id',monotonically_increasing_id())
#v = combine_df(ents.select(ents.cik,ents.filing_date.cast('string').
alias('e_filing_date')),filings.select(filings.accession_number,filings.filing_date.cast('string')))
v = v.withColumn('id', sqlf.when(v.cik.isNotNull(),sqlf.concat(v.e_filing_date,v.cik)).
otherwise(v.accession_number))
fdates = filings.select(filings.accession_number.alias('filing_acc_number'),filings.filing_date)
f = filings_ents.join(fdates)
r = owner_rels
#swap order of cik and filing date in creating key because accession number is the issuer_cik+filing_date!
e_owner_rels = r.withColumn('src',sqlf.concat(r.filing_date,r.owner_cik)).\
withColumn('dst',sqlf.concat(r.filing_date,r.issuer_cik))
e_filings_ents = f.withColumn('src',sqlf.concat(f.filing_date,f.cik)).\
withColumn('dst',f.accession_number)
e = combine_df(e_owner_rels,e_filings_ents)
g = GraphFrame(v,e)
