from pyspark.sql.functions import lit
from pyspark.sql.types import NullType
from re import sub
from datetime import datetime,timezone
from itertools import chain,count
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql import functions as sqlf
from graphframes.graphframe import GraphFrame
from graph_utils import *

sc.setCheckpointDir('/opt/data/spark_checkpoints')
#spark.sql('SET spark.sql.crossJoin.enabled=true')
_colnames = ",".join(("c{} STRING".format(i) for i in range(0,10)))
secdata = spark.read.csv('/opt/data/data/allDocs.data',sep="|",schema=_colnames)

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

#v = combine_df(\
ents.select(ents.cik,ents.filing_date.cast('string').alias('e_filing_date')),\
filings.select(filings.accession_number,filings.filing_date))
#v = v.withColumn('id', sqlf.when(v.cik.isNotNull(),v.cik).
otherwise(v.accession_number))
#fdates = filings.select(filings.accession_number.alias('filing_acc_number'),filings.filing_date)
#f = filings_ents.join(fdates,on=fdates.filing_acc_number == filings_ents.accession_number)
r = owner_rels
f = filings_ents
#swap order of cik and filing date in creating key because accession number is the issuer_cik+filing_date!
e_owner_rels = r.withColumn('src',r.owner_cik).\
withColumn('dst',r.issuer_cik)
e_filings_ents = f.withColumn('src',f.cik).\
withColumn('dst',f.accession_number)
e = combine_df(e_owner_rels,e_filings_ents)
g = GraphFrame(spark.read.parquet('ccs'),e)

#be sure to remove the order by when running on the big cluster, it might be more efficient to sort later on a single node?
degrees = g.degrees.orderBy('degree',ascending=False)
gp = g.pageRank(tol=0.1)
pr = g.vertices.orderBy('pagerank',ascending=False)
ccs = gp.connectedComponents()
top_cc =  ccs.groupBy('component').count().orderBy('count',ascending=False).limit(5).collect()
#get degree distr and page range distr for top 5 components

#largest_component = 42949672974#found by inspecting output
for row in top_cc:
    ccs.where(ccs.component == row.component).show()#dump everything we need stats-wise from each of the five largest components
