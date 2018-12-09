from pyspark.sql.functions import lit
from pyspark.sql.types import NullType
from re import sub
from datetime import datetime,timezone
from itertools import chain,count
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql import functions as sqlf
from graphframes.graphframe import GraphFrame

sc.setCheckpointDir('~/my_spark_checkpoints')
spark.sql('SET spark.sql.crossJoin.enabled=true')
_colnames = ",".join(("c{} STRING".format(i) for i in range(0,10)))
secdata = spark.read.csv('stocks.data',sep="|",schema=_colnames)

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

def combine_df(one,other):
    one_types = {colname:coltype for colname,coltype in one.dtypes}
    other_types = {colname:coltype for colname,coltype in other.dtypes}
    one_with_other = 'one.'+'.'.join((f'withColumn("{colname}",lit(None).cast("{other_types[colname]}"))' for colname in other.columns if colname not in one.columns))
    other_with_one = 'other.'+'.'.join((f'withColumn("{colname}",lit(None).cast("{one_types[colname]}"))' for colname in one.columns if colname not in other.columns))
    one = eval(one_with_other)
    other = eval(other_with_one)
    return one.unionByName(other)

def owner_rel_labels(edge_row):
    return f'{edge_row.issuer_cik}-{edge_row.owner_cik}'

def get_attribs_dict(attrib_pairs):
    titles = (attrib_pair[0] for attrib_pair in attrib_pairs)
    return {attrib_title:attrib_id for attrib_id,attrib_title in enumerate(titles)}

def get_attribute_block(attribs,attrib_class):
    attrib_id_dict = get_attribs_dict(attribs)
    attribute_block=f'<attributes class="{attrib_class}">\n'
    for attrib_title,attrib_type in attribs:
        attribute_block+=f'\t<attribute id="{attrib_id_dict[attrib_title]}" title="{attrib_title}" type="{attrib_type}"/>\n'
    attribute_block += '</attributes>\n'
    return attribute_block

def get_attrib_block_from_df(df,attrib_class):
    attrib_id_dict = get_attribs_dict(df.dtypes)
    attribute_block=f'<attributes class="{attrib_class}">\n'
    for attrib_title,attrib_type in df.dtypes:
        attribute_block+=f'\t<attribute id="{attrib_id_dict[attrib_title]}" title="{attrib_title}" type="{attrib_type}"/>\n'
    attribute_block += '</attributes>\n'
    return attribute_block

def get_nodes_block(nodes_df,attribs,id_func,label_func):
    nodes_block = '<nodes>\n'
    attribs_dict = get_attribs_dict(attribs)
    for node in nodes_df.collect():
        nodes_block+=f'\t<node id="{id_func(node)}" label="{label_func(node)}">\n'
        nodes_block+='\t\t<attvalues>\n'
        realkeys = (key for key in node.asDict().keys() if key not in ('id'))
        try:
            for attrib_title in realkeys:
                if node[attrib_title] != None:
                    nodes_block+=f'\t\t\t<attvalue for="{attribs_dict[attrib_title]}" value="{node[attrib_title]}"/>\n'
        except KeyError as e:
            print(f'Attribute {e} is either not in dataframe.dtypes or could not be found in attribs_dict')
            raise e
        nodes_block += '\t\t</attvalues>\n'
        nodes_block+='\t</node>\n'
    nodes_block += '</nodes>\n'
    return nodes_block

def get_edges_block(edges_df,attribs,id_func,label_func,weight_func=None):
    edges_block = '<edges>\n'
    attribs_dict = get_attribs_dict(attribs)
    for edge in edges_df.collect():
        edges_block+= f'\t<edge id="{id_func(edge)}" source="{edge.src}" target="{edge.dst}" label="{label_func(edge)}"'
        if weight_func:
            edges_block+=' weight="{weight_func(edge)}"'
        edges_block+= '>\n'
        edges_block+='\t\t<attvalues>\n'
        realkeys = (key for key in edge.asDict().keys() if key not in ('src','dst','id'))
        for attrib_title in realkeys:
            if edge[attrib_title] != None:
                edges_block+=f'\t\t\t<attvalue for="{attribs_dict[attrib_title]}" value="{edge[attrib_title]}"/>\n'
        edges_block += '\t\t</attvalues>\n'
        edges_block += '\t</edge>\n'
    edges_block += '</edges>\n'
    return edges_block

def defaultId(row):
    return row['id']

def defaultLabel(dont_care):
    return ''

def toGEFX(v,e,
edge_type='undirected',
node_attribs=None,
edge_attribs=None,
creator='Some person',
description='A graph',
last_modified=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S%z'),
node_id_func=defaultId,
node_label_func=defaultLabel,
edge_id_func=defaultId,
edge_label_func=defaultLabel,
weight_func=None):
    _GEPHI_DATATYPE_NAMES = {'integer':'integer','int':'integer','bigint':'integer','double':'double', 'float':'float',  'boolean':'boolean',  'string':'string', 'date':'date'}
    if not node_attribs:
        node_attribs = v.dtypes
    if not edge_attribs:
        edge_attribs = e.dtypes
    if 'id' not in e.columns:
        e = e.withColumn('id',monotonically_increasing_id())
    try:
        for attrib_pair in chain(node_attribs,edge_attribs):
            #print(attrib_pair)
            _GEPHI_DATATYPE_NAMES[attrib_pair[1]]
    except KeyError as e:
        print(f'Datatype {e} given in dataframe is not valid for GEFX')
        raise e
    xml =  f'''<?xml version="1.0" encoding="UTF-8"?>
    <gexf xmlns="http://www.gexf.net/1.2draft" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd" version="1.2">
    <meta lastmodifieddate="{last_modified}">
        <creator>{creator}</creator>
        <description>{description}</description>
    </meta>  
    <graph defaultedgetype="{edge_type}">
    {get_attribute_block(node_attribs,'node')}
    {get_attribute_block(edge_attribs,'edge')}
    {get_nodes_block(v, node_attribs, node_id_func, node_label_func)}
    {get_edges_block(e, edge_attribs, edge_id_func, edge_label_func,weight_func=weight_func)}
    </graph>
    </gexf>'''
    return sub('&','&amp;',xml)


#v = combine_df(ents,filings).withColumn('id',monotonically_increasing_id())
v = combine_df(ents.select(ents.cik,ents.filing_date.cast('string').
alias('e_filing_date')),filings.select(filings.accession_number,filings.filing_date.cast('string')))
v = v.withColumn('id', sqlf.when(v.cik.isNotNull(),sqlf.concat(v.e_filing_date,v.cik)).
otherwise(v.accession_number))
fdates = filings.select(filings.accession_number.alias('filing_acc_number'),filings.filing_date)
f = filings_ents.join(fdates)
r = owner_rels
#cik_id = v.select(v.cik,v.id.alias('src')).where(v.cik.isNotNull())
#acc_id = v.select(v.accession_number,v.id.alias('dst')).where(v.accession_number.isNotNull())
#e_filings_ents = f.join(cik_id,'cik').join(acc_id,'accession_number')
#issuer_cik_id = cik_id.withColumvnRenamed('cik','issuer_cik').withColumnRenamed('src','dst')
#e_owner_rels = r.join(cik_id.withColumnRenamed('cik','owner_cik'),'owner_cik').join(issuer_cik_id,'issuer_cik')
#e = combine_df(e_owner_rels,e_filings_ents).withColumnRenamed('cik','filer_cik')
#swap order of cik and filing date in creating key because accession number is the issuer_cik+filing_date!
e_owner_rels = r.withColumn('src',sqlf.concat(r.filing_date,r.owner_cik)).\
withColumn('dst',sqlf.concat(r.filing_date,r.issuer_cik))
e_filings_ents = f.withColumn('src',sqlf.concat(f.filing_date,f.cik)).\
withColumn('dst',f.accession_number)
e = combine_df(e_owner_rels,e_filings_ents)
g = GraphFrame(v,e)

#be sure to remove the order by when running on the big cluster, it might be more efficient to sort later on a single node?
degrees = g.degrees.orderBy('degree',ascending=False)
g = g.pageRank(tol=0.1)
pr = g.vertices.orderBy('pagerank',ascending=False).show()
ccs = g.connectedComponents()
top_cc =  ccs.groupBy('component').count().orderBy('count',ascending=False).limit(5).collect()
#get degree distr and page range distr for top 5 components

#largest_component = 42949672974#found by inspecting output
for row in top_cc:
    ccs.where(ccs.component == row.component).show()#dump everything we need stats-wise from each of the five largest components


############################## old stuff below #############################
g2 = toGEFX(nodes,edges)
g3 = toGEFX(v,e)
with open('ex2.gexf','w') as ex2,open('ex3.gexf','w') as ex3:
    ex2.write(g2)
    ex3.write(g3)

#Experiment 2: Examine how many trades entities are involved in
#Could we define a mapping using a dict between dataframe types and GEFX types to render these unnecessary or greatly reduce how many need to be entered?

nodes = combine_df(ents,filings).withColumn('id',monotonically_increasing_id())
edges = filings_ents.withColumn('src',filings_ents.accession_number).withColumn('dst',filings_ents.cik).withColumn('id',monotonically_increasing_id())

#Experiment 3: Find the persons involved with the most companies or vice vera
v = ents.withColumn('id',ents.cik)
e = owner_rels.withColumn('src',owner_rels.owner_cik).withColumn('dst',owner_rels.issuer_cik)
#ranks = g.degrees
#ranks = ranks.orderBy(ranks.degree.desc())
