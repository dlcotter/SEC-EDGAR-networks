from pyspark.sql.functions import lit
from pyspark.sql.types import NullType
from re import sub
from datetime import datetime,timezone
from itertools import chain

def combine_df(one,other):
    one_types = {colname:coltype for colname,coltype in one.dtypes}
    other_types = {colname:coltype for colname,coltype in other.dtypes}
    one_with_other = 'one.'+'.'.join((f'withColumn("{colname}",lit(None).cast("{other_types[colname]}"))' for colname in other.columns))
    other_with_one = 'other.'+'.'.join((f'withColumn("{colname}",lit(None).cast("{one_types[colname]}"))' for colname in one.columns))
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

