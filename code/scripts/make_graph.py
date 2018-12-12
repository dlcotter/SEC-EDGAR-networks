"""
Make a graph from the connections and entities.
"""

from datetime import datetime,timezone
from functools import reduce
from graphframes.graphframe import GraphFrame
from itertools import chain,count
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import lit,monotonically_increasing_id 
from pyspark.sql.types import *
from re import sub
import sys

def genConnectionRow(f):
    return Row( src=f[0],
                dst=f[1],
                issuerCik=f[2])

def genEntityRow(f):
    return Row( id=f[0],
                filingDate=f[1],
                tradingSym=f[2],
                name      =f[3],
                IRSnumber =f[4],
                SICDesc   =f[5],
                SIC       =f[6],
                IncState  =f[7],
                FisYearEnd=f[8])

def readFile(spark, inFile, fcn, schema, name ):
    lines     = spark.sparkContext.textFile(inFile)
    fields    = lines.map(lambda l: l.split('|'))
    datarows  = fields.map(lambda f: fcn(f))
    dataFrame = spark.createDataFrame( datarows, schema )
    dataFrame.createOrReplaceTempView(name)
    return dataFrame


def makeGraph(entities, connections):
    return GraphFrame( entities, connections )

    
if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
            .builder \
            .appName("Generate graph") \
            .getOrCreate()
    # $example off:init_session$
    #   .config("spark.some.config.option", "some-value")

    connectionSchema = StructType([StructField('src',  StringType(),False),
                                   StructField('dest', StringType(),False),
                                   StructField('issuerCik', StringType(),False)])

    entitySchema     = StructType([StructField('id',        StringType(),False),
                                   StructField('filingDate', StringType(),False),
                                   StructField('tradingSym', StringType(),True),
                                   StructField('name',       StringType(),False),
                                   StructField('IRSnumber',  StringType(),True),
                                   StructField('SICDesc',    StringType(),True),
                                   StructField('SIC',        StringType(),True),
                                   StructField('IncState',   StringType(),True),
                                   StructField('FisYearEnd', StringType(),True)])
    
    if len(sys.argv) > 1:
        cnxFile = sys.argv[1]
    else:
        cnxFile = "connections.table"

    if len(sys.argv) > 2:
        entFile = sys.argv[2]
    else:
        entFile = "entities.table"

    if len(sys.argv) > 3:
        outFile = "graphs.gefx"
    else:
        outFile = sys.argv[3]

    connections = readFile(spark,cnxFile,genConnectionRow,connectionSchema, "connections")
    entities    = readFile(spark,entFile,genEntityRow,    entitySchema,     "entities")

    secgraph = GraphFrame(entities,connections)
    secgraph.degrees.sort("id").show(20,False)
    
#    for e in entities.take(5):
#        print(str(e))
    
#    g = makeGraph(entities, connections)
