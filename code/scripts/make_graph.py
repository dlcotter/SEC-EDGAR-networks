"""
Make a graph from the connections and entities.
"""

from datetime import datetime,timezone
from itertools import chain,count
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import lit,monotonically_increasing_id 
from pyspark.sql.types import *
from re import sub
# from graphframes.graphframe import GraphFrame
import sys

def genConnectionRow(f):
    return Row( owner1Cik=f[0],
                owner2Cik=f[1],
                issuerCik=f[2])

def genEntityRow(f):
    return Row( cik=f[1],
                filingDate=f[2],
                tradingSym=f[3],
                name      =f[4],
                IRSnumber =f[5],
                SICDesc   =f[6],
                SIC       =f[7],
                IncState  =f[8],
                FisYearEnd=f[9])

def readFile(spark, inFile, fcn, schema, name ):
    lines     = spark.sparkContext.textFile(inFile)
    fields    = lines.map(lambda l: l.split('|'))
    datarows  = fields.map(lambda f: fcn(f))
    dataFrame = spark.createDataFrame( datarows, schema )
    dataFrame.createOrReplaceTempView(name)
    return dataFrame
    
if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
            .builder \
            .appName("Generate graph") \
            .getOrCreate()
    # $example off:init_session$
    #   .config("spark.some.config.option", "some-value")

    connectionSchema = StructType([StructField('owner1Cik', StringType(),False),
                                   StructField('owner2Cik', StringType(),False),
                                   StructField('issuerCik', StringType(),False)])

    entitySchema     = StructType([StructField('cik',        StringType(),False),
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
