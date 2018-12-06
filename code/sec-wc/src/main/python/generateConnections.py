"""
Generate connections from the owner_rels table.
"""

from __future__ import print_function

from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

issuerCik = ''
prevOwwner = ''
connections = {}

def selectConnection(transaction):
    if transaction.issuerCik != issuerCik:
        connections = set()
        issuerCik = transaction.issuerCik
    if transaction.ownerCik in connections:
        returnRDD = []
        for otherOwner in connections[transaction.ownerCik]:
            if otherOwner < transaction.ownerCik:
                returnRdd.append(Row(issuerCik,otherOwnerCik,transaction.ownerCik))
            else:
                returnRdd.append(Row(issuerCi,ktransaction.ownerCik,otherOwnerCik))
        connections[transaction.ownerCik].clear()
        return returnRDD
    else:
        for key in connections:
            connections[key].add(transaction.ownerCik)
        return []
    
def generateConnections(spark):
    sc = spark.sparkContext

    # entities
    lines = sc.textFile("entities.table")
    fields = lines.map(lambda l: l.split('|'))
    entities = fields.map(lambda f: Row(cik        = f[1],
                                        filingDate = f[2],
                                        tradingSym = f[3],
                                        name       = f[4],
                                        IRS        = f[5],
                                        SICDesc    = f[6],
                                        SIC        = f[7],
                                        IncState   = f[8],
                                        FisYearEnd = f[9]))
    entitiesSchema = StructType([StructField('cik',        StringType(),False),
                                 StructField('filingDate', StringType(),False),
                                 StructField('tradingSym', StringType(),True),
                                 StructField('name',       StringType(),False),
                                 StructField('IRS',        StringType(),True),
                                 StructField('SICDesc',    StringType(),True),
                                 StructField('SIC',        StringType(),True),
                                 StructField('IncState',   StringType(),True),
                                 StructField('FisYearEnd', StringType(),True)])
    entitiesTable = spark.createDataFrame( entities, entitiesSchema )
    entitiesTable.createOrReplaceTempView("entities")
#    entitiesTable.rdd.saveAsPickleFile("entities.pkl" )


    # filings
    lines    = sc.textFile("filings.table")
    fields  = lines.map(lambda l: l.split('|'))
    filings = fields.map(lambda f: Row(accessionNumber = f[1],
                                       subType         = f[2],
                                       docCount        = f[3],
                                       reportingDate   = f[4],
                                       filingDate      = f[5],
                                       changeDate      = f[6]))
    filingsSchema = StructType([StructField('accessionNumber',  StringType(),False),
                                StructField('subType',          StringType(),False),
                                StructField('docCount',         StringType(),False),
                                StructField('reportingDate',    StringType(),False),
                                StructField('filingDate',       StringType(),False),
                                StructField('changeDate',       StringType(),True)])
    filingsTable = spark.createDataFrame( filings, filingsSchema )
    filingsTable.createOrReplaceTempView("filings")
#    filingsTable.rdd.saveAsPickleFile("filings.pkl")
    
    # filings_entities
    lines    = sc.textFile("filings_entities.table")
    fields  = lines.map(lambda l: l.split('|'))
    filings_entities = fields.map(lambda f: Row(accessionNumber = f[1],
                                                cik             = f[2],
                                                role            = f[3]))
    filings_entitiesSchema = StructType([StructField('accessionNumber', StringType(),False),
                                         StructField('cik'   ,          StringType(),False),
                                         StructField('role',            StringType(),False)])
    filings_entitiesTable = spark.createDataFrame( filings_entities, filings_entitiesSchema )
    filings_entitiesTable.createOrReplaceTempView("filings_entities")
#    filings_entitiesTable.rdd.saveAsPickleFile("filings_entities.pkl")
                           
    # owner_rels
    lines = sc.textFile("owner_rels.table")
    fields = lines.map(lambda l: l.split('|'))
    owner_rels = fields.map(lambda f: Row(issuerCik    = f[1],
                                          rptOwnerCik  = f[2],
                                          filingDate   = f[3],
                                          isDirector   = f[4],
                                          isOfficer    = f[5],
                                          is10pctOwner = f[6],
                                          isOther      = f[7],
                                          officerTitle = f[8]))
    owner_relsSchema = StructType([StructField('issuerCik', StringType(),False),
                                   StructField('rptOwnerCik', StringType(),False),
                                   StructField('filingDate', StringType(),False),
                                   StructField('isDirector', StringType(),True),
                                   StructField('isOfficer', StringType(),True),
                                   StructField('is10pctOwner', StringType(),True),
                                   StructField('isOther', StringType(),True),
                                   StructField('officerTitle', StringType(),True)])
    owner_relsTable = spark.createDataFrame( owner_rels, owner_relsSchema )
    owner_relsTable.createOrReplaceTempView("owner_rels")
#    owner_relsTable.rdd.saveAsTextFile("owner_rels.text")

    transactions = spark.sql("SELECT issuerCik,filingDate,rptOwnerCik FROM owner_rels ORDER BY issuerCik,filingDate,rtpOwnerCik " )
    connections  = transactions.rdd.filter(lambda t: selectConnection(t)).flatten()
    connections.saveAsTextFile("connections.rdd")

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("LoadSECdata") \
        .getOrCreate()
    # $example off:init_session$
    #   .config("spark.some.config.option", "some-value") \

    generateConnections(spark)
    spark.stop()
