"""
Generate connections from the owner_rels table.
"""

from __future__ import print_function

# from io.TextIOBase import *
from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

issuerCik = ''
prevOwwner = ''
connections = {}

def selectConnection(transaction):
    global issuerCik,prevOwner,connections
    
    if transaction.issuerCik != issuerCik:
        connections = {} 
        issuerCik = transaction.issuerCik
    ownerCik = transaction.rptOwnerCik
    if ownerCik in connections:
        returnRDD = []
        for otherOwner in connections[ownerCik]:
            if otherOwner < ownerCik:
                returnRDD.append(Row(issuer=issuerCik,owner1=otherOwner,owner2=ownerCik))
#                print(issuerCik+"|"+otherOwner+"|"+ownerCik)
            else:
                returnRDD.append(Row(issuer=issuerCik,owner1=ownerCik,owner2=otherOwner))
#                print(issuerCik+"|"+transaction.rptOwnerCik+"|"+otherOwner)
        connections[ownerCik].clear()
        return returnRDD
    else:
        for key in connections:
            connections[key].add(transaction.rptOwnerCik)
        connections[transaction.rptOwnerCik] = set()
        return None
    
def generateConnections(spark):
    sc = spark.sparkContext

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

    transactions = spark.sql("SELECT issuerCik,filingDate,rptOwnerCik FROM owner_rels ORDER BY issuerCik,filingDate,rptOwnerCik " )
    connections  = transactions.rdd.flatMap(lambda t: selectConnection(t)).collect()
    for r in connections:
      ofile.write(str(r)+"\n")
    ofile.close()

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("LoadSECdata") \
        .getOrCreate()
    # $example off:init_session$
    #   .config("spark.some.config.option", "some-value") \

    generateConnections(spark)
