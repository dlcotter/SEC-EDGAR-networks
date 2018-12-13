"""
Generate connections from the owner_rels table.
"""

#
# Steve Roggenkamp
#

from __future__ import print_function

# from io.TextIOBase import *
from csv import writer
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import sys

#
# selectConncions porcesses a single set of transactions for a given issueCik
#

def selectConnection(trans_issuer_t):
    issuerCik    = trans_issuer_t[0]  # get the issuerCIK
    transactions = trans_issuer_t[1]  # get the list of transactions for this issuerCIK
    connections  = {}                 # initialize the dictionary
    returnRDD    = []                 # list of connections

    for t in transactions:
        if t.rptOwnerCik in connections:                    # have we seen this CIK before?
            for otherOwner in connections[t.rptOwnerCik]:   # yes, so generate output connections
                if otherOwner < t.rptOwnerCik:              # put the smaller CIK in first position
                    returnRDD.append(Row(issuer=issuerCik,owner1=otherOwner,owner2=t.rptOwnerCik))
                else:
                    returnRDD.append(Row(issuer=issuerCik,owner1=t.rptOwnerCik,owner2=otherOwner))
            connections[t.rptOwnerCik].clear()              # clear the dictionary entry for this owner
        else:                                               # no, we have not seen it
            for key in connections:                         # add this owner to other dictionary entries
                connections[key].add(t.rptOwnerCik)
            connections[t.rptOwnerCik] = set()              # create an entry for this owner
    return returnRDD

#
# Generate a Row from the owner_rels table
#
def genRow(f):
    return Row(issuerCik    = f[1],
               rptOwnerCik  = f[2],
               filingDate   = f[3],
               isDirector   = f[4],
               isOfficer    = f[5],
               is10pctOwner = f[6],
               isOther      = f[7],
               officerTitle = f[8])

#
# generateConnections - read an owner_rels file and generate the connections based on transactions
#                       represented in the data
def generateConnections(spark, inFile):
    sc = spark.sparkContext

    # read owner_rels and generate a table from it
    lines = sc.textFile(inFile)
    fields = lines.map(lambda l: l.split('|'))
    owner_rels = fields.filter( lambda f: f[1]).map(lambda f: genRow(f))
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

    # generate a list of transactions from the table using only transactions which
    # involve rows having isDirector or isOfficer set
    
    transactions = spark.sql("SELECT issuerCik,filingDate,rptOwnerCik FROM owner_rels WHERE isDirector = 1 OR isOfficer = 1 ORDER BY issuerCik,filingDate,rptOwnerCik" )

    # group transactions by issuer, returring an dict with issueCik as key
    txByIssuer  = transactions.rdd.groupBy(lambda t: t.issuerCik)

    # iterate over groups of transactions finding connections
    cxs = txByIssuer.flatMap(lambda t: selectConnection(t))

    # collect the connections
    connections = cxs.map(lambda c: (c.owner1,c.owner2,c.issuer)).collect()
    return connections

# Main entry point

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Generate Connections") \
        .getOrCreate()

    # argument containing name of owner_rels table
    if len(sys.argv) > 1:
        inFile = sys.argv[1]
    else:
        inFile = "owner_rels.table"

    # argument containing name of output file
    if len(sys.argv) > 2:
        outFile = sys.argv[2]
    else:
        outFile = "connections.table"

    # generate the connections
    connections = generateConnections(spark,inFile)

    # and write them out
    with open("connections.table","w") as ofile:
        csvwriter = writer(ofile, delimiter='|',lineterminator='\n')
        csvwriter.writerows(connections)
