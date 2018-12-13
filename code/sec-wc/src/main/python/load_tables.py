"""
Load tables into Spark
"""

# Script written to insure we have everything working with Spark
# and can read our data into it.
#
# Steve Roggenkamp
#

from __future__ import print_function

from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def load_SEC_data(spark):
    sc = spark.sparkContext

    # contacts
    lines = sc.textFile("contacts_100.table")
    fields = lines.map(lambda l: l.replace('contacts|','').split('|'))
    contacts = fields.map(lambda f: Row(cik          = f[1],
                                        filingDate   = f[2],
                                        role         = f[2],
                                        street1      = f[3],
                                        street2      = f[4],
                                        street3      = f[5],
                                        city         = f[6],
                                        state        = f[7],
                                        zip          = f[8],
                                        phone        = f[9]))
    contactsSchema = StructType([StructField('cik',        StringType(),False),
                                 StructField('filingDate', StringType(),False),
                                 StructField('role',       StringType(),False),
                                 StructField('street1',    StringType(),True),
                                 StructField('street2',    StringType(),True),
                                 StructField('street3',    StringType(),True),
                                 StructField('city',       StringType(),True),
                                 StructField('state',      StringType(),True),
                                 StructField('zip',        StringType(),True),
                                 StructField('phone',      StringType(),True)])

    contactsTable = spark.createDataFrame( contacts, contactsSchema )
    contactsTable.createOrReplaceTempView("contacts")
#    contactsTable.rdd.saveAsPickleFile("contacts.pkl" )
 
    # documents
    lines = sc.textFile("documents_100.table")
    fields = lines.map(lambda l: l.split('|'))
    documents = fields.map(lambda f: Row(accessionNumber = f[1],
                                         formType        = f[2],
                                         filename        = f[3],
                                         format          = f[4],
                                         description     = f[5]))
    documentsSchema = StructType([StructField('accessionNumber', StringType(), False),
                                  StructField('formType',        StringType(), False),
                                  StructField('filename',        StringType(), True),
                                  StructField('format',          StringType(), True),
                                  StructField('description',     StringType(), True)])
                       
    documentsTable = spark.createDataFrame( documents, documentsSchema )
    documentsTable.createOrReplaceTempView("documents")
 #   documentsTable.rdd.saveAsPickleFile("documents.pkl" )
        
    

    # entities
    lines = sc.textFile("entities_100.table")
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
    lines    = sc.textFile("filings_100.table")
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
    lines    = sc.textFile("filings_entities_100.table")
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
    lines = sc.textFile("owner_rels_100.table")
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

    answers =  spark.sql("SELECT count(*) from contacts" )
    numAns = answers.rdd.map(lambda p: "Number of contacts: "+str(p)).collect()
    for n in numAns:
        print(n)

    answers =  spark.sql("SELECT count(*) from documents" )
    numAns = answers.rdd.map(lambda p: "Number of documents: "+str(p)).collect()
    for n in numAns:
        print(n)

    answers =  spark.sql("SELECT count(*) from entities" )
    numAns = answers.rdd.map(lambda p: "Number of entities: "+str(p)).collect()
    for n in numAns:
        print(n)

    answers =  spark.sql("SELECT count(*) from filings" )
    numAns = answers.rdd.map(lambda p: "Number of filings: "+str(p)).collect()
    for n in numAns:
        print(n)

    answers =  spark.sql("SELECT count(*) from filings_entities" )
    numAns = answers.rdd.map(lambda p: "Number of filings_entities: "+str(p)).collect()
    for n in numAns:
        print(n)

    answers =  spark.sql("SELECT count(*) from owner_rels" )
    numAns = answers.rdd.map(lambda p: "Number of owner_rels: "+str(p)).collect()
    for n in numAns:
        print(n)


#  The following crashes the Executor processes with out of memory error
#    answers = spark.sql("SELECT A.accessionNumber, A.reportingDate, C.name, B.role, E.name, D.role FROM filings A INNER JOIN filings_entities B ON B.accessionNumber = A.accessionNumber INNER JOIN entities C ON C.cik = B.cik AND C.filingDate = A.filingDate INNER JOIN filings_entities D on D.accessionNumber = A.accessionNumber INNER JOIN entities E on E.cik = D.cik and E.filingDate = A.filingDate WHERE E.cik != C.cik ORDER BY A.accessionNumber, A.reportingDate" )
#    print("Accession Numbers with owners and roles")
#    numAns = answers.rdd.map(lambda p: str(p)).collect()
#    for n in numAns:
#        print(n)



if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("LoadSECdata") \
        .getOrCreate()
    # $example off:init_session$
    #   .config("spark.some.config.option", "some-value") \

    load_SEC_data(spark)
    spark.stop()
