"""
Load tables into Spark
"""

from __future__ import print_function

from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


def load_SEC_data(spark):
    sc = spark.sparkContext

    # contacts
    lines = sc.textFile("contacts.table")
    fields = lines.map(lambda l: l.split('|'))
    contacts = fields.map(lambda f: Row(cik        = f[1],
                                        filingDate = f[2],
                                        role       = f[3],
                                        street1    = f[4],
                                        street2    = f[5],
                                        street3    = f[6],
                                        city       = f[7],
                                        state      = f[8],
                                        zip        = f[9],
                                        phone      = f[10]))

    contactsSchema = [StructField('cik',        StringType(),False),
                      StructField('filingDate', DateType(),  False),
                      StructField('role',       StringType(),False),
                      StructField('street1',    StringType(),True),
                      StructField('street2',    StringType(),True),
                      StructField('street3',    StringType(),True),
                      StructField('city',       StringType(),True),
                      StructField('state',      StringType(),True),
                      StructField('zip',        StringType(),True),
                      StructField('phone',      StringType(),True)]

    contactsTable = spark.createDataFrame( contacts )
    contactsTable.createOrReplaceTempView("contacts")
    contactsTable.persist()

    # documents
    lines = sc.textFile("documents.table")
    fields = lines.map(lambda l: l.split('|'))
    documents = fields.map(lambda f: Row(accessionNumber = f[1],
                                         formType        = f[2],
                                         filename        = f[3],
                                         format          = f[4],
                                         description     = f[5]))
    documentsTable = spark.createDataFrame( documents )
    documentsTable.createOrReplaceTempView("documents")
    documentsTable.persist()

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
    entitiesTable = spark.createDataFrame( entities )
    entitiesTable.createOrReplaceTempView("entities")
    entitiesTable.persist()

    # filings
    line    = sc.textFile("filings.table")
    fields  = lines.map(lambda l: l.split('|'))
    filings = fields.map(lambda f: Row(accessionNumber = f[1],
                                       subType         = f[2],
                                       docCount        = f[3],
                                       reportingDate   = f[4],
                                       filingDate      = f[5],
                                       changeDate      = f[6]))
    filingsTable = spark.createDataFrame( filings )
    filingsTable.createOrReplaceTempView("filings")
    filingsTable.persist()
    
    # filings_entities
    line    = sc.textFile("filings_entities.table")
    fields  = lines.map(lambda l: l.split('|'))
    filings_entities = fields.map(lambda f: Row(accessionNumber = f[1],
                                                cik             = f[2],
                                                role            = f[3]))
    filings_entitiesTable = spark.createDataFrame( filings_entities )
    filings_entitiesTable.createOrReplaceTempView("filings_entities")
    filings_entitiesTable.persist()
    
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
    owner_relsTable = spark.createDataFrame( owner_rels )
    owner_relsTable.createOrReplaceTempView("owner_rels")
    owner_relsTable.persist()

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
