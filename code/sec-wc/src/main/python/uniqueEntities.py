"""
uniqueEntities.py - Eliminate multiple entities.

We have multiple records of enitties with the number depending on the
number of filings.  This program selects the latest record in the set
for each entity.
"""

# uniqueEntities.py - Eliminate duplicate entities, saving the latest
#
# We use this to keep only one record per entity CIK, keeping the latest
# one we have encountered to reduce multiple records per CIK.
#
# Steve Roggenkamp
#
from csv import writer
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def uniqueEntities(spark):
    sc = spark.sparkContext

    # read the table in
    lines = sc.textFile("entities.data")
    fields = lines.map(lambda l: l.split('|'))
    entities = fields.map(lambda f: (f[1],(f[1],f[2],f[3],f[4],f[5],f[6],f[7],f[8],f[9])))

    # group by CIK and select the one with the latest date
    minimumEntities = entities.groupByKey().map(lambda x: (x[0],max(x[1]))).map(lambda x: x[1]).collect()

    # write it out
    with open("entities_unique.table","w") as of:
        csvwriter = writer(of, delimiter="|")
        csvwriter.writerows(minimumEntities)

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Unique Entities") \
        .getOrCreate()
    uniqueEntities(spark)
