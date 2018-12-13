from graphframes.graphframe import GraphFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession
import matplotlib
import matplotlib.pyplot as plt

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()
ind = spark.read.parquet('/home/nima/datasets/indegrees')
outd = spark.read.parquet('/home/nima/datasets/outdegrees')
indp = ind.toPandas()
outdp = outd.toPandas()
outdp.hist('outDegree',bins=100,log=True,label='Degree Distribution - outDegree')
plt.xlabel('Degree')
plt.ylabel('Count')
plt.xlim(left=0)
indp.hist('inDegree',bins=100,log=True,label='Degree Distribution - InDegree')
plt.xlabel('Degree')
plt.ylabel('Count')
plt.xlim(left=0)
plt.show()


ccs = spark.read.parquet('/home/nima/datasets/ccs')
cc_counts = ccs.groupBy(ccs.component).count()
cc_cnt_p = cc_counts.toPandas()
cc_cnt_p.hist('count',bins=(tuple(range(0,5000,20))),log=True,label='Size Distribution of Connected Components')#NOTE:outliers were removed!
plt.title('Size Distribution of Connected Components')
plt.xlabel('Vertex count')
plt.ylabel('Number of components')
plt.xlim(left=0)

#zuckerberg's cik:000154870
c = spark.read.csv('allButOne_cc',sep='|',schema='id string,component bigint,pagerank double,blank string').drop('blank')
c.join(ents,on=c.id == ents.cik).select('component','cik','entity_name','pagerank',).distinct().groupBy('component').max('pagerank').orderBy('max(pagerank)',ascending=False).show()



