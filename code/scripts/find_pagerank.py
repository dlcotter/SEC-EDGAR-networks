from graphframes.graphframe import GraphFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession


def write_collected_df(path,df):
    with open(path,'w') as f:
        for row in df.collect():
            f.write("|".join((str(i) for i in row)))
            f.write('\n') 

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()
sc.setCheckpointDir('/opt/data/spark_checkpoints')
v = spark.read.parquet('ccs').select('id','component')
e = spark.read.parquet('g_edges').select('src','dst')
g = GraphFrame(v,e).cache()
#2293 is second-largest
f = g.filterVertices(g.vertices.component != 0).cache()
f.pageRank(maxIter=1).vertices.cache()

v.groupBy(v.component).count().orderBy('count',ascending=False).show()

