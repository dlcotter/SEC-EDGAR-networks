import java.sql.{Date,Timestamp}
import java.text.DateFormat
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import spark.implicits._

val files = List("contacts","entities","filings_entities","documents","filings","owner_rels","connections")
val dataframes =
        files.map(f => spark.read.
			format("csv").
			option("delimiter","|").
			option("header","true").
			option("inferSchema","true").
			load("data/" + f + ".csv"))
val data = (files zip dataframes).toMap
val contacts: RDD[Row] = data("contacts").rdd
val connections: RDD[Row] = data("connections").rdd
val entities: RDD[Row] = data("entities").rdd
val filings_entities: RDD[Row] = data("filings_entities").rdd
val documents: RDD[Row] = data("documents").rdd
val filings: RDD[Row] = data("filings").rdd
val owner_rels: RDD[Row] = data("owner_rels").rdd

//val edges: RDD[Edge[String]] = owner_rels.map(rec => Edge(rec(0).toString().toLong, rec(1).toString().toLong, rec(3).toString))
//val vertices: RDD[(VertexId, (String, String))] = entities.map(rec => (rec(0).toString().toLong, (rec(3).toString(), rec(2).toString())))

val edges: RDD[Edge[String]] = connections.map(rec => Edge(rec(0).toString().toLong, rec(1).toString().toLong))
val vertices: RDD[(VertexId, (String, String))] = entities.map(rec => (rec(0).toString().toLong, (rec(3).toString(), rec(2).toString())))
val graph = Graph(vertices, edges)


