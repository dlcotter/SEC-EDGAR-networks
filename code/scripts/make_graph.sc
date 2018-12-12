import apache.spark.rdd._
import apache.spark.graphx._

val entities: RDD[(String, String,String,String,String,String,String,String)] =
    sc.textFile("./entities_test.table").map {
                line => val row = line split '|'
                (row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8))
		}
val owner_rels: RDD[(String, String, String,String,String,String,String,String,String)] =
    sc.textFile("./owner_rels_.table").map {
                line => val row = line split '|'
                (row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9))
		}

entities.take(5).show()

owner_rels.take(5).show()
