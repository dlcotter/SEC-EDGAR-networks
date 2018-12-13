import java.sql.{Date,Timestamp}
import java.text.DateFormat
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
// import spark.implicits._
import scala.util.Try

// The complete list of tables
// val files = List("contacts",
//                  "entities",
//		    "filings_entities",
//		    "documents",
//		    "filings",
//		    "owner_rels",
//		    "connections",
//		    "surnames",
//		    "female_names",
//		    "male_names")

// use only these for now
val files = List("entities","owner_rels","surnames")

val dataframes =
        files.map(f => spark.read.
			format("csv").
			option("delimiter","|").
			option("header","true").
			option("inferSchema","true").
			load("data/" + f + ".csv"))
val data = (files zip dataframes).toMap
val entities: DataFrame = data("entities")
val owner_rels: DataFrame = data("owner_rels")
val surnames: DataFrame = data("surnames")

// create table views
entities.createOrReplaceTempView("entities")
surnames.createOrReplaceTempView("surnames")
owner_rels.createOrReplaceTempView("owner_rels")

// find individuals assuming only individuals can be a director or officer
val individuals = spark.sql("SELECT DISTINCT A.cik,A.name FROM entities A INNER JOIN owner_rels B on A.cik = B.rptOwnerCik WHERE B.isOfficer = '1' or isDirector = '1'")

individuals.createOrReplaceTempView("individuals")

// define a User Defined Function to extract the surnames
val get_first = udf((xs: Seq[String]) => Try(xs.head).toOption)

// pull the last names from entity names
val last_names = individuals.withColumn( "last_names", get_first(split(col("name")," ")))

// and create a view for it
last_names.createOrReplaceTempView("last_names")

// join the census data with the corporate entity names
val individual_race_prob = spark.sql("SELECT B.cik,A.name,A.pctwhite,A.pctblack,A.pctapi,A.pctaian,A.pct2prace,A.pcthispanic FROM surnames A INNER JOIN last_names B on A.name = B.last_names")
individual_race_prob.createOrReplaceTempView("individual_race_prob")

// make a view for it
individual_race_prob.createOrReplaceTempView("individual_race_prob")

// sum it all up
val race_prob = spark.sql("SELECT avg(pctwhite) as white, avg(pctblack) as black, avg(pctapi) as api, avg(pctaian) as AIAN, avg(pct2prace) as multi, avg(pcthispanic) as hispanic from individual_race_prob")

// and show it
race_prob.show()

// TODO: extend this for SIC groups