// Not a Scala script so much as a catalog of working queries for analysis...

data("contacts")
// org.apache.spark.sql.DataFrame = [cik: int, filing_date: timestamp ... 8 more fields]

data("contacts").show()
// cik|filing_date|contact_type|street1|street2|street3|city|state|zip|phone
// 0000000020|2003-05-23|issuer|ROUTE 55 & 553|||PITMAN|NJ|08071-0888|8562563318
// 0000000020|2003-06-09|issuer|ROUTE 55 & 553|||PITMAN|NJ|08071-0888|8562563318

data("contacts").
	select("filing_date","cik").
	filter(row => row(1) == "2004-08-26 00:00:00").
	show()

data("contacts").
	select("contact_type").
	distinct().
	show()
// +------------+
// |contact_type|
// +------------+
// |      issuer|
// |       owner|
// +------------+

data("contacts").
	filter(rec => rec(6) == "PITMAN" & rec(7) == "NJ").
	count()
// Long = 307

data("contacts").
	first.
	fieldIndex("cik")
// Int = 0
 
data("contacts").
	first.
	fieldIndex("filing_date")
// Int = 1

// Uses the following (completely unintuitive) scheme for initializing:
//    year - the year minus 1900
//    month - 0 to 11
//    date - 1 to 31
//    hour - 0 to 23
//    minute - 0 to 59
//    second - 0 to 59
//    nano - 0 to 999,999,999
val today = new Date(118,10,23)
val now = new Timestamp(118,10,23,0,0,0,0)

data("contacts").
	filter($"filing_date" < today).
	count()
// Long = 4581575

data("contacts").
	filter($"filing_date" > today).
	count()
// Long = 0
// note: $"column_name" in the above is shorthand for spark.implicits.StringToColumn("filing_date")

// Oddly, this gets an error:
 data("owner_rels").
	filter($"is_director" == 1)

// but this does not:
data("owner_rels").
	filter($"is_director" > 0)

// Most numerous owners
data("owner_rels")
	.filter($"isDirector" > 0)
	.groupBy("rptOwnerCik")
	.count()
	.orderBy(desc("count"))
	.show(10)
// +----------+-----+
// | owner_cik|count|
// +----------+-----+
// |0000898860| 1665|
// |0001294693| 1616|
// |0001037854|  895|
// |0001106578|  705|
// |0001033331|  639|
// |0001242914|  605|
// |0001008357|  587|
// |0001185533|  546|
// |0001166334|  495|
// |0000937797|  446|
// +----------+-----+

// Look up the most frequently trading owner:
data("contacts")
	.filter(row => row(1) == 898860)
	.distinct()
	.show()

scala> data("connections")
	.groupBy("owner1")
	.count()
	.orderBy(desc("count"))
	.show()
// +-------+-----+
// | owner1|count|
// +-------+-----+
// | 929408|  538|
// |1028689|  445|
// |1087939|  379|
// |1087940|  353|
// |1009139|  336|
// | 937797|  334|
// |1032669|  327|
// |1351069|  326|
// |1168019|  322|
// |1025094|  320|
// |1099636|  313|
// |1351073|  305|
// |1013947|  299|
// |1159187|  294|
// |1176400|  294|
// |1024354|  289|
// |1075522|  285|
// |1055951|  285|
// |1047251|  281|
// |1179631|  278|
// +-------+-----+

// Count of owner_rels
data("owner_rels").count()
// res0: Long = 3188691

// What is the most common type of insider in insider trading?
data("owner_rels").
	agg(	sum($"is_director"),
		sum($"is_officer"),
		sum($"is_10_percent_owner"),
		sum($"is_other")).show()
// +----------------+---------------+------------------------+-------------+
// |sum(is_director)|sum(is_officer)|sum(is_10_percent_owner)|sum(is_other)|
// +----------------+---------------+------------------------+-------------+
// |       1594577.0|      1582545.0|                414954.0|     129549.0|
// +----------------+---------------+------------------------+-------------+

// What is the most common type of officer?
data("owner_rels").
	filter($"is_officer" > 0).
 	groupBy("officer_title").
	count().
 	orderBy(desc("count")).
 	show(10)
// +--------------------+-----+
// |       officer_title|count|
// +--------------------+-----+
// |                 CEO|70105|
// |Executive Vice Pr...|60777|
// |Senior Vice Presi...|51084|
// |   President and CEO|49022|
// |Chief Financial O...|48128|
// |Chief Executive O...|44465|
// |      Vice President|40085|
// |                 CFO|35047|
// |           President|26048|
// |    Chairman and CEO|20671|
// +--------------------+-----+

data("owner_rels").
	select($"issuer_cik").
	distinct().
	show(5)
// +----------+
// |issuer_cik|
// +----------+
// |     18498|
// |     28146|
// |     29834|
// |     47217|
// |     73048|
// +----------+

data("entities").
	filter(rec => rec(0) == 18498).
	select($"entity_name").
	show(1)
// +-----------+
// |entity_name|
// +-----------+
// |GENESCO INC|
// +-----------+

data("owner_rels").
	select($"owner_cik").
	distinct().
	show(5)
// +----------+
// | owner_cik|
// +----------+
// |0001424402|
// |0001239127|
// |0001197542|
// |0001126562|
// |0001234608|
// +----------+

data("entities").
	filter(rec => rec(0) == 1424402).
	select($"entity_name").
	show(1)
// +-----------+
// |entity_name|
// +-----------+
// | PACE PETER|
// +-----------+

data("entities").
	filter(rec => rec(0) == 1424402).
	select($"*").
	show(1)
// +-------+--------------+-----------+-----------+---------+----+----------+------------+---------------+
// |    cik|trading_symbol|entity_name|entity_type|irsnumber| sic|sic_number|state_of_inc|fiscal_year_end|
// +-------+--------------+-----------+-----------+---------+----+----------+------------+---------------+
// |1424402|          null| PACE PETER|      owner|     null|null|      null|        null|           null|
// +-------+--------------+-----------+-----------+---------+----+----------+------------+---------------+

data("contacts").
	filter(rec => rec(0) == 1424402).
	select($"*").
	show(1)
// +-------+-------------------+------------+--------------------+-------+-------+-------------+-----+-----+----------+
// |    cik|        filing_date|contact_type|             street1|street2|street3|         city|state|  zip|     phone|
// +-------+-------------------+------------+--------------------+-------+-------+-------------+-----+-----+----------+
// |1424402|2008-01-22 00:00:00|       owner|4695 MACARTHUR COURT|   null|   null|NEWPORT BEACH|   CA|92660|9499751550|
// +-------+-------------------+------------+--------------------+-------+-------+-------------+-----+-----+----------+

// I sorted the connections by greatest number of issuers per owner-owner pair to least and found that there are
// lots of owners who have dozens of connections to other owners but with different issuers. That sounds to me
// like pairs of insiders trading stock while employed at dozens of different companies.
scala> data("connections")
	.groupBy("owner1","owner2")
	.count()
	.orderBy(desc("count"))
	.show()
// +-------+-------+-----+
// | owner1| owner2|count|
// +-------+-------+-----+
// |1059243|1075522|   38|
// |1059243|1333739|   36|
// |1075524|1333739|   35|
// |1075522|1075524|   34|
// |1075522|1358150|   32|
// |1075522|1333739|   32|
// |1141904|1333739|   31|
// |1059243|1326138|   30|
// |1059243|1075524|   29|
// |1075522|1141904|   28|
// |1075524|1141904|   28|
// |1059243|1141904|   27|
// |1075522|1326138|   24|
// |1075522|1162744|   24|
// |1326138|1333739|   23|
// |1075524|1162744|   23|
// |1075522|1283051|   23|
// |1141904|1162744|   22|
// |1075524|1283051|   22|
// |1162744|1358150|   22|
// +-------+-------+-----+

df.dtypes
df.show()
df.printSchema()
df.groupBy("state").count().show()
df.describe().show()
df.describe().getClass()

df.createOrReplaceTempView("contacts")

val lines = sc.textFile("contacts.csv")
val records = lines.map(_.split("|"))
val filtered = records.filter(rec => rec(3) == "issuer")
val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
val tuples = filtered.map(rec => (rec(0), date_format.parse(rec(1))))
val minDate = tuples.reduceByKey((a,b) => a < b)
*/
