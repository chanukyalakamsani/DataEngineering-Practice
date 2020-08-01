// Databricks notebook source
// MAGIC %md
// MAGIC ** Example 1.1 - Word count example **

// COMMAND ----------

// Step 1: Upload a random text file to DBFS(DataBrick File System) and read that as an RDD[String]
val textRdd:RDD[String] = sc.textFile("/FileStore/tables/sample.txt")

// Step 2: Split each record/entry of the RDD through " " delimeter.
// A function that takes a String value as an parameter, splits it through " " and return the splitted words as an Array[String]
def strSplit(str:String): Array[String] = {
  return str.split(" ")
}
// Apply the above function to each record of the RDD and flatten it
val wordsRdd:RDD[String] = textRdd.flatMap(strSplit)

// Step 3: Covert each record or word into a (word, 1) pair
// A function that takes a word of type String value as an parameter and returns a (word, 1) pair
def wordPair(word: String): (String, Int) = {
  return (word, 1)
}
// Apply the above function to each record of the RDD and convert them into (word, 1) pair
val wordsCounterRdd:RDD[(String, Int)] = wordsRdd.map(wordPair)

// Step 4: Aggregate each key, i.e. the words, and calculate the sum of the counters, i.e. 1's 
// Calculating sum of counters for each key, i.e, word
def sum(accumulatedVal: Int, currentVal: Int): Int = {
  return (accumulatedVal + currentVal)
}
// Apply the above sum() function to calculate the frequency of each words.
val wordCountRdd:RDD[(String, Int)] = wordsCounterRdd.reduceByKey(sum)

wordCountRdd.collect()

// COMMAND ----------

val logLine = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"
val pattern = """^([\d.]+) (\S+) (\S+) \[(.*)\] \"(.+?)\" (\d{3}) (\d+) \"(\S+)\" \"([^\"]+)\"$""".r
val matched = pattern.findFirstMatchIn(logLine)
(^[0-9]*) ([0-9]*) ([0-9]*) ([0-9]*) ([0-9]*) ([^ ]*) ([^ ]*)   ([^ ]*)   ([0-9]*) ([0-9]*) ([0-9]*) ([^ ]*)(?:[:]) ([0-9]+.[0-9]+|)(?:ms |)([^ ]*)(?:[:])

// COMMAND ----------

case class Demographic ("Int")

// COMMAND ----------

case class Demo1 (id:Int, 
                   country:String)

// COMMAND ----------

 val df = Seq(("Rajesh", 21, "Delhi"), ("Rajesh", 21, "Mumbai"),("chanukya",23,"Hyd")).toDF("Name", "Age","place")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.expressions._


// COMMAND ----------

val window = Window.partitionBy("Name").orderBy("Age")
val result = df.filter("Age is not null")
  .withColumn("rank", count("Name").over(window))
  

// COMMAND ----------

result.select("*").where("rank <= 1").show()

// COMMAND ----------

val x = Seq((1,"2020-01-01 10:10:10"),
(2,"2020-01-01 10:20:10"),
(3,"2020-01-01 10:55:10"),
(4,"2020-01-01 11:10:10"),
(5,"2020-01-01 12:20:10"),
(6,"2020-01-01 12:55:10"))

// COMMAND ----------

val df = x.toDF("id", "Time")

// COMMAND ----------

df.show()

// COMMAND ----------

df.withColumn("Duration",to_timestamp(col("Time"),"yyyy-MM-dd HH:mm:ss").cast(LongType)-
to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss").cast(LongType)).show()

//or by using unix_timestamp function

// df.withColumn("Duration",unix_timestamp(col("time_dest"),"dd/MM/yyyy HH:mm").cast(LongType)-unix_timestamp(col("time"),"dd/MM/yyyy HH:mm").cast(LongType)).show()

// COMMAND ----------

 val df2 = df.select(col("*"),split(col("Time")," ").getItem(0).as("Date"),
    split(col("Time")," ").getItem(1).as("Time_IST"))

// COMMAND ----------

val df3 = df2.drop(col("Time"))

// COMMAND ----------

df3.show()

// COMMAND ----------

df3.printSchema()

// COMMAND ----------

var ddf = df3.select(col("*"),
    date_format(col("Time_IST"),"HH:mm:ss.SSS").as("time")).drop("Time_IST")


// COMMAND ----------

val w = Window.orderBy("date")  

val leadDf = ddf.withColumn("Lag", lag("time", 1, 0).over(w))




// COMMAND ----------



// COMMAND ----------

// reading in rdd

val txting = sc.textFile("/FileStore/tables/demographic.csv")
val final1 = txting.map(value => value.split(" ")).map(x => (x(0),x(4)))
//final1.foreach(println)


// COMMAND ----------


val txt = spark
  .read
  .format("csv")
  .option("mode", "PERMISSIVE").header(sche).load("/FileStore/tables/demographic.csv")
txt.show(3)

// COMMAND ----------

"""(^[0-9]*) ([0-9]*) ([0-9]*) ([0-9]*) ([0-9]*) ([^ ]*) ([^ ]*)   ([^ ]*)   ([0-9]*) ([0-9]*) ([0-9]*) ([^ ]*)(?:[:]) ([0-9]+.[0-9]+|)(?:ms |)([^ ]*)(?:[:]) (.*$)"""r

// COMMAND ----------

r

val input: String =
  199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245""".split(" ")

String(0)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.2 - Creating RDD with parallelize() function **

// COMMAND ----------

val numOneToFiveRdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
numOneToFiveRdd.getNumPartitions

// COMMAND ----------

sc.defaultParallelism

// COMMAND ----------

val numOneToFiveRddParts: RDD[Array[Int]] = numOneToFiveRdd.glom()
numOneToFiveRddParts.collect()

// COMMAND ----------

numOneToFiveRdd.collect()

// COMMAND ----------

val biggerRdd: RDD[Int] = sc.parallelize(1 to 1000)
biggerRdd.take(5)

// COMMAND ----------

biggerRdd.getNumPartitions

// COMMAND ----------

val defPartRdd: RDD[Array[Int]] = biggerRdd.glom() // 1000 / 8 = 125

def getArraySize(arr: Array[Int]): Int = {
  return arr.length
}
val recCountRdd: RDD[Int] = defPartRdd.map(getArraySize)
recCountRdd.collect()

// COMMAND ----------

val defPartRdd: RDD[Array[Int]] = biggerRdd.repartition(6).glom() // 1000 / 6 = 166

def getArraySize(arr: Array[Int]): Int = {
  return arr.size
}
val recCountRdd: RDD[Int] = defPartRdd.map(getArraySize)
recCountRdd.collect()

// COMMAND ----------

val defPartRdd: RDD[Array[Int]] = biggerRdd.coalesce(6).glom() // 1000 / 6 = 166

def getArraySize(arr: Array[Int]): Int = {
  return arr.size
}
val recCountRdd: RDD[Int] = defPartRdd.map(getArraySize)
recCountRdd.collect()

// COMMAND ----------

biggerRdd.count()

// COMMAND ----------

biggerRdd.getNumPartitions

// COMMAND ----------

//biggerRdd.repartition(10).saveAsTextFile("/FileStore/tables/ten")
sc.textFile("/FileStore/tables/biggerRdd/ten").getNumPartitions


// COMMAND ----------

//biggerRdd.repartition(4).saveAsTextFile("dbfs:/FileStore/tables/biggerRdd/four")
sc.textFile("dbfs:/FileStore/tables/biggerRdd/four").getNumPartitions


// COMMAND ----------

val txnList: List[String] = List("txn01~1000", "txn02~2000", "txn03~1000", "txn04~2000")
val txnRdd: RDD[String] = sc.parallelize(txnList)

def f(T: String): Array[String] = {
  return T.split("~")
}
val splittedRdd: RDD[Array[String]] = txnRdd.map(f) // f: (T) ⇒ U

def getAmount(T: Array[String]): Double = {
  return T(1).toDouble
}
val amountRdd: RDD[Double] = splittedRdd.map(getAmount)

def sum(n1: Double, n2: Double): Double = {
  return n1 + n2
}
val totalAmount: Double = amountRdd.reduce(sum) // f: (T, T) ⇒ T  // 1000, 2000, 1000, 2000
//amountRdd.take(4)
totalAmount

// COMMAND ----------

val txnRdd: RDD[String] = sc.textFile("/FileStore/tables/cred_txn.csv")

def removeHeader(rec: String): Boolean = {
  return !rec.contains("AccNum~Amount~Dat")
}
val filteredRdd: RDD[String] = txnRdd.filter(removeHeader)

def stringSplit(rec: String): Array[String] = {
  return rec.split("~")
}
val splittedRdd: RDD[Array[String]] = filteredRdd.map(stringSplit)

def getAmount(rec: Array[String]): Double = {
  return rec(1).toDouble
}
val amountRdd: RDD[Double] = splittedRdd.map(getAmount)

def sum(n1: Double, n2: Double): Double = {
  return n1 + n2
}
val totalAmount: Double = amountRdd.reduce(sum)
//amountRdd.take(5)
totalAmount



// COMMAND ----------

val x: Int = 10
val y = 10

// COMMAND ----------

sc.textFile("/FileStore/tables/cred_txn.csv")  // RDD[String]
  .filter(rec => !rec.contains("AccNum~Amount~Dat"))  // RDD[String]
  .map(rec => rec.split("~"))  // RDD[Array[String]]
  .map(rec => rec(1).toDouble) // RDD[Double]
  .reduce((n1, n2) => n1 + n2)  // Double



// COMMAND ----------

sc.textFile("/FileStore/tables/cred_txn.csv")  // RDD[String]
  .filter(rec => !rec.contains("AccNum~Amount~Date~Categ"))  // RDD[String]
  .map(rec => rec.split("~"))  // RDD[Array[String]]
  .map(rec => rec(0)) // RDD[String]
  .distinct() // RDD[String]
  .count()


// COMMAND ----------

val txnRdd: RDD[String] = sc.textFile("/FileStore/tables/finances.csv")

def stringSplit(line: String): Array[String] = {
  return line.split(",")
}
val splittedRdd: RDD[Array[String]] = txnRdd.map(stringSplit)

def getAmount(arr: Array[String]): Double = {
  return arr(4).toDouble
}
val amountRdd: RDD[Double] = splittedRdd.map(getAmount)

def sum(n1: Double, n2: Double): Double = {
  return n1 + n2
}
val totalAmount: Double = amountRdd.reduce(sum)
//amountRdd.take(5)
totalAmount

// COMMAND ----------

// Homework : which is most prefered channel, KC_Extract_1_2017

// COMMAND ----------

sc.textFile("/FileStore/tables/KC_Extract_1_20171009.csv") //RDD[string]
  .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
  .map( rec => rec.split('|')) // RDD[Array[string]]
//   .map( rec => !rec.contains("REGIS_CNTRY_CODE, REGIS_CTY_CODE, REGIS_ID, REGIS_LTY_ID, REGIS_CNSM_ID, REGIS_DATE"))
  .map(rec => (rec(7).toLowerCase(),1))
  .reduceByKey(_ + _)
  .take(2)



// COMMAND ----------

val rdd5 = rdd4.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
def f(T: String): Array[String] = {
  return T.split("~")
}
val splittedRdd: RDD[Array[String]] = rdd5.map(f)// f: (T) ⇒ U

def getAmount(T: Array[String]): Double = {
  return T(1).toDouble
}
val amountRdd: RDD[Double] = splittedRdd.map(getAmount)

def sum(n1: Double, n2: Double): Double = {
  return n1 + n2
}
val totalAmount: Double = amountRdd.reduce(sum)

totalAmount

// COMMAND ----------

//biggerRdd.coalesce(1).saveAsTextFile("dbfs:/FileStore/tables/biggerRdd/coalesce1")
sc.textFile("dbfs:/FileStore/tables/biggerRdd/coalesce1")
//val Partdd: RDD[String] = sc.textFile("dbfs:/FileStore/tables/biggerRdd/coalesce")
//val numOneToTwoRddParts: RDD[Array[Int]] = numOneToFiveRdd.glom()
//numOneToTwoRddParts.take(2)

// Homework: Is the number of records same in each of the partitions?
//  Answers
// They are not
biggerRdd.repartition(8).saveAsTextFile("dbfs:/FileStore/tables/biggerRdd/eight")
val eightRddParts = sc.textFile("dbfs:/FileStore/tables/biggerRdd/eight")
eightRddParts.getNumPartitions
biggerRdd.coalesce(6).saveAsTextFile("dbfs:/FileStore/tables/biggerRdd/six")
val sixRddParts = sc.textFile("dbfs:/FileStore/tables/biggerRdd/six")
sixRddParts.getNumPartitions


// change the num of partitions of biggerRdd from 8 to 6 using both repartition() as well as coalesce() function and count the num of records in each partition for both the cases




// COMMAND ----------

sixRddParts.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.3 - Saving data as ObjectFile, SequenceFile and NewAPIHadoopFile **

// COMMAND ----------

val regRdd = sc.textFile("/FileStore/tables/KC_Extract_1_20171009.csv")
  .map(record => record.split("\\|", -1))

regRdd.take(3)

// regRdd.saveAsTextFile("/FileStore/tables/Reg_v1/")
// sc.textFile("dbfs:/FileStore/tables/Reg_v1/").take(3)


// COMMAND ----------

regRdd.map(record => record.mkString(",")).saveAsTextFile("/FileStore/tables/Reg_v2/")
sc.textFile("dbfs:/FileStore/tables/Reg_v2/part-00000").take(3)


// COMMAND ----------

regRdd.saveAsObjectFile("/FileStore/tables/Reg_v3/")
sc.objectFile[Array[String]]("dbfs:/FileStore/tables/Reg_v3/part-00000").take(1)


// COMMAND ----------

import org.apache.hadoop.io.Text
regRdd.map(rec => (rec(4), rec(10))).saveAsSequenceFile("dbfs:/FileStore/tables/Reg_v4")
sc.sequenceFile("dbfs:/FileStore/tables/Reg_v4", classOf[Text], classOf[LongWritable]).map(rec => rec.toString()).collect()


// COMMAND ----------

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

regRdd.map(record => (new Text(record(2)), new Text(record(10))))
  .saveAsNewAPIHadoopFile("dbfs:/FileStore/tables/Reg_v5/",
                         classOf[Text],
                         classOf[Text],
                         classOf[SequenceFileOutputFormat[Text, Text]])

sc.newAPIHadoopFile("/FileStore/tables/Reg_v5/part-r-00000",
                   classOf[SequenceFileInputFormat[Text, Text]],
                   classOf[Text],
                   classOf[Text])
  .map(rec => rec.toString()).take(3)


// COMMAND ----------

val largeLogs = List(
  "2015-09-01 10:00:01|Error|Ac #3211001 ATW 10000 INR", 
  "2015-09-02 10:00:07|Info|Ac #3281001 ATW 11000 INR",
  "2015-10-01 10:00:09|error|Ac #3311001 AWT 10500 INR", 
  "2015-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-09-01 10:00:01|info|Ac #3211001 AWT 5000 INR", 
  "2016-09-02 10:00:01|ERROR|Ac #3211001 AWT 10000 INR",
  "2016-10-01 10:00:01|error|Ac #3211001 AWT 8000 INR", 
  "2016-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-12-01 10:00:01|Error|Ac #8211001 AWT 80000 INR", 
  "2016-12-02 10:00:01|error|Ac #9211001 AWT 90000 INR",
  "2016-12-10 10:00:01|error|Ac #3811001 AWT 15000 INR", 
  "2016-12-01 10:00:01|info|Ac #3219001 AWT 16000 INR"
)

val logsRdd = sc.parallelize(largeLogs)

logsRdd.filter(log => log.contains("2016-12")).count()


// COMMAND ----------

logsRdd.map(log => log.toLowerCase()).filter(log => log.contains("2016-12") && log.contains("error")).count()


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.4: Benefits of Laziness for Large - Scale Data **

// COMMAND ----------

val firstLogsWithErrors = logsRdd.filter(log => log.contains("2016") && log.contains("error")).take(5)
println(firstLogsWithErrors.length)

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.5: Set Operations **

// COMMAND ----------

val etLog = List(
  "2017-10-13 10:00:00|RIL|830.00",
  "2017-10-13 10:00:10|MARUTI SUZUKI|8910.00",
  "2017-10-13 10:00:20|RIL|835.00",
  "2017-10-13 10:00:30|MARUTI SUZUKI|8890.00"
)
val mcLog = List(
  "2017-10-13 10:00:20|RIL|835.00",
  "2017-10-13 10:00:40|MARUTI SUZUKI|8870.00"
)
val etLogRDD = sc.parallelize(etLog)
val mcLogRDD = sc.parallelize(mcLog)

// Union - Share price example
println("Total number of stocks: " + etLogRDD.union(mcLogRDD).distinct().count())
etLogRDD.union(mcLogRDD).distinct().collect()


// COMMAND ----------

// Intersection
etLogRDD.intersection(mcLogRDD).collect()


// COMMAND ----------

// Set difference
etLogRDD.subtract(mcLogRDD).collect()


// COMMAND ----------

// Cartesian product
etLogRDD.cartesian(mcLogRDD).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.6: Other Useful RDD Actions **

// COMMAND ----------

val numRDD = sc.parallelize(List.range(0, 10))
numRDD.takeSample(false, 5)
//numRDD.takeSample(true, 5)
//numRDD.takeSample(false, 5, 100)

// COMMAND ----------

sc.parallelize(List(10, 1, 2, 9, 3, 4, 5, 6, 7)).takeOrdered(6)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.7: Caching and Persistence **

// COMMAND ----------

import org.apache.spark.storage.StorageLevel
val lastYearsLogs = List(
  "2015-09-01 10:00:01|Error|Ac #3211001 ATW 10000 INR", 
  "2015-09-02 10:00:07|Info|Ac #3281001 ATW 11000 INR",
  "2015-10-01 10:00:09|error|Ac #3311001 AWT 10500 INR", 
  "2015-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-09-01 10:00:01|info|Ac #3211001 AWT 5000 INR", 
  "2016-09-02 10:00:01|ERROR|Ac #3211001 AWT 10000 INR",
  "2016-10-01 10:00:01|error|Ac #3211001 AWT 8000 INR", 
  "2016-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-12-01 10:00:01|Error|Ac #8211001 AWT 80000 INR", 
  "2016-12-02 10:00:01|error|Ac #9211001 AWT 90000 INR",
  "2016-12-10 10:00:01|error|Ac #3811001 AWT 15000 INR", 
  "2016-12-01 10:00:01|info|Ac #3219001 AWT 16000 INR"
)
val lastYearsLogsRDD = sc.parallelize(lastYearsLogs)
val logsWithErrors = lastYearsLogsRDD
  .map(log => log.toLowerCase())
  .filter(log => log.contains("error"))
  .persist(StorageLevel.MEMORY_ONLY_SER)

logsWithErrors.take(2)


// COMMAND ----------

logsWithErrors.count()   // faster


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.8: Understanding cluster topology Example-1 **

// COMMAND ----------

val people = List(
  "Sidharth,31",
  "Atul,32",
  "Sachin,29"
)
val peopleRdd = sc.parallelize(people)

def printPerson(line: Array[String]): Unit = {
  println("Person(" + line(0) + "," + line(1) + ")")
}

peopleRdd
  .map(line => line.split(","))
  .foreach(printPerson)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.9: Understanding cluster topology Example-2 **

// COMMAND ----------

val people = List(
  "Sidharth,31",
  "Atul,32",
  "Sachin,29"
)
val peopleRdd = sc.parallelize(people)

peopleRdd
  .map(line => line.split(","))
  .take(2)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.10: mapPartitions() example **

// COMMAND ----------

def splitStr(recIter: Iterator[String]): Iterator[Array[String]] = {
  var resList: List[Array[String]] = List()
  for(rec <- recIter) {
    resList = resList :+ rec.split('|')
  }
  return resList.iterator
}

def arrToDouble(recIter: Iterator[Array[String]]): Iterator[Double] = {
  // Create a JDBC connection object for the DB
  var resList: List[Double] = List()
  for(rec <- recIter) {
    resList = resList :+ rec(2).toDouble
    // Lookup the pin code for the cust_id-- select pincode from customer where cust_id = rec._2
  }
  // Close the connection
  return resList.iterator
}

sc.textFile("/FileStore/tables/txn_fct.csv", 2)
  .filter(rec => !rec.contains("amount")) // RDD[String]
  .mapPartitions(splitStr) // RDD[Array[String]]
  .mapPartitions(arrToDouble)
  .reduce((n1, n2) => n1 + n2)
  //.take(5)

// COMMAND ----------

// Create a JDBC connection object for the DB
// Lookup the pin code for the cust_id-- select pincode from customer where cust_id = rec._2
// Close the connection

// COMMAND ----------

sc.textFile("/FileStore/tables/txn_fct.csv")
  .filter(rec => !rec.contains("amount")) // RDD[String]
  .mapPartitions(recIter => recIter.map(rec => rec.split('|')))  // RDD[Array[String]]
  .mapPartitions{recIter => recIter.map(rec => rec(2).toDouble)}  // RDD[Double]
  .reduce((n1, n2) => n1 + n2) // Double

// COMMAND ----------

sc.textFile("/FileStore/tables/txn_fct.csv", 2)
  //.filter(rec => !rec.contains("amount")) // RDD[String]
  .mapPartitionsWithIndex{(idx, recIter) =>
    if(idx == 0)
      recIter.drop(1)
    else
      recIter
  }
  .mapPartitions(recIter => recIter.map(rec => rec.split('|')))  // RDD[Array[String]]
  .mapPartitions{recIter => recIter.map(rec => rec(2).toDouble)}  // RDD[Double]
  .reduce((n1, n2) => n1 + n2) // Double
