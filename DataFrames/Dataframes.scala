// Databricks notebook source
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

val simpleSchema = StructType(Array(
    StructField("Row",StringType,true)
    ))

// COMMAND ----------

val df = spark.read.format("csv").load("/FileStore/tables/NASA_access_log_Jul95")

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

case class DeviceIoTData (
  A:String,
  B:String,
  C:String,
  D:String,
  E:String
)

// COMMAND ----------

val splitDF = df.withColumn("split_raw_arr", split($"_c0", " "))
  .withColumn("A", $"split_raw_arr"(0))
  .withColumn("B", $"split_raw_arr"(1))
  .withColumn("C", $"split_raw_arr"(2))
  .withColumn("D", $"split_raw_arr"(3))
  .withColumn("E", $"split_raw_arr"(4))
  .drop("_c0", "split_raw_arr")

// COMMAND ----------

splitDF.show(false)

// COMMAND ----------

val tempDS = splitDF.as[DeviceIoTData]

// COMMAND ----------

tempDS.take(4)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

case class Salary(depName: String, empNo: Long, salary: Long)
val empsalary = Seq(
  Salary("sales", 1, 5000),
  Salary("personnel", 2, 3900),
  Salary("sales", 3, 4800),
  Salary("sales", 4, 4800),
  Salary("personnel", 5, 3500),
  Salary("develop", 7, 4200),
  Salary("develop", 8, 6000),
  Salary("develop", 9, 4500),
  Salary("develop", 10, 5200),
  Salary("develop", 11, 5200)).toDS


// COMMAND ----------

empsalary.show(3)

// COMMAND ----------

// Aggregate functions

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
val byDepName = Window.partitionBy("depName").orderBy("empNo")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

//empsalary.withColumn("avg", avg('salary) over byDepName).sho

empsalary.withColumn("salary_avg", avg("salary") over byDepName).show()

// COMMAND ----------

val dataset = Seq(
  ("Thin",       "cell phone", 6000),
  ("Normal",     "tablet",     1500),
  ("Mini",       "tablet",     5500),
  ("Ultra thin", "cell phone", 5000),
  ("Very thin",  "cell phone", 6000),
  ("Big",        "tablet",     2500),
  ("Bendable",   "cell phone", 3000),
  ("Foldable",   "cell phone", 3000),
  ("Pro",        "tablet",     4500),
  ("Pro2",       "tablet",     6500))
  .toDF("product", "category", "revenue")


// COMMAND ----------

val partition1 = Window.partitionBy("category").orderBy("revenue")
val partition2 = dataset.withColumn("lag", lag("revenue",2) over partition1)


// COMMAND ----------

partition2.show()

// COMMAND ----------

dataset.withColumn("lag", lead("revenue",1) over partition1).show()

// COMMAND ----------

case class Person(name: String, age: Int)
//import org.apache.spark.rdd.RDD
val peopleRDD: RDD[Person] = sc.parallelize(Seq(Person("Jacek", 10)))

// COMMAND ----------

//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
//import sqlContext.implicits._
//partition1.createOrReplaceTempView("nums")
//%sql
//select * from nums;


// COMMAND ----------

partition2.createOrReplaceTempView("nums")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from nums;

// COMMAND ----------

// UDF's
val squared = (s:Long) => {
  s * s
}
spark.udf.register("square", squared)


// COMMAND ----------

// MAGIC %sql
// MAGIC select *, square(revenue) as revenuesquare from nums;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * , 
// MAGIC case 
// MAGIC   when lag is NULL Then 0
// MAGIC   else square(lag) 
// MAGIC   END as lagsquare 
// MAGIC from nums;

// COMMAND ----------

sample = window.PartitionBy("colname").orderBy(colname)
df.withColumn("newcolname", avg("colname") over(sample) )
