// Databricks notebook source
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

// COMMAND ----------

val df = Seq(("Yoda",             "Obi-Wan Kenobi"),
             ("Anakin Skywalker", "Sheev Palpatine"),
             ("Luke Skywalker",   "Han Solo, Leia Skywalker"),
             ("Leia Skywalker",   "Obi-Wan Kenobi"),
             ("Sheev Palpatine",  "Anakin Skywalker"),
             ("Han Solo",         "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
             ("Obi-Wan Kenobi",   "Yoda, Qui-Gon Jinn"),
             ("R2-D2",            "C-3PO"),
             ("C-3PO",            "R2-D2"),
             ("Darth Maul",       "Sheev Palpatine"),
             ("Chewbacca",        "Han Solo"),
             ("Lando Calrissian", "Han Solo"),
             ("Jabba",            "Boba Fett")
            )
.toDF("name", "friends")


// COMMAND ----------

case class starwars(name:String,friends:String)
val ds = df.as[starwars]

// COMMAND ----------

display(ds)

// COMMAND ----------

ds.filter(x => x.friends == "Obi-Wan Kenobi").show()
          

// COMMAND ----------

ds.select(col("name")).show()

// COMMAND ----------

ds.select($"name").show()

// COMMAND ----------

ds.select("name").show()

// COMMAND ----------

ds.filter(x => x.name == "Yoda" & x.friends == "Anakin Skywalker").show()

// COMMAND ----------


