// Databricks notebook source
// MAGIC %md
// MAGIC #![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) ETL
// MAGIC                     
// MAGIC We'll be using [UK crime data](https://data.police.uk) from 2015, 2016 and (later) 2017. The data was downloaded for all forces, without outcomes or stop-and-search data.

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/bmc/scala-world-2017

// COMMAND ----------

// MAGIC %md
// MAGIC Here's an example of the data.

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.csv

// COMMAND ----------

// MAGIC %md
// MAGIC **Goal:** Convert this data into Parquet, which is more efficient.
// MAGIC 
// MAGIC **Problem:** Some of the column names contain blanks, which aren't allowed in Parquet column names. A quick tail-recursive function fixes that.

// COMMAND ----------

import scala.annotation.tailrec
import org.apache.spark.sql.DataFrame

/** Convert all column names to camelCase. The raw data has blanks
  * in the column names, and attempts to write Parquet with blanks
  * in the column names won't work.
  */
def fixColumns(df: DataFrame): DataFrame = {
  @tailrec def fixNext(colsLeft: List[String], curDF: DataFrame): DataFrame = {
    colsLeft match {
      case Nil => curDF
      case col :: rest => 
        val Array(firstToken, remainingTokens @ _*) = col.split(" ")
        val newName = firstToken.toLowerCase + remainingTokens.map(_.capitalize).mkString("")
        fixNext(rest, curDF.withColumnRenamed(col, newName))
    }
  }
  
  fixNext(df.columns.toList, df)
}

// COMMAND ----------

// MAGIC %md
// MAGIC We're going to convert all three files, so we might as well use a function.
// MAGIC 
// MAGIC Let's speed things up a little, though. The 2017 file is smaller, so we'll let Spark use that file to infer the schema. We'll then use that schema
// MAGIC in all three conversions.

// COMMAND ----------

val schema = spark
  .read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/mnt/bmc/scala-world-2017/uk-crime-data-2017.csv")
  .schema

// COMMAND ----------

import org.apache.spark.sql.SaveMode
def convert(from: String, to: String) = {
  val df = spark
    .read
    .option("header", "true")
    .schema(schema)
    .csv(from)

  fixColumns(df)
    .drop("crimeID")
    .write
    .mode(SaveMode.Overwrite)
    .parquet(to)
  df
}


// COMMAND ----------

convert("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.csv", "dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.parquet")

// COMMAND ----------

convert("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2016.csv", "dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2016.parquet")

// COMMAND ----------

convert("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2017.csv", "dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2017.parquet")

// COMMAND ----------

val df = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.parquet")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC The LSOA (Lower Layer Super Output Area) codes identify geographical regions within the UK.
// MAGIC (See <https://en.wikipedia.org/wiki/ONS_coding_system> for a description of this system. The data
// MAGIC comes from the LSOA Atlas, located at <https://data.london.gov.uk/dataset/lsoa-atlas>). We'll restrict most of our analysis to the
// MAGIC London area, so we need a way to pare the codes down. There's another file that contains only the London-area codes.

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/bmc/scala-world-2017/london-lsoa-codes.csv

// COMMAND ----------

// MAGIC %md
// MAGIC Let's convert that to Parquet, too.

// COMMAND ----------

spark
  .read
  .option("header", "true")
  .csv("dbfs:/mnt/bmc/scala-world-2017/london-lsoa-codes.csv")
  .withColumnRenamed("Codes", "code")
  .withColumnRenamed("Names", "name")
  .write
  .mode(SaveMode.Overwrite)
  .parquet("dbfs:/mnt/bmc/scala-world-2017/london-lsoa-codes.parquet")

// COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/london-lsoa-codes.parquet"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Let's move on...
// MAGIC 
// MAGIC ...to some [analysis]($./02-Analyze).
