// Databricks notebook source
// MAGIC %md 
// MAGIC #![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Future Crime
// MAGIC 
// MAGIC ## Using linear regression to (try to) predict future crimes
// MAGIC 
// MAGIC The objective here is to predict the monthly occurrences of a particular type of crime in the London area in 2017, based on the statistics from 2015 and 2016.
// MAGIC 
// MAGIC Let's start by reading in the data, paring it down to the London area, and removing all but one crime type.
// MAGIC 
// MAGIC https://www.police.uk/crime-prevention-advice/anti-social-behaviour/

// COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.parquet").select("crimeType").groupBy("crimeType").count.orderBy($"count".desc))

// COMMAND ----------

//val CrimeType = "Anti-social behaviour"
val CrimeType = "Violence and sexual offences"

// COMMAND ----------

val londonLsoaDF = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/london-lsoa-codes.parquet")
val rawTrainDF = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.parquet", "dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2016.parquet")
  .join(londonLsoaDF, $"code" === $"lsoaCode")
  .filter($"crimeType" === CrimeType)
  .select("month")

val rawTestDF = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2017.parquet")
  .join(londonLsoaDF, $"code" === $"lsoaCode")
  .filter($"crimeType" === CrimeType)
  .select("month")


// COMMAND ----------

// MAGIC %md When we build a machine learning model, we generally split our data into a training set and a test set. The test set mimics unseen data, and gives us an idea of how well our model will generalize to new data.
// MAGIC 
// MAGIC ![trainTest](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/TrainTestSplit.png)
// MAGIC 
// MAGIC In our case, though, we already have some good test data: The existing 2017 data. However, 2017 is only partial data, and we're using the month number as a bucket. The bucket sizes have to match. 2015 and 2016 will have 12 buckets. What about the 2017 data?

// COMMAND ----------

display(rawTestDF.select("month").distinct.orderBy("month"))

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, so we need to pad out the test data to have the same number of months as the training data. (Otherwise, we'll get an error.)
// MAGIC 
// MAGIC We're interested in the number of thefts per month, so we can roll that up, as well.

// COMMAND ----------

case class MonthCount(month: Int, count: Int)
val padData = (10 to 12).map { mon => MonthCount(mon, 0) }
val paddedDF = spark.createDataFrame(padData)
display(paddedDF)

// COMMAND ----------

import org.apache.spark.sql.functions._

val trainDF = rawTrainDF
  .select(month($"month").alias("month"), year($"month").as("year"))
  .na.drop
  .groupBy("year", "month")
  .count

val testDF = (
  rawTestDF
    .select(month($"month").alias("month"), year($"month").as("year"))
    .na.drop
    .groupBy("month")
    .count
  unionAll
  paddedDF
)

display(trainDF.orderBy("year", "month"))

// COMMAND ----------

// MAGIC %md
// MAGIC One-hot encoding allows algorithms which expect continuous features, such as Linear Regression, to use categorical features.

// COMMAND ----------

import org.apache.spark.sql.functions.{floor, translate, round}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression

val encoder =  new OneHotEncoder()
  .setInputCol("month")
  .setOutputCol("features")

val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("count")
  .setPredictionCol("predictedCount")
  .setRegParam(0.0)

println(lr.explainParams)
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC There are a few different hyperparameters we could change. For example, we could set the regularization parameter to a non-negative value to guard against overfitting to our training data (though if it is too large, we could underfit).

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(encoder, lr))

val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

val predictions = pipelineModel.transform(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's evaluate how well our model performed. Normally, we would use a technique like [RMSE](https://en.wikipedia.org/wiki/Root-mean-square_deviation) to evaluate the model, but, with this data set, we can just eyeball a graph.

// COMMAND ----------

display(predictions)

// COMMAND ----------

val combined = (
  predictions.select(lit("2017").as("year"), $"month", $"count")
  union
  predictions.select(lit("2017-predicted").as("year"), $"month", $"predictedCount".cast("integer").as("count"))
  union
  rawTrainDF.select(year($"month").cast("string").as("year"), month($"month").as("month")).groupBy("year", "month").count
)
display(combined.orderBy("year", "month"))

// COMMAND ----------

// MAGIC %md
// MAGIC Relative to the inputs, the predictions are pretty close. They're not as close to the actual crime statistics for 2017, however.
// MAGIC 
// MAGIC Why?
