// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <h1 style="font-size: 96px">
// MAGIC   <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/Apache-Spark-Logo_TM_400px.png"/>
// MAGIC   Workshop
// MAGIC </h1>
// MAGIC 
// MAGIC <p/><p/>
// MAGIC 
// MAGIC <div style="float: right">
// MAGIC   <img src="https://www.ardentex.com/images/bmc.jpg" style="width: 200px"/>
// MAGIC   <br clear="all"/>
// MAGIC   <div style="text-align: right">
// MAGIC     Brian Clapper, Databricks<br/>
// MAGIC     _bmc@databricks.com_<br/>
// MAGIC     @brianclapper
// MAGIC   </div>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Agenda
// MAGIC 
// MAGIC * [ETL (of UK crime data)]($./01 ETL)
// MAGIC * Some data analysis
// MAGIC * Structured streaming of something else (if you're interested)
// MAGIC * A bit of prediction
// MAGIC * Spark architecture overview (?)
// MAGIC 
// MAGIC Throughout this workshop, I'll be using Databricks, a Spark as a Service product that also happens to help pay my bills.
// MAGIC 
// MAGIC You can import the DBC directly into Databricks' free Spark as a Service
// MAGIC product,
// MAGIC [Databricks Community Edition](https://databricks.com/ce). The URL for
// MAGIC the package of notebooks (a "DBC" archive file) is:
// MAGIC <https://github.com/bmc/scala-world-2017-spark-workshop/blob/master/notebooks.dbc?raw=true>.
// MAGIC 
// MAGIC The data files used by the notebooks are at the following locations.
// MAGIC You can download these raw files, copy them into your own S3 bucket, and
// MAGIC [mount the S3 bucket to DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#mounting-an-s3-bucket).
// MAGIC 
// MAGIC 
// MAGIC * <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/london-lsoa-codes.csv>
// MAGIC * <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2015.csv>
// MAGIC * <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2016.csv>
// MAGIC * <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2017.csv>
// MAGIC 
// MAGIC 
// MAGIC You'll need to change the paths in the notebooks to correspond to your mount point, but everything else should work as is.

// COMMAND ----------


