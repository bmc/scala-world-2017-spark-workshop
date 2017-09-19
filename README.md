[Databricks](https://databricks.com) notebooks used during the
Scala Workshop at [Scala World 2017](http://scala.world).

You can import the DBC directly into Databricks' free Spark as a Service
product, 
[Databricks Community Edition](https://databricks.com/ce). The URL for
the package of notebooks (a "DBC" archive file) is:
<https://github.com/bmc/scala-world-2017-spark-workshop/blob/master/notebooks.dbc?raw=true>.

The data files used by the notebooks are at the following locations.
You can download these raw files, copy them into your own S3 bucket, and
[mount the S3 bucket to DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#mounting-an-s3-bucket).

* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/london-lsoa-codes.csv>
* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2015.csv>
* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2016.csv>
* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2017.csv>

You'll need to change the paths in the notebooks to correspond to your mount point, but everything else should work as is.
