// Databricks notebook source
// MAGIC %md 
// MAGIC #![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Analysis and Visualization

// COMMAND ----------

// MAGIC %fs ls /mnt/bmc/scala-world-2017

// COMMAND ----------

val df2016 = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2016.parquet")

// COMMAND ----------

display(
  df2016.select($"month").distinct.orderBy($"month".desc)
)

// COMMAND ----------

display(df2016)

// COMMAND ----------

// MAGIC %md
// MAGIC If you prefer to work with a Scala type, rather than column names, you can cast the result to a `case class`.
// MAGIC 
// MAGIC There are some potential performance implications, which we can discuss.
// MAGIC 
// MAGIC * Casting to a case class allows you to use traditional lambdas with your data.
// MAGIC * But, because Spark stores the data internal in an efficient column-oriented form (Tungsten), it has to deserialize
// MAGIC   the data for each row into an object to pass to your lambda. (It does so with custom-built encoders and decoders.)
// MAGIC * If you never use a lambda, you never pay that penalty.

// COMMAND ----------

case class CrimeDataElement(month:               String,
                            reportedBy:          String,
                            fallsWithin:         String,
                            longitude:           Double,
                            latitude:            Double,
                            location:            String,
                            lsoaCode:            String,
                            lsoaName:            String,
                            crimeType:           String,
                            lastOutcomeCategory: Option[String],
                            context:             Option[String])
val ds2016 = df2016.as[CrimeDataElement]

// COMMAND ----------

ds2016

// COMMAND ----------

// MAGIC %md
// MAGIC What kinds of crimes are recorded? Note that you can _still_ use this Dataset with the DataFrame API.

// COMMAND ----------

display(ds2016.select($"crimeType").groupBy("crimeType").count.orderBy($"crimeType"))

// COMMAND ----------

import org.apache.spark.sql.functions._
val londonLSOA = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/london-lsoa-codes.parquet")
val bikeThefts2016 = df2016.join(londonLSOA, $"lsoaCode" === $"code").where($"crimeType" like "Bicycle%")

display(bikeThefts2016)

// COMMAND ----------

// MAGIC %md
// MAGIC Roll 'em up by month.

// COMMAND ----------

import org.apache.spark.sql.functions._
display(
  bikeThefts2016
    .select(month($"month").as("monthNum"))
    .groupBy("monthNum")
    .count
    .orderBy("monthNum")
)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize this data geographically. Google Maps supports up to 400 geodata points. How many bike thefts do we have in this reduced result set?

// COMMAND ----------

val total = bikeThefts2016.count

// COMMAND ----------

// MAGIC %md
// MAGIC Too many. Let's take a sampling of the data from the month with the most thefts.

// COMMAND ----------

case class LargestMonth(monthNum: Int, count: BigInt)
val biggest = bikeThefts2016
    .filter(! isnull($"latitude"))
    .filter(! isnull($"longitude"))
    .select(month($"month").as("monthNum"))
    .groupBy("monthNum")
    .count
    .orderBy($"count".desc)
    .as[LargestMonth]
    .collect()
    .take(1)
    .head

// COMMAND ----------

val fraction = 400.0 / biggest.count.toDouble
val theftSample = bikeThefts2016
  .filter(month($"month") === biggest.monthNum)
  .sample(withReplacement = false, fraction = fraction, seed = 345678654334567l)
  .limit(400)
println(theftSample.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Now we can pull the data back and graph it.

// COMMAND ----------

case class Location(latitude: Double, longitude: Double, lsoaCode: String)
val theftLocations = theftSample.select($"latitude", $"longitude", $"lsoaCode").as[Location].collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Plot the 400 we got.

// COMMAND ----------

val html = s"""
<html>
  <head>
  <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
  <script>
    google.charts.load('current', { 'packages': ['map'] });
    google.charts.setOnLoadCallback(drawMap);

    function drawMap() {
      var data = new google.visualization.DataTable();
      data.addColumn('number', 'Latitude');
      data.addColumn('number', 'Longitude');
      data.addColumn('string', 'Marker');
      data.addRows([
${theftLocations.map { l => s"[${l.latitude}, ${l.longitude}, 'alt']" }.mkString(",\n") }
      ]);

    var options = {
      showTooltip: true,
      showInfoWindow: true,
      zoomLevel: 12,
      height: 750,
      icons: {
        alt: {
          normal: 'http://icons.iconarchive.com/icons/icons8/windows-8/48/Maps-Marker-icon.png',
          selected: 'http://icons.iconarchive.com/icons/icons8/windows-8/48/Maps-Marker-icon.png'
        }
      }
    };

    var map = new google.visualization.Map(document.getElementById('chart_div'));

    map.draw(data, options);
  };
  </script>
  </head>
  <body>
    <div id="chart_div"></div>
  </body>
</html>
"""
displayHTML(html)
//println(html)

// COMMAND ----------

// MAGIC %md
// MAGIC How does 2016 compare to 2015?

// COMMAND ----------

val df2015 = spark.read.parquet("dbfs:/mnt/bmc/scala-world-2017/uk-crime-data-2015.parquet")
val bikeThefts2015 = df2015.join(londonLSOA, $"lsoaCode" === $"code").filter($"crimeType" like "Bicycle%")
val combined = (
  bikeThefts2016
    .select(year($"month").as("year"), month($"month").as("monthNum"))
    .groupBy("year", "monthNum")
    .count
  union
  bikeThefts2015
    .select(year($"month").as("year"), month($"month").as("monthNum"))
    .groupBy("year", "monthNum")
    .count
)

display(combined.orderBy("year", "monthNum"))

// COMMAND ----------

// MAGIC %md
// MAGIC Obviously, we could do the same with any other crime.

// COMMAND ----------

// MAGIC %md
// MAGIC Next up: Some [predictions]($./03-Predict)
