# Spark Workshop @ Scala World 2017

This repository contains the
[Databricks](https://databricks.com) notebooks used during my
Spark Workshop at [Scala World 2017](http://scala.world).

## Importing the notebooks

You can import the DBC directly into Databricks' free Spark as a Service
product,
[Databricks Community Edition](https://databricks.com/ce). The URL for
the package of notebooks (a "DBC" archive file) is:
<https://github.com/bmc/scala-world-2017-spark-workshop/blob/master/notebooks.dbc?raw=true>.

Start by logging into your Community Edition account. Use Firefox or
Chrome, for best results. Then:

1. Click the **Home** icon in the left sidebar.  
![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/home.png)

2. Right click your home folder, then click **Import**.  
![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/import-labs-1.png)

3. In the popup, click **URL**.  
![](https://github.com/bmc/scala-world-2017-spark-workshop/raw/master/images/import.png)

4. Paste the DBC URL (from above) into the text box.

5. Click the **Import** button.  
![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/import-labs-3.png)

6. Wait for the import to finish. This can take a minute or so to complete.

## Running the notebooks

### Create a cluster

Start by creating a Spark cluster. (If you already have a running cluster, skip this bit. You can only have one cluster per Community Edition account.)

1. Select the **Clusters** icon in the sidebar.  
![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-4.png)

2. Click the **Create Cluster** button.  
![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-5.png)

3. Name your cluster. In Community Edition, since you can only have one running cluster, the name doesn't matter too much.

4. Select the cluster type. We recommend the latest runtime (**3.2**, **3.3**, etc.) and Scala **2.11**.

5. Click the **Create Cluster** button.  
![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-2.png)

### Open the first notebook

Select the **Home** button in the sidebar, and select the "Scala World 2017"
folder. Then, click on the notebook you want to run (e.g., "01 ETL").

You can run individual cells with Command-Click (Mac) or Alt-Click
(Linux and Windows), which runs the cells and moves the cursor to the next
cell. You can use Control-Click (all platforms) to run the cell and leave
the cursor in the cell.

The first time you try to run a non-Markdown cell, Databricks will prompt
you to attach to your cluster.

## Data Files

The data files used by the notebooks are at the following locations. You
can't run the notebooks without them.

You can download these raw files, copy them into your own S3 bucket, and
[mount the S3 bucket to DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#mounting-an-s3-bucket).

You'll need to change the paths in the notebooks. They all start with
`/mnt/bmc`. You'll need to change that prefix to correspond to the mount
point you choose.

* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/london-lsoa-codes.csv>
* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2015.csv>
* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2016.csv>
* <https://s3-us-west-2.amazonaws.com/bmc-work/scala-world-2017/uk-crime-data-2017.csv>

## Getting Help

Drop me an email (bmc@clapper.org) or open an issue if you're having problems. I'm happy to help.
