// Databricks notebook source
// This is used to implicitly convert an RDD to a DataFrame.
import spark.implicits._
// Create a simple DataFrame, store into a partition directory
val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
squaresDF.write.parquet("/FileStore/tables/test_table/key=1")
squaresDF.printSchema()

// COMMAND ----------

// Create another DataFrame in a new partition directory,
// adding a new column and dropping an existing column
val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
cubesDF.write.parquet("/FileStore/tables/test_table/key=2")
// Read the partitioned table
val mergedDF = spark.read.option("mergeSchema", "true").parquet("/FileStore/tables/test_table")
mergedDF.printSchema()

// COMMAND ----------

// You may find the free examples in the below GITHUB
// https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples
