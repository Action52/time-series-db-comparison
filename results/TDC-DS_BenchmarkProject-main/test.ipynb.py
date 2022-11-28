# Databricks notebook source
print("Starting, hello world")

# COMMAND ----------

from pyspark.sql import types
stats_path = "s3a://tpcds-spark/results/1G/test_run_stats_csv".format(size="data_size")
schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])
df_s = spark.read.option("header", "true").csv(stats_path, schema)
df= df_s.toPandas()
df.head()

# COMMAND ----------


