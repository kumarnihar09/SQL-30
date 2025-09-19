# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *


# Define schema for the Tweets table
schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Input data as list of tuples
data = [
    (1, "Let us Code"),
    (2, "More than fifteen chars are here!"),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show(truncate=False)

df.createOrReplaceTempView('Tweets')


# COMMAND ----------

df_final = df.filter(length(col('content'))>15).select('tweet_id')
df_final.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select tweet_id
# MAGIC from tweets 
# MAGIC where len(content) > 15
