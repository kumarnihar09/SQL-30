# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (1, 'john@example.com'),
    (2, 'bob@example.com'),
    (3, 'john@example.com')
]

# Create DataFrame
df = spark.createDataFrame(data, ['id', 'email'])

df.show()


# COMMAND ----------

window = Window.partitionBy('email').orderBy('id')
ranked_df = df.withColumn('rnk',dense_rank().over(window))
result_df = ranked_df.filter('rnk = 1').select('id','email')
result_df.display()


# COMMAND ----------

/* Write your T-SQL query statement below */
with cte as (

select *, DENSE_RANK() over(partition by email order by id) as rnk
from person
)
delete 
from cte where rnk >1
