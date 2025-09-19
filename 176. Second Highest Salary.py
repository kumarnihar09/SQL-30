# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (1, 100),
    (2, 200),
    (3, 300)
]

# Create DataFrame
df = spark.createDataFrame(data, ['id', 'salary'])

# Show the DataFrame
df.show()


# COMMAND ----------

window = Window.orderBy(desc('salary'))
ranked_df = df.withColumn('rnk',dense_rank().over(window))
result_df = ranked_df.filter(col('rnk') == 2).select(col('salary').alias('SecondHighestSalary'))
result_df.display()


# COMMAND ----------

/* Write your T-SQL query statement below */
with cte as (

select *, DENSE_RANK() over(partition by email order by id) as rnk
from person
)
delete 
from cte where rnk >1
