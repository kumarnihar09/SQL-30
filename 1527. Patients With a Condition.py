# Databricks notebook source
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (1, "aLice"),
    (2, "bOB")
]

# Define schema
columns = ["user_id", "name"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()


# COMMAND ----------

result_df = df.filter(col('conditions').like('DIAB1%') | col('conditions').like('% DIAB1%')).select('patient_id','patient_name','conditions')
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select patient_id, patient_name, conditions
# MAGIC from patients
# MAGIC where conditions like 'DIAB1%'
# MAGIC OR conditions like '% DIAB1%'

# COMMAND ----------

/* Write your T-SQL query statement below */
with cte as (

select *, DENSE_RANK() over(partition by email order by id) as rnk
from person
)
delete 
from cte where rnk >1
