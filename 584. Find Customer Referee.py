# Databricks notebook source
from pyspark.sql.functions import *


# Input data as list of dictionaries, use None for null
data = [
    {"id": 1, "name": "Will", "referee_id": None},
    {"id": 2, "name": "Jane", "referee_id": None},
    {"id": 3, "name": "Alex", "referee_id": 2},
    {"id": 4, "name": "Bill", "referee_id": None},
    {"id": 5, "name": "Zack", "referee_id": 1},
    {"id": 6, "name": "Mark", "referee_id": 2}
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show DataFrame
df.show()

# Register as a temporary view (table)
df.createOrReplaceTempView("Customer")

# Optional: Query with SQL
spark.sql("SELECT * FROM Customer").show()


# COMMAND ----------

# MAGIC %md
# MAGIC Find the names of the customer that are either:
# MAGIC
# MAGIC 1. referred by any customer with id != 2.
# MAGIC 2. not referred by any customer.

# COMMAND ----------

df_final = df.filter((col('referee_id') != 2) | (col('referee_id').isNull())).select('name')
df_final.display()

# COMMAND ----------

spark.sql(
    """
    select name
    from customer
    where referee_id is null or
    referee_id != 2
    """
).display()
