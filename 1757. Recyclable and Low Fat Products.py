# Databricks notebook source
../Includes/utilis

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Input data
data = [
    {"product_id": 0, "low_fats": "Y", "recyclable": "N"},
    {"product_id": 1, "low_fats": "Y", "recyclable": "Y"},
    {"product_id": 2, "low_fats": "N", "recyclable": "Y"},
    {"product_id": 3, "low_fats": "Y", "recyclable": "Y"},
    {"product_id": 4, "low_fats": "N", "recyclable": "N"}
]

# Create DataFrame
df = spark.createDataFrame(data)

# Reorder columns with product_id first
df = df.select("product_id", "low_fats", "recyclable")

# Show DataFrame
df.show()

# Register as a temp view
df.createOrReplaceTempView("Products")
spark.sql("select * from Products").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write a solution to find the ids of products that are both low fat and recyclable.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

df_final = df.filter((col('low_fats') == 'Y') & (col('recyclable') == 'Y')).select('product_id')
df_final.display()

# COMMAND ----------

spark.sql(
    """
    select product_id
    from products
    where low_fats = 'Y'
    and recyclable = 'Y'
    """
).display()
