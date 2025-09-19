# Databricks notebook source
from pyspark.sql.functions import *

# Input data as list of dictionaries
data = [
    {"name": "Afghanistan", "continent": "Asia",   "area": 652230,  "population": 25500100, "gdp": 20343000000},
    {"name": "Albania",     "continent": "Europe", "area": 28748,   "population": 2831741,  "gdp": 12960000000},
    {"name": "Algeria",     "continent": "Africa", "area": 2381741, "population": 37100000, "gdp": 188681000000},
    {"name": "Andorra",     "continent": "Europe", "area": 468,     "population": 78115,    "gdp": 3712000000},
    {"name": "Angola",      "continent": "Africa", "area": 1246700, "population": 20609294, "gdp": 100990000000}
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show DataFrame
df = df.select('name','continent','area','population','gdp')
df.show()

# Register as a temporary view (table)
df.createOrReplaceTempView("World")

# Optional: Query with SQL
spark.sql("SELECT * FROM World").show()


# COMMAND ----------

# MAGIC %md
# MAGIC A country is big if:
# MAGIC
# MAGIC 1. it has an area of at least three million (i.e., 3000000 km2), or
# MAGIC 2. it has a population of at least twenty-five million (i.e., 25000000).
# MAGIC Write a solution to find the name, population, and area of the big countries.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

df_final = df.filter((col('area') >= 3000000) | (col('population') >= 25000000)).select('name', 'population', 'area')
df_final.display()

# COMMAND ----------

spark.sql(
    """
    select name,population,area
    from world
    where area >= 3000000
    or population >= 25000000
    """
).display()
