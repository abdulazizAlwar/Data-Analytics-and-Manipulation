# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started With Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running Python Code

# COMMAND ----------

x = 1

# COMMAND ----------

print(x)

# COMMAND ----------

x

# COMMAND ----------

#This is a Python comment

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Writing Markdown

# COMMAND ----------

# MAGIC %md
# MAGIC To create a markdown cell add `%md` to the first line of the cell

# COMMAND ----------

# MAGIC %md
# MAGIC #### Heading 4
# MAGIC 
# MAGIC This is sample text

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls ../..

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Tables

# COMMAND ----------

import pandas as pd

path = "/dbfs/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"

df = pd.read_csv(path)

# COMMAND ----------

df.head()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write CSV file to Databricks Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Click on `File` then click on `Upload Data` to upload csv file. Then paste the path to the `file_location` variable below.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/sample.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS study_firstnamel_db

# COMMAND ----------

# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "study_firstnamel_db.sample_data"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read Databricks Table to DataFrame

# COMMAND ----------

db_and_table = 'study_firstnamel_db.sample_data'

df_spark = spark.read.table(db_and_table)

# COMMAND ----------

display(df_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### To Pandas DataFrame
# MAGIC 
# MAGIC If the data set is very big, you might not be able to convert to pandas. In that case use `.sample()` to take a sample of the data.

# COMMAND ----------

db_and_table = 'study_firstnamel_db.sample_data'

sample_size = 0.01

df_spark = spark.read.table(db_and_table).sample(sample_size)

# COMMAND ----------

df_pandas = df_spark.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### To Koalas Dataframe

# COMMAND ----------

df_koalas = df_spark.to_koalas()
