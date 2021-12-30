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


