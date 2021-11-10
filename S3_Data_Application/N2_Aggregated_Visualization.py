# Databricks notebook source
# MAGIC %md
# MAGIC #### Version 0.1
# MAGIC 
# MAGIC Version History:-

# COMMAND ----------

# MAGIC %md
# MAGIC Reference for wide vs long table format: <br>
# MAGIC https://kiwidamien.github.io/long-vs-wide-data.html#:~:text=Long%20form%20This%20is%20very,making%20tables%20for%20quick%20comparison.

# COMMAND ----------

#Column Metadata
metadata = spark.table('externaluc_flight_analysis_db.arrivals_metadata_v1')

display(metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO:-
# MAGIC 1. ~~Visualize Elements vs Columns~~
# MAGIC 1. **Visualize Columns against Select Elements**
# MAGIC 1. Timeseries plot of original dataset <br>
# MAGIC 2. Trend analysis of each dataset <br>
# MAGIC 3. Scatterplot, Countplot, Boxplot, Distplot <br>
# MAGIC 4. Relationship between certain columns <br>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Package Imports

# COMMAND ----------

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

pd.options.display.float_format = '{:,.2f}'.format

# Set ipython's max row display
pd.set_option('display.max_row', 1000)

# Set iPython's max column width to 50
pd.set_option('display.max_columns', 50)

sns.set(rc={'figure.figsize':(20, 10)})
#plt.rcParams["figure.figsize"] = (20,10)

sns.set_theme()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

def actual_and_scaled_df(dataframe,
                         column_prefix,
                         table_data,
                         drop_all=True):
  
  vis_columns = [col for col in dataframe.columns if col.startswith(column_prefix)]
  
  if drop_all:
    df = dataframe[vis_columns].copy().drop('All')
  else:     
    df = dataframe[vis_columns].copy()
 
  df['actual_or_scaled'] = 'Count'
  
  try:
    df_scaled = dataframe[vis_columns].div(dataframe[column_prefix +'_All'], axis=0)
  except:
    df_scaled = dataframe[vis_columns].div(dataframe[column_prefix +'_sum'], axis=0)    
    
  df_scaled['actual_or_scaled'] = 'Proportion of '+ column_prefix
  
  df = df.append(df_scaled)
  
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Data

# COMMAND ----------

db_name = 'externaluc_flight_analysis_db'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Terminals Datasets

# COMMAND ----------

table_name = 'arrivals_per_terminal_trends_v1'
db_and_tablename = db_name +'.'+ table_name

spark_df_per_terminal = spark.table(db_and_tablename)

index = 'aircraftTerminal'

df_per_terminal = spark_df_per_terminal.toPandas().set_index(index)

# COMMAND ----------

df_per_terminal.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Origin Airports Dataset

# COMMAND ----------

table_name = 'arrivals_per_origin_trends_v2'
db_and_tablename = db_name +'.'+ table_name

spark_df_per_origin = spark.table(db_and_tablename)

index = 'originName'

df_per_origin = spark_df_per_origin.toPandas().set_index(index).fillna(0)

# COMMAND ----------

#samples = df_per_origin.sample(5).index
samples = ['All', 'Bahrain', 'Narita/Tokyo', 'New Delhi']

df_per_origin.loc[samples]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Airlines Dataset

# COMMAND ----------

table_name = 'arrivals_per_airline_trends_v1'
db_and_tablename = db_name +'.'+ table_name

spark_df_per_airline = spark.table(db_and_tablename)

index = 'airlineName'

df_per_airline = spark_df_per_airline.toPandas().set_index(index).fillna(0)

# COMMAND ----------

display(
  spark_df_per_airline
)

# COMMAND ----------

#samples = df_per_origin.sample(5).index
samples = ['All', 'Emirates', 'Flydubai', 'Kuwait Airways']

df_per_airline.loc[samples]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic Analysis

# COMMAND ----------

df_per_origin.describe()

# COMMAND ----------

df_per_airline.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC #### Per Origin Airport

# COMMAND ----------

table_data = 'Origin Airport'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Flight Status

# COMMAND ----------

column_prefix = 'flightStatus'

df = actual_and_scaled_df(df_per_origin, column_prefix, table_data=table_data)

df.head()

# COMMAND ----------

g = sns.catplot(data = df,
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5,
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Traffic Type

# COMMAND ----------

column_prefix = 'trafficType'

df = actual_and_scaled_df(df_per_origin, column_prefix, table_data=table_data)

df.loc['Cairo']

# COMMAND ----------

g = sns.catplot(data = df,
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of count of each "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Traffic Type

# COMMAND ----------

column_prefix = 'aircraftTerminal'

df = actual_and_scaled_df(df_per_origin, column_prefix, table_data=table_data)

df.loc['Cairo']

# COMMAND ----------

g = sns.catplot(data = df,
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of count of each "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Baggage Count

# COMMAND ----------

column_prefix = 'baggageClaimUnit'

df = actual_and_scaled_df(df_per_origin, column_prefix, table_data=table_data)

df.loc['Cairo']

# COMMAND ----------

g = sns.catplot(data = df.drop(columns='baggageClaimUnit_sum'),
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of count of each "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Per Airline

# COMMAND ----------

table_data = 'Airline'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Flight Status

# COMMAND ----------

column_prefix = 'flightStatus'

df = actual_and_scaled_df(df_per_airline, column_prefix, table_data=table_data)

# COMMAND ----------

g = sns.catplot(data = df,
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5,
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Traffic Type

# COMMAND ----------

column_prefix = 'trafficType'

df = actual_and_scaled_df(df_per_airline, column_prefix, table_data=table_data)

# COMMAND ----------

g = sns.catplot(data = df,
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of count of each "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Traffic Type

# COMMAND ----------

column_prefix = 'aircraftTerminal'

df = actual_and_scaled_df(df_per_airline, column_prefix, table_data=table_data)

# COMMAND ----------

g = sns.catplot(data = df,
                col='actual_or_scaled', kind='strip', orient='h',
                sharex=False, aspect=1.5
                )

g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
g.fig.suptitle("Plot of count of each "+ column_prefix +' per '+ table_data, size=16)
g.fig.subplots_adjust(top=.8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Baggage Count

# COMMAND ----------

column_prefix = 'baggageClaimUnit'

df = actual_and_scaled_df(df_per_airline, column_prefix, table_data=table_data)

# COMMAND ----------

g = sns.displot(data=df.loc[df['actual_or_scaled'] == 'Count'], x='baggageClaimUnit_avg', bins=20, height=6, aspect=1.5)

# g.set_titles(col_template="Distribution of {col_name} per "+ table_data)
# g.fig.suptitle("Plot of count of each "+ column_prefix +' per '+ table_data, size=16)
# g.fig.subplots_adjust(top=.8)

# COMMAND ----------


