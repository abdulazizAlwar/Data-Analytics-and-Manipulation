# Databricks notebook source
# MAGIC %md
# MAGIC ### Version 1.00

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Background
# MAGIC 
# MAGIC This open dataset was accuired from the following Dubai Government Open Data portal: <br>
# MAGIC https://www.dubaipulse.gov.ae/data/dubai-airports-flight-info/da_flight_information_arrivals-open
# MAGIC 
# MAGIC The dataset structure is Time-series of flight arrivals with mostly categorical attributes like flight status, airline, origin airport, etc. The only numeric column is bagge claim count which can be used to estimate the number of passengers.
# MAGIC 
# MAGIC This notebook will be used to aggregate the data to a more analysis friendly tables such as A Per Airline Dataset, A Per Origin Dataset and so on.

# COMMAND ----------

#Column Metadata
metadata = spark.table('externaluc_flight_analysis_db.arrivals_metadata_v1')

display(metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### TODO:
# MAGIC 
# MAGIC 1. Create Per Airline Dataset
# MAGIC 2. Create Per Origin Dataset
# MAGIC 3. Create Per Terminal Dataset
# MAGIC 4. Columns to Create: 
# MAGIC   a. Value count of each categorical column unique values <br>
# MAGIC   b. Sum & Average of bagge count <br>
# MAGIC   c. Each Month (1-12) totals of the above <br>
# MAGIC   e. Most common flight time, aircraft parking position, joint flight number, origin/airline, via origin/airline etc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import numpy as np
import pandas as pd

# COMMAND ----------

# Set ipython's max row display
pd.set_option('display.max_row', 1000)

# Set iPython's max column width to 50
pd.set_option('display.max_columns', 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Dataframe & Basic Analysis

# COMMAND ----------

db_name = 'externaluc_flight_analysis_db'
table_name = 'original_flight_information_arrivals_v1'
db_and_tablename = db_name +'.'+ table_name

spark_df = spark.table(db_and_tablename)

spark_df = spark_df.select('*')

# COMMAND ----------

# Now to we simply use the .toPandas() function to convert the spark dataframe to a pandas dataframe

df_original = spark_df.toPandas()

# COMMAND ----------

# One thing I like to to do is copy the original df so I can come back to it later
df = df_original.copy()

# COMMAND ----------

df.head(3)

# COMMAND ----------

df.shape

# COMMAND ----------

#df.dtypes # For column datatypes only
df.info(verbose = True)

# COMMAND ----------

#Some columns are redundant, are all nulls or are in arabic so let's remove them
df.columns

# COMMAND ----------

drop_columns = [
  'airlineNameA', 'originNameA', 'viaNameA', 'flightStatusTextA', #In Arabic and redundant
  'estimatedInblockTime', #All values are null
  'airlineCode_icao', 'origin_icao', 'via_icao', 'destination_icao', 'aircraft_icao', #Redundant with the IATA columns
  'trafficTypeCode', #Redundant with the trafficType
  ]

# COMMAND ----------

df = df.drop(columns=drop_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Aggregation Dataframes

# COMMAND ----------

index = 'originName'

# COMMAND ----------

samples = df.drop_duplicates(subset=index)[index].sample(5)
samples

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using Crosstab [Unique Value Counts]

# COMMAND ----------

df.columns

# COMMAND ----------

columns = 'destination_iata'

df_ct = pd.crosstab(df[index], 
                   df[columns], 
                   margins = True,
                   dropna = False)

df_ct.columns = [df_ct.columns.name +'_'+ col for col in df_ct.columns.values]

df_terminal_status = df_ct 
df_terminal_status.loc[samples]

# COMMAND ----------

columns = ['flightStatus', 'trafficType', 'arrivalOrDeparture', 'destination_iata', 'aircraftTerminal',]

df_ct_all = pd.DataFrame()

for column in columns:
  df_ct = pd.crosstab(df[index], 
                     df[column], 
                     margins = True,
                     dropna = False)

  df_ct.columns = [df_ct.columns.name +'_'+ col for col in df_ct.columns.values]
  
  df_ct_all = pd.concat([df_ct_all, df_ct], axis=1)
  
df_ct_all.loc[samples]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find Most Common Unique Value

# COMMAND ----------

column = 'flightNumber'

groupby = df[[column] + [index]].groupby(index).agg({
  column: [(column +'_mostFrequent', lambda x:x.value_counts().index[0]), 
           (column +'_mostFrequent_count', lambda x:x.value_counts()[0]),
          ]
})

groupby.columns = groupby.columns.droplevel()
groupby.loc[samples]

# COMMAND ----------

columns = ['flightNumber', 'airlineCode_iata', 'airlineName', 'viaName', 'aircraftParkingPosition', 'jointFlightNumber', 'aircraft_iata']

df_frequent_all = pd.DataFrame()

for column in columns:
  
  agg_functions = {
    column: [
       (column +'_mostFrequent', lambda x:x.value_counts().index[0]), 
       (column +'_mostFrequent_count', lambda x:x.value_counts()[0]),   
    ]
  }
  
  df_frequent = df[[column] + [index]].dropna().groupby(index, dropna=True).agg(agg_functions)
  
  df_frequent.loc['All', (column, agg_functions[column][0][0])] = df[[column]].apply(agg_functions[column][0][1])[0]
  df_frequent.loc['All', (column, agg_functions[column][1][0])] = df[[column]].apply(agg_functions[column][1][1])[0]
  
  df_frequent.columns = df_frequent.columns.droplevel()
  df_frequent_all = pd.concat([df_frequent_all, df_frequent], axis=1)

df_frequent_all.loc[samples]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Numerical Columns

# COMMAND ----------

column = 'baggageClaimUnit'

agg_functions ={
    column: [
       (column +'_sum', 'sum'), 
       (column +'_avg', 'mean'),
       (column +'_median', 'median'),
       (column +'_min', 'min'),      
       (column +'_max', 'max'), 
    ]
  }

pivot = pd.pivot_table(df, fill_value=0, index=index, aggfunc=agg_functions)

for element in agg_functions[column]:
  pivot.loc['All', (column, element[0])] = df[column].apply(element[1])

pivot.columns = pivot.columns.droplevel()

df_numeric_all = pivot
df_numeric_all.loc[samples]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Time-Series Columns [Find most common day of week, month, etc]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join DFs

# COMMAND ----------

dataframes = [
 df_ct_all,
 df_frequent_all,
 df_numeric_all,
]

df_per_origin = pd.concat(dataframes, axis=1)
df_per_origin = df_per_origin.reset_index()

# COMMAND ----------

df_per_origin.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Table

# COMMAND ----------

#Here we will will store our database name and table name
table_name = 'arrivals_per_origin_trends_v2'
db_and_tablename = db_name +'.'+ table_name

table = spark.createDataFrame(df_per_origin)

# COMMAND ----------

display(table)

# COMMAND ----------

(table.write #Write the table
#.format("parquet") #Try parquet format first as it's the best practice. Use csv format if parquet is not usable
.format("csv") #Format of table. CSV is slowest to read but the only format found to be compatble with our dataset column names
.saveAsTable(db_and_tablename)) #Use specified db and table name

# COMMAND ----------


