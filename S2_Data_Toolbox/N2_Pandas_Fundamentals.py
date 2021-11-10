# Databricks notebook source
# MAGIC %md
# MAGIC ### Version 1.0

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO:
# MAGIC 
# MAGIC 0. **Some Databricks Table Basics** <br>
# MAGIC   a. Create a table & databse in Databricks <br>
# MAGIC   b. Reading a table from Databricks with filters <br>
# MAGIC   c. [Extra] Unzip & read files from Blob Storage and save to Databricks table <br>
# MAGIC   
# MAGIC 1. **Babies First Pandas Dataframe** <br>
# MAGIC a. Read table from Databricks and store to Dataframe <br>
# MAGIC b. Dataframe useful functions (head, describe, info, copy, dtypes etc) <br>
# MAGIC c. Filter dataframes with .loc <br>
# MAGIC 
# MAGIC 2. **More Things to Do with Dataframes** <br>
# MAGIC a. Apply basic functions on DF (sum two columns, change datatypes and more) <br>
# MAGIC b. More DF functions (...) <br>
# MAGIC c. More on filtering dataframes (multiple conditions, using booleans, etc) <br>
# MAGIC d. Sort values, fillna, unique values, unique value counts and more <br>
# MAGIC 
# MAGIC 3. **Groupby and why you will be doing this a lot with Pandas too** <br>
# MAGIC a. More on groupby and aggregation and why is it so important <br>
# MAGIC b. Aggregate by Mean, Sum, First, Max, Head(top 5 values per day for example), etc <br>
# MAGIC c. Aggregate using more than one column, Exclude columns from aggregation <br>
# MAGIC 
# MAGIC 4. **Working with more than one DF** <br>
# MAGIC a. Concat, Join, Split DFs <br>
# MAGIC b. Keeping track of dataframes <br>
# MAGIC c. Using two dataframes together (For plotting, Normalization for ML, etc) <br>
# MAGIC d. Adding a column to an existing dataframe <br>
# MAGIC 
# MAGIC 5. **Other Nice Things** <br>
# MAGIC a. Indexes and multi-indexes and why they are so confusing <br>
# MAGIC b. Rename columns, change column type, etc <br>
# MAGIC c. Encoding using a dictionary <br>
# MAGIC d. Pandas and Numpies <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Dataset Background
# MAGIC 
# MAGIC This open dataset was accuired from the following Dubai Government Open Data portal: <br>
# MAGIC https://www.dubaipulse.gov.ae/data/dubai-airports-flight-info/da_flight_information_arrivals-open
# MAGIC 
# MAGIC The dataset structure is Time-series of flight arrivals with mostly categorical attributes like flight status, airline, origin airport, etc. The only numeric column is baggage claim count which can be used to estimate the number of passengers.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Some Databricks Table Basics

# COMMAND ----------

# MAGIC %md
# MAGIC First let's upload our dataset

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /dbfs/FileStore/abdulaziza_files
# MAGIC cd /dbfs/FileStore/abdulaziza_files
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/abdulaziza_files
# MAGIC ls 

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/FileStore/abdulaziza_files/flight_information_arrivals.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC However, it might be easier to organize if we save our dataset in a Databricks table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --We can inject an SQL command in a Python notebook using the above magic command
# MAGIC 
# MAGIC CREATE DATABASE study_abdulaziza_db COMMENT 'This is a temporary database for the class' LOCATION '/dbfs/FileStore/abdulaziza_files'

# COMMAND ----------

#Here we will will store our database name and table name
db_name = 'study_abdulaziza_db'
table_name = 'dxb_flight_arrivals'
db_and_tablename = db_name +'.'+ table_name

# COMMAND ----------

#Read the table to a spark dataframe from a csv file
table = spark.read.csv("/FileStore/abdulaziza_files/flight_information_arrivals.csv", header="true", inferSchema="true")

(table.write #Write the table
.format("parquet") #Try parquet format first as it's the best practice. Use csv format if parquet is not usable
#.format("csv") #Format of table. CSV is slowest to read but the only format found to be compatble with our dataset column names
.saveAsTable(db_and_tablename)) #Use specified db and table name

# COMMAND ----------

#read the table into a spark dataframe from a Databricks table
spark_df = spark.table(db_and_tablename)
display(spark_df)

# COMMAND ----------

spark_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC There a lot of columns that aren't really useful for us. So let's select the columns we want and ignore the rest before converting to a pandas dataframe. 
# MAGIC 
# MAGIC This is especially important when working with very large datasets, as working with less columns will improve peformance and speed tremendously.

# COMMAND ----------

# For example the 'iata' codes seem redundant with the 'icao' codes. So let's ignore all those columns by commenting them out. 

keepColumns = [
 #'aodbUniqueField',
 'flightStatus',
 'aircraftRegistration',
 'tenMileOut',
 'flightNumber',
 #'trafficTypeCode',
 'arrivalOrDeparture',
 'lastChanged',
 'airlineCode_iata',
 #'airlineCode_icao',
 'jointFlightNumber',
 #'origin_iata',
 'origin_icao',
 'via_iata',
 #'via_icao',
 'destination_iata',
 #'destination_icao',
 'aircraft_iata',
 #'aircraft_icao',
 'flightStatusCode',
 'publicScheduledDateTime',
 'scheduledInblockTime',
 'estimatedInblockTime',
 'actualLandingTime',
 'actualInblockTime',
 'aircraftParkingPosition',
 'baggageClaimUnit',
 'airlineName',
 'originName',
 'viaName',
 'trafficType',
 'aircraftTerminal'
]

# COMMAND ----------

spark_df = spark.table(db_and_tablename)

spark_df = spark_df.select(*keepColumns)

display(spark_df)

# COMMAND ----------

# Now to we simply use the .toPandas() function to convert the spark dataframe to a pandas dataframe

df_original = spark_df.toPandas()

df_original.head()

# COMMAND ----------

# One thing I like to to do is copy the original df so I can come back to it later
df = df_original.copy()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Babies First (or Third) Pandas Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now that we have a dataframe. Let's go through a few useful functions.

# COMMAND ----------

# Set ipython's max row display
pd.set_option('display.max_row', 1000)

# Set iPython's max column width to 50
pd.set_option('display.max_columns', 50)

# COMMAND ----------

df.tail(3)

# COMMAND ----------

df.shape

# COMMAND ----------

#df.dtypes # For column datatypes only
df.info(verbose = True)

# COMMAND ----------

df.describe(include='all')

# COMMAND ----------

# The column 'estimatedInblockTime' has 0 values so let's drop it. There are other columns that we don't understand so let's drop those too

dropColumns = ['estimatedInblockTime', 'aircraftRegistration']

df = df.drop(columns=dropColumns)

# COMMAND ----------

df.head(1)

# COMMAND ----------

df.columns

# COMMAND ----------

columns = ['flightStatus', 'airlineName', 'originName', 'viaName', 'trafficType',
       'aircraftTerminal']
df[columns].head()

# COMMAND ----------

#let's see how many airlines are in the dataset
df['airlineName'].nunique()

# COMMAND ----------

#Let's find all the airline names
df['airlineName'].unique()

# COMMAND ----------

df['airlineName'].value_counts()

# COMMAND ----------

#How about the count of cancelled flights per airline?

df_cancelled = df.loc[df['flightStatus'] == 'Cancelled']

df_cancelled['airlineName'].value_counts()

# COMMAND ----------

type(df['airlineName'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering & Creating new Data

# COMMAND ----------

#What about the ratio of cancelled flights to total flights per air line? Let's join the above into one dataframe

df_airline_counts = pd.DataFrame()

df_airline_counts['totalCount'] = df['airlineName'].value_counts()
df_airline_counts['cancelledCount'] = df_cancelled['airlineName'].value_counts()

df_airline_counts.head()

# COMMAND ----------

#Now that we have the counts dataframe. let's calculate the ratio!

df_airline_counts['cancelledRatio'] = df_airline_counts['cancelledCount'] / df_airline_counts['totalCount']

df_airline_counts.head()

# COMMAND ----------

#Now let's sort our dataframe by the airlines with the highest cancelled ratio

df_airline_counts = df_airline_counts.sort_values('cancelledRatio', ascending=False)

df_airline_counts.head(10)

# COMMAND ----------

#It seems That most of the top are airlines with a small number of flighs. So let's filter to airlines with more than 100 flights.

df_airline_counts.loc[df_airline_counts['totalCount'] > 100].head(10)

# COMMAND ----------

#We can very simply plot the above data using the .plot() function

df_airline_counts.plot()

# COMMAND ----------

#Hmmm let's try a different way to plot it

df_airline_counts.loc[df_airline_counts['totalCount'] > 1000].plot(kind='bar')

# COMMAND ----------

df_airline_counts[['totalCount', 'cancelledCount']].loc[df_airline_counts['totalCount'] > 1000].plot.bar(stacked=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## More Complex Filters

# COMMAND ----------

#How about we filter to all airlines in terminal 1

df.loc[df['aircraftTerminal'] == 1] 

# COMMAND ----------

# MAGIC %md
# MAGIC That's weird? It doesn't return anything...
# MAGIC 
# MAGIC What seems to be wrong?

# COMMAND ----------

df.loc[df['aircraftTerminal'] == '1']['airlineName'].value_counts()

# COMMAND ----------

#How about we look at the count of flights for each airline for each terminal?

df_airline_counts_perterminal = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  df_airline_counts_perterminal['terminal_' +terminal+ '_count'] = df.loc[df['aircraftTerminal'] == terminal]['airlineName'].value_counts()
  
df_airline_counts_perterminal.head()  

# COMMAND ----------

# We have a lot of nulls above, but in our case null means 0 (No flights in that terminal)

df_airline_counts_perterminal = df_airline_counts_perterminal.fillna(value=0)

df_airline_counts_perterminal.head()

# COMMAND ----------

#Let's also reset the index and make into a column
df_airline_counts_perterminal = df_airline_counts_perterminal.reset_index()

df_airline_counts_perterminal.head()

# COMMAND ----------

#Let's give the index column a name

df_airline_counts_perterminal = df_airline_counts_perterminal.rename(
  columns = {'index': 'airline'}
)
df_airline_counts_perterminal.head()

# COMMAND ----------

#Now let's look at Emirates Airlines counts per terminal
pd.DataFrame(df_airline_counts_perterminal.loc['Emirates']).fillna(value=0) #Because the airline name is now the index, you don't have to specify the column name

# COMMAND ----------

airlines = ['Emirates', 'Flydubai']

df_airline_counts_perterminal.loc[df_airline_counts_perterminal['airline'].isin(airlines)].plot.bar(x='airline')

# COMMAND ----------

df['flightStatus'].unique()

# COMMAND ----------

#How about we look at the count of calncelled flights for each airline for each terminal?

df_airline_counts_perterminal_cancelled = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  df_airline_counts_perterminal_cancelled['terminal_' +terminal+ '_count'] = df.loc[
    
    (df['aircraftTerminal'] == terminal) & (df['flightStatus'] == 'Cancelled')
    
  ]['airlineName'].value_counts()
  
df_airline_counts_perterminal_cancelled = df_airline_counts_perterminal_cancelled.fillna(0)
  
df_airline_counts_perterminal_cancelled.head()

# COMMAND ----------

#How about we look at all the arrived as well as the landed flights per terminal?

df_airline_counts_perterminal_cancelled_or_delayed = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  df_airline_counts_perterminal_cancelled_or_delayed['terminal_' +terminal+ '_count'] = df.loc[
    
    (df['aircraftTerminal'] == terminal) & 
    ((df['flightStatus'] == 'Cancelled') | (df['flightStatus'] == 'Delayed'))
    
  ]['airlineName'].value_counts()
  
df_airline_counts_perterminal_cancelled_or_delayed = df_airline_counts_perterminal_cancelled_or_delayed.fillna(0)
  
df_airline_counts_perterminal_cancelled_or_delayed.head()

# COMMAND ----------

#Finally how about all the flights that were not cancelled

df_airline_counts_perterminal_not_cancelled = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  df_airline_counts_perterminal_not_cancelled['terminal_' +terminal+ '_count'] = df.loc[
    
    (df['aircraftTerminal'] == terminal) & 
    (df['flightStatus'] != 'Cancelled')
    
  ]['airlineName'].value_counts()
  
df_airline_counts_perterminal_not_cancelled = df_airline_counts_perterminal_not_cancelled.fillna(0)
  
df_airline_counts_perterminal_not_cancelled.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's do something different here and look at the flights from Tokyo

# COMMAND ----------

#First let's find all the flights from Tokyo.
df.loc[df['originName'].str.contains("Tokyo")].head()

# COMMAND ----------

df.loc[df['originName'].str.contains("Tokyo")]['originName'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Tokyo has another airport called Haneda but it seems that the string 'Tokyo' isn't included in the originName column for it.
# MAGIC 
# MAGIC So let's search for all flights with Tokyo or Haneda in their originName

# COMMAND ----------

#We can use a regex expression
df.loc[
  df['originName'].str.contains("Tokyo|Haneda")
]['originName'].unique()

# COMMAND ----------

#Or we can filter the dataframe using multiple conditions
tokyo_airports = df.loc[
  (df['originName'].str.contains("Tokyo") | df['originName'].str.contains("Haneda"))
]['originName'].unique()

tokyo_airports

# COMMAND ----------

#Now let's look at the count of flights coming from Tokyo per airline per terminal

df_airline_counts_perterminal_tokyo = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  df_airline_counts_perterminal_tokyo['terminal_' +terminal+ '_count'] = df.loc[
    (df['aircraftTerminal'] == terminal) &
    (df['originName'].isin(tokyo_airports))
  ]['airlineName'].value_counts()

#Also let's try transposing the table
df_airline_counts_perterminal_tokyo = df_airline_counts_perterminal_tokyo.T 
  
df_airline_counts_perterminal_tokyo.head()  

# COMMAND ----------

# How about the count of flights coming from Tokyo per airline per terminal and airport?

df_airline_counts_perterminal_tokyo_perairport = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  for airport in tokyo_airports:
    df_airline_counts_perterminal_tokyo_perairport['terminal_' +terminal+ '_from' +airport+ '_count'] = df.loc[
      (df['aircraftTerminal'] == terminal) &
      (df['originName'] == airport)
    ]['airlineName'].value_counts()

df_airline_counts_perterminal_tokyo_perairport = df_airline_counts_perterminal_tokyo_perairport.T     
    
df_airline_counts_perterminal_tokyo_perairport.head()  

# COMMAND ----------

#Since all flights or on Emirates airlines anyway, Why don't we have the columns as the terminal and the airport as the rows instead?

df_counts_perterminal_tokyo_perairport = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
  df_counts_perterminal_tokyo_perairport['terminal_' +terminal+ '_count'] = df.loc[
    (df['aircraftTerminal'] == terminal) &
    (df['originName'].isin(tokyo_airports))
  ]['originName'].value_counts()
  
df_counts_perterminal_tokyo_perairport

# COMMAND ----------

#It seems that all flights are arriving to terminal 3 so let's pick something else to show

df_flight_counts_tokyo_perairport = pd.DataFrame()

for flight in df['flightNumber'].unique():
  df_flight_counts_tokyo_perairport['flightNumber_' +str(flight)+ '_count'] = df.loc[
    (df['flightNumber'] == flight) &
    (df['originName'].isin(tokyo_airports))
  ]['originName'].value_counts()
  
df_flight_counts_tokyo_perairport

# COMMAND ----------

# can see that most flights are null. What we can do is transpose then drop nulls to find all valid flights 

df_flight_counts_tokyo_perairport = df_flight_counts_tokyo_perairport.T.dropna()

df_flight_counts_tokyo_perairport

# COMMAND ----------

#Hmmmm, most flights from Narita are EK319. but there's a single EK 9441 flight. Let's check it out!

df.loc[df['flightNumber'] == 'EK 9441']

# COMMAND ----------

# MAGIC %md
# MAGIC The flight was in May so probably an emergency flight related to Covid? Feel free to look into it more if you're curious!

# COMMAND ----------

df.head()

# COMMAND ----------

#Last one I promise. How about we combine all we learned for a really needlessly complicated query.

complex_df = pd.DataFrame()

for terminal in df['aircraftTerminal'].unique():
    for airport in tokyo_airports:
      complex_df['terminal_' +terminal+ '_from' +airport+ '_count'] = df.loc[

        (df['aircraftTerminal'] == terminal) &
        (df['originName'] == airport) &
        ((df['flightStatus'] == 'Arrived') | (df['flightStatus'] == 'Delayed'))

      ]['trafficType'].value_counts()
    
complex_df.T.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Numeric Columns

# COMMAND ----------

# MAGIC %md
# MAGIC Here is where the real juicy parts come in. Usually we would be working with numeric data, and there are a variety of ways to analyse them.

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC The only numeric column we have is `baggageClaimUnit`, so let's try playing with it.
# MAGIC 
# MAGIC For numeric data we can use the dataframe function `.groupby()` to aggregate our data. 
# MAGIC This works with 3 main components:
# MAGIC 1. **The Group By column.** The categorical column or columns to separate our data. Each unique value in our data will be it's own row.
# MAGIC 2. **The columns to group.** The numerical (or categorical) data to be aggregated. 
# MAGIC 3. **The aggregation function.** The function to use on the data to be aggregated. It can be `.sum()`, `.mean()`, `.median()`, and many more.
# MAGIC 
# MAGIC Previously we worked on count of unique values for each column using `value_counts()`.  This is essentially using group by on the column with a `.count()` function. As it is only the count of each value, it will take the row count of each column.

# COMMAND ----------

#Let's see with terminals
pd.DataFrame(
  df['aircraftTerminal'].value_counts()
)

# COMMAND ----------

df.groupby('aircraftTerminal').count()

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see above. It will return the value count of each column for each terminal. However, it will not include the null values into the count. Basically, the more null values a row has, the smaller the count will be.

# COMMAND ----------

#Now let's work with numeric data
df_perairline_sum = df.groupby('airlineName').sum()

df_perairline_sum = df_perairline_sum.sort_values(by='baggageClaimUnit', ascending=False)

df_perairline_sum.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Unlike the `.count()` function, the `.sum()` function will only group numeric columns. 
# MAGIC 
# MAGIC We can also try other functions like `.mean()`
# MAGIC 
# MAGIC We will also use `as_index=False`, so that the groupby column is not set as index which will make plotting easier

# COMMAND ----------

#Now let's work with numeric data
df_perairline_avg = df.groupby('airlineName', as_index=False).mean()

df_perairline_avg = df_perairline_avg.sort_values(by='baggageClaimUnit', ascending=False)

df_perairline_avg.head()

# COMMAND ----------

#We can also reset the index of an existing dataframe
df_perairline_sum = df_perairline_sum.reset_index()

df_perairline_sum.head()

# COMMAND ----------

#Let's find all the airlines with more baggage claims than the mean
airlines_morethan_mean = df_perairline_sum.loc[
  df_perairline_sum['baggageClaimUnit'] > df_perairline_sum['baggageClaimUnit'].mean()
]

airlines_morethanmean.tail()

# COMMAND ----------

#Let's try plotting

airlines = airlines_morethanmean['airlineName']

df_perairline.loc[df_perairline['airlineName'].isin(airlines)].plot.bar(x='airlineName')

# COMMAND ----------

# MAGIC %md
# MAGIC But what if want to use multiple functions? Here we can use the `.agg()` function.

# COMMAND ----------

df_perairline = df.groupby('airlineName').agg(['sum', 'mean', 'median', 'max'])

df_perairline.head()

# COMMAND ----------

# The above is using multiple column levels and is hard to work with. So let's do something else
df_perairline.columns = df_perairline.columns.to_flat_index()

df_perairline.head()

# COMMAND ----------

#as_index=False doesn't work here so let's reset the index manually

df_perairline = df_perairline.reset_index()

df_perairline.head()

# COMMAND ----------

df_perairport = df.groupby('originName').agg(['sum', 'mean', 'median', 'max'])

df_perairport.columns = df_perairport.columns.to_flat_index()
df_perairport = df_perairport.reset_index()

df_perairport.head()

# COMMAND ----------

#We can also groupby multiple columns
df_perairport_perairline = df.groupby(['originName', 'airlineName']).sum()

df_perairport_perairline.head()

# COMMAND ----------

#Pay attention to the order of the groupby columns
df_perairport_perairline = df.groupby(['airlineName', 'originName']).sum()

df_perairport_perairline.head()
