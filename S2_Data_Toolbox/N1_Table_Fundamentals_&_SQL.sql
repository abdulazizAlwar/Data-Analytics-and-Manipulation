-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### **Version 1.1**
-- MAGIC 
-- MAGIC Version History:
-- MAGIC 
-- MAGIC **Version 1.1** <br>
-- MAGIC -Added dataset description
-- MAGIC 
-- MAGIC **Version 1.0** <br>
-- MAGIC -First version ready end to end

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TODO:
-- MAGIC 
-- MAGIC 1. **Some Command Line & File Structure Basics** <br>
-- MAGIC   a. Command Line basics <br>
-- MAGIC   b. Create a folder to upload CSV file <br>
-- MAGIC   c. [Extra] Set up connection to and read files from Blob Storage <br>
-- MAGIC   
-- MAGIC 2. **Babies First SQL Table** <br>
-- MAGIC a. Creating a Table from a CSV File <br>
-- MAGIC b. Reading a Table <br>
-- MAGIC c. Reading a Table with Filters <br>
-- MAGIC 
-- MAGIC 3. **More Things to Do with Tables** <br>
-- MAGIC a. Apply basic transformation on table (Change all letters in column values to uppercase) <br>
-- MAGIC b. Merge two columns into one column <br>
-- MAGIC c. Filter table based on multiple conditions <br>
-- MAGIC d. Sort table <br>
-- MAGIC  
-- MAGIC 4. **Groupby and why you will be doing this a lot** <br>
-- MAGIC a. Basics of groupby and aggregation and why is it so useful <br>
-- MAGIC b. Aggregate by Mean and Sum <br>
-- MAGIC c. Aggregate using more than one column <br>
-- MAGIC 
-- MAGIC 5. **Merging Tables** <br>
-- MAGIC a. Kinds if merges and when you want to use them <br>
-- MAGIC b. Union two tables (Up and Down) <br>
-- MAGIC c. Join two tables (Left and Right) <br>
-- MAGIC d. Kinds of Joins
-- MAGIC 
-- MAGIC 6. **Other Cool Things** <br>
-- MAGIC a. Dropping Tables and Databases <br>
-- MAGIC b. ... <br>
-- MAGIC c. ... <br>
-- MAGIC d. ... <br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Dataset Background
-- MAGIC 
-- MAGIC This open dataset was accuired from the following Dubai Government Open Data portal: <br>
-- MAGIC https://www.dubaipulse.gov.ae/data/dubai-airports-flight-info/da_flight_information_arrivals-open
-- MAGIC 
-- MAGIC The dataset structure is Time-series of flight arrivals with mostly categorical attributes like flight status, airline, origin airport, etc. The only numeric column is baggage claim count which can be used to estimate the number of passengers.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Some Command Line & File Structure Basics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First thing, let's understand how command line works and how to work with files using it.<br>
-- MAGIC 
-- MAGIC Databricks allows us to use a command line statement using thr `%sh` command. This runs the command on the existing cluster and can also be used to access any mounted storages.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The above command shows all files/folders in the current folders <br>
-- MAGIC 
-- MAGIC To go to a different folder we use the below command

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /dbfs/FileStore
-- MAGIC ls

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Let's make a folder here to upload our CSV file

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir /dbfs/FileStore/abdulaziza_files

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls /dbfs/FileStore

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Manual File Upload
-- MAGIC Now let's upload our CSV file to our created folder using the UI. 
-- MAGIC 
-- MAGIC Click on `File`, then `Upload Data` to upload the CSV.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After uploading the file use the below command to check where it is.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /dbfs/FileStore/abdulaziza_files
-- MAGIC ls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Babies First SQL Table**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, let's create a database and table for our CSV file using the following format
-- MAGIC 
-- MAGIC Create a Database with your name
-- MAGIC 
-- MAGIC `CREATE DATABASE study_<your name>_db COMMENT '<write any comment you want>' LOCATION '/dbfs/FileStore/<your name>_files'`
-- MAGIC 
-- MAGIC Create a table for the dataset in the above database
-- MAGIC 
-- MAGIC `CREATE Table study_<your name>_db.dxb_flight_arrivals`

-- COMMAND ----------

-- Let's see the list of Databases in our Workspace first
SHOW DATABASES

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS study_abdulaziza_db COMMENT 'This is a temporary database for learning' LOCATION '/dbfs/FileStore/abdulaziza_files'

-- COMMAND ----------

-- Now let's look at the details of our new little database
DESCRIBE DATABASE EXTENDED study_abdulaziza_db

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #Using the above %python command we can run a Python cell in an SQL notebook. We will use this cell to save our CSV file to a delta table
-- MAGIC 
-- MAGIC file_path = "/FileStore/abdulaziza_files/Flight_Information_Arrivals.csv"
-- MAGIC table = spark.read.csv(file_path, header="true", inferSchema="true")
-- MAGIC 
-- MAGIC #Here we will will store our database name and table name
-- MAGIC db_and_tablename = 'study_abdulaziza_db.dxb_flight_arrivals'
-- MAGIC 
-- MAGIC (table.write #Write the table
-- MAGIC .format("parquet") #Try parquet format first as it's the best practice and runs the fastest.
-- MAGIC #.format("csv") #Try csv if parquet does not work
-- MAGIC .saveAsTable(db_and_tablename)) #Use specified db and table name

-- COMMAND ----------

-- We can also create it using SQL commands. However, it is more complicated and it's easier with Python
-- CREATE Table study_abdulaziza_db.dxb_flight_arrivals USING CSV OPTIONS (path "/dbfs/FileStore/abdulaziza_files/flight_information_arrivals.csv", header "true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Finally let's query the table!

-- COMMAND ----------

SELECT * FROM study_abdulaziza_db.dxb_flight_arrivals

-- COMMAND ----------

-- Let's look at the table details
DESCRIBE TABLE EXTENDED study_abdulaziza_db.dxb_flight_arrivals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC We see that the column `publicScheduledDateTime` is automatically inferred as `timestamp` data type which is great! This means we can use SQL to search for specific flight dates for example.
-- MAGIC 
-- MAGIC Let's look for the flights on 2020-03-18 using the `WHERE` clause.

-- COMMAND ----------

SELECT * FROM study_abdulaziza_db.dxb_flight_arrivals WHERE publicScheduledDateTime = '2022-03-18'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Hmmm, the above query didn't return anything. I wonder why...

-- COMMAND ----------

SELECT * FROM study_abdulaziza_db.dxb_flight_arrivals WHERE DATE(publicScheduledDateTime) = '2022-03-18'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. More Things to Do with Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's say we want the Date as column on it's own so we don't have to cast the timestamp to `Date` everytime.

-- COMMAND ----------

SELECT CAST(publicScheduledDateTime AS DATE) AS publicScheduledDate FROM study_abdulaziza_db.dxb_flight_arrivals 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, let's say we want to filter the table using multiple conditions. For example all flights from a specific country on a specific date range. This operation is also very useful when importing tables before transforming to Pandas, As it will take much less time to tranfsorm a smaller table to Pandas.

-- COMMAND ----------

SELECT * 
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE (DATE(publicScheduledDateTime) >= '2022-03-01' AND DATE(publicScheduledDateTime) <= '2022-04-01')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We Don't need all these columns, let's filter to the columns we really need. This way we can also reorder the columns in any way we want.

-- COMMAND ----------

SELECT originName, publicScheduledDateTime, flightStatus, flightNumber, airlineName
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE (DATE(publicScheduledDateTime) >= '2022-03-01' AND DATE(publicScheduledDateTime) <= '2022-04-01')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Now Let's look at all the flights from Tokyo

-- COMMAND ----------

SELECT originName, publicScheduledDateTime, flightStatus, flightNumber, airlineName
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE originName = 'Tokyo'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Hmmmmm, It seems there is no originName called `Tokyo`, maybe because it also includes the airport name? Let's try something else...

-- COMMAND ----------

SELECT originName, publicScheduledDateTime, flightStatus, flightNumber, airlineName
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE originName LIKE '%Tokyo%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Actually, there are two airports in Tokyo, Haneda and Narita. However, it seems we only got Narita from the above. We can use the `OR` statement to select values from either of two conditions.

-- COMMAND ----------

SELECT originName, publicScheduledDateTime, flightStatus, flightNumber, airlineName
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE originName LIKE '%Tokyo%' 
  OR originName LIKE '%Narita%' 
  OR originName LIKE '%Haneda%'   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Most of the above entries are for Narita and it's a hassle to find the Haneda values manually. We can sort the table to find the rows we want much easier. 

-- COMMAND ----------

SELECT originName, publicScheduledDateTime, flightStatus, flightNumber, airlineName
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE originName LIKE '%Tokyo%' 
  OR originName LIKE '%Narita%' 
  OR originName LIKE '%Haneda%'
ORDER BY originName ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's look at all flights between two specific dates from Tokyo!

-- COMMAND ----------

SELECT originName, publicScheduledDateTime, flightStatus, flightNumber, airlineName
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE (DATE(publicScheduledDateTime) >= '2022-03-01' AND DATE(publicScheduledDateTime) <= '2022-05-01')
  AND originName LIKE '%Tokyo%'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Here we can also see how we can combine multiple And and OR conditions in multiple columns.

-- COMMAND ----------

SELECT publicScheduledDateTime, originName, flightStatus, baggageClaimUnit
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE (DATE(publicScheduledDateTime) >= '2022-03-01' AND DATE(publicScheduledDateTime) <= '2022-04-01')
 AND (
  originName LIKE '%Tokyo%' 
  OR originName LIKE '%Narita%' 
  OR originName LIKE '%Haneda%'
  )
ORDER BY originName ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Challenge
-- MAGIC If we look at the `flightNumber` column, we can see that it includes the airline code then the flight code for that airline. Let's say for example we want the airline code as a column on it's own. let's name this column `airlineCode`. Let's also create a column for the flight code without the airline code called `flightNumber_without_airlineCode`. 
-- MAGIC 
-- MAGIC See if you can use SQL to create a table with the above.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Groupby and why you will be doing this a lot**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Before getting into Groupby, let's first understand some concepts related to aggregation (As well a useful SQL commands)

-- COMMAND ----------

SELECT publicScheduledDateTime, originName, flightStatus, baggageClaimUnit
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE flightStatus = 'Arrived'

-- COMMAND ----------

SELECT publicScheduledDateTime, originName, flightStatus, baggageClaimUnit
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE flightStatus = 'Cancelled'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We see a lot of canncelled flights in 2022 but what if we want to know the count?

-- COMMAND ----------

SELECT COUNT(publicScheduledDateTime)
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE flightStatus = 'Cancelled'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice how we selected the count of one column but our filter condidtion is a different column? 
-- MAGIC 
-- MAGIC Now let's do something intersting, let's look at how many flights were cancelled during a certain period. You can get the data here. https://en.wikipedia.org/wiki/COVID-19_lockdowns

-- COMMAND ----------

SELECT COUNT(publicScheduledDateTime)
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE flightStatus = 'Cancelled'
  AND (DATE(publicScheduledDateTime) >= '2022-03-26' AND DATE(publicScheduledDateTime) <= '2022-04-17')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Not many cancelled flights it seems. Could it be that this dataset only has data on flights cancelled during the flight day? Let's dive deeper into it!
-- MAGIC 
-- MAGIC First, let's look at all the unique flight statuses.

-- COMMAND ----------

SELECT DISTINCT(flightStatus)
FROM study_abdulaziza_db.dxb_flight_arrivals 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's find the count of each flightStatus, but how? This is where groupby comes in!

-- COMMAND ----------

SELECT flightStatus, COUNT(flightStatus)
FROM study_abdulaziza_db.dxb_flight_arrivals 
GROUP BY flightStatus

-- COMMAND ----------

--Now let's look at the count during our period

SELECT flightStatus, COUNT(flightStatus)
FROM study_abdulaziza_db.dxb_flight_arrivals 
  WHERE (DATE(publicScheduledDateTime) >= '2022-03-26' AND DATE(publicScheduledDateTime) <= '2022-04-17')  
GROUP BY flightStatus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Isn't that nice! Now we know how to find the count of each flight status. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The aggregation function we used above was count. However, for numeric columns we can use many other things like Sum, Min, Max and so on.
-- MAGIC 
-- MAGIC For example, let's find the baggleClaim sum from each location. This can be also used to estimate the number of passengers from each location!
-- MAGIC 
-- MAGIC Here we are using two columns instead of one. The column we want to aggregate `baggageClaimUnit`, the aggregation function `SUM` and the the column we want to aggregate our numerical column by `originName`

-- COMMAND ----------

SELECT originName, SUM(baggageClaimUnit) as totalBagageClaim
FROM study_abdulaziza_db.dxb_flight_arrivals 
GROUP BY originName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, we can also sort our results from **highest to lowest** using `ORDER bY <column> DESC` and **lowest to highest** using `ORDER BY <column> ASC`

-- COMMAND ----------

SELECT originName, SUM(baggageClaimUnit) as totalBagageClaim
FROM study_abdulaziza_db.dxb_flight_arrivals 
GROUP BY originName
ORDER BY totalBagageClaim DESC

-- COMMAND ----------

-- Let's say we want to group by Terminal now instead
SELECT aircraftTerminal, SUM(baggageClaimUnit) as totalBagageClaim
FROM study_abdulaziza_db.dxb_flight_arrivals 
GROUP BY aircraftTerminal
ORDER BY totalBagageClaim ASC

-- COMMAND ----------

--Or we want to find the count of arrived flights for each terminal
SELECT aircraftTerminal, COUNT(flightStatus) as arrivedCount
FROM study_abdulaziza_db.dxb_flight_arrivals 
WHERE flightStatus = 'Arrived'
GROUP BY aircraftTerminal
ORDER BY arrivedCount ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you can see. Groupby and aggregation are extremely useful for finding trends in our data. We done the above using SQL just for practice, but it will be much more useful and easier to visualize using Pandas!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Merging Tables**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Remeber above where we counted the flight status counts? Well, what if we want to find that count and compare it to the count during a specific period?

-- COMMAND ----------

SELECT * 
FROM(
  SELECT flightStatus, COUNT(flightStatus) as totalCount
  FROM study_abdulaziza_db.dxb_flight_arrivals 
  GROUP BY flightStatus
) AS a
FULL OUTER JOIN(
  SELECT  flightStatus, COUNT(flightStatus) as countDuringCustomPeriod
  FROM study_abdulaziza_db.dxb_flight_arrivals 
    WHERE (DATE(publicScheduledDateTime) >= '2022-03-26' AND DATE(publicScheduledDateTime) <= '2022-04-17')  
  GROUP BY flightStatus
) AS b
ON a.flightStatus = b.flightStatus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is really helpful for analytics (and much easier to do with Pandas)
-- MAGIC 
-- MAGIC We can even find the ratio between them. Also since the value for everything other than arrived and cancelled is mostly null during the custom period, we can do an inner join instead.

-- COMMAND ----------

SELECT *, (countDuringLockdown / totalCount) as ratioDuringLockdown
FROM(
  SELECT flightStatus, COUNT(flightStatus) as totalCount
  FROM study_abdulaziza_db.dxb_flight_arrivals 
  GROUP BY flightStatus
) AS a
INNER JOIN(
  SELECT  flightStatus, COUNT(flightStatus) as countDuringLockdown
  FROM study_abdulaziza_db.dxb_flight_arrivals 
    WHERE (DATE(publicScheduledDateTime) >= '2022-03-26' AND DATE(publicScheduledDateTime) <= '2022-04-17')  
  GROUP BY flightStatus
) AS b
ON a.flightStatus = b.flightStatus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC As an exercise try to find the ratio of a similar timespan before lockdown. Or you can find the dataset for the previous year and find the ratio for the same timespan.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Other Cool Things**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, let's delete the table and database, as well as any files and folders.
-- MAGIC 
-- MAGIC ##### Be very careful when deleting data! As you can delete important files by mistake!

-- COMMAND ----------

DROP TABLE study_abdulaziza_db.dxb_flight_arrivals;

-- COMMAND ----------

DROP DATABASE study_abdulaziza_db

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC rm -r /dbfs/FileStore/abdulaziza_files

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls /dbfs/FileStore/
