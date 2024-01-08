-- Databricks notebook source
-- MAGIC %python
-- MAGIC print(" Hello Aditya \n Welcome to DataBricks")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Basic Queries

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS People10M;
-- MAGIC CREATE TABLE People10M
-- MAGIC USING parquet
-- MAGIC OPTIONS (
-- MAGIC path "/mnt/training/dataframes/people-10m.parquet",
-- MAGIC header "true");

-- COMMAND ----------


SELECT * FROM People10M;

-- COMMAND ----------

DESCRIBE People10M;

-- COMMAND ----------

SELECT
  firstName,
  middleName,
  lastName,
  birthDate
FROM
  People10M
WHERE
  year(birthDate) > 1990
  AND gender = 'F'

-- COMMAND ----------

SELECT
  firstName,
  lastName,
  salary,
  salary * 0.2 AS savings
FROM
  People10M

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TEMPORARY VIEW

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PeopleSavings AS
SELECT
  firstName,
  lastName,
  year(birthDate) as birthYear,
  salary,
  salary * 0.2 AS savings
FROM
  People10M;

-- COMMAND ----------

SELECT * FROM PeopleSavings;

-- COMMAND ----------

SELECT
  birthYear,
  ROUND(AVG(salary), 2) AS avgSalary
FROM
  peopleSavings
GROUP BY
  birthYear
ORDER BY
  avgSalary DESC

-- COMMAND ----------

DROP TABLE IF EXISTS ssaNames;
CREATE TABLE ssaNames USING parquet OPTIONS (
  path "/mnt/training/ssn/names.parquet",
  header "true"
)

-- COMMAND ----------

SELECT
  *
FROM
  ssaNames
LIMIT
  5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Joining two tables

-- COMMAND ----------

SELECT count(DISTINCT firstName)
FROM SSANames;

-- COMMAND ----------

SELECT count(DISTINCT firstName) 
FROM People10M;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
  SELECT DISTINCT firstName AS ssaFirstName 
  FROM SSANames;

CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
  SELECT DISTINCT firstName 
  FROM People10M

-- COMMAND ----------

SELECT firstName 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName

-- COMMAND ----------

SELECT count(*) 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DATA VISUALIZATION

-- COMMAND ----------

DROP TABLE IF EXISTS movieRatings;
CREATE TABLE movieRatings (
  userId INT,
  movieId INT,
  rating FLOAT,
  timeRecorded INT
) USING csv OPTIONS (
  PATH "/mnt/training/movies/20m/ratings.csv",
  header "true"
);

-- COMMAND ----------

SELECT
  *
FROM
  movieRatings;
  

-- COMMAND ----------

--"CASTING DATA"
SELECT
  rating,
  CAST(timeRecorded as timestamp)
FROM
  movieRatings;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW ratingsByMonth AS
SELECT
  ROUND(AVG(rating), 3) AS avgRating,
  month(CAST(timeRecorded as timestamp)) AS month
FROM
  movieRatings
GROUP BY
  month;

-- COMMAND ----------

SELECT
  *
FROM
  ratingsByMonth
ORDER BY
  avgRating;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #NESTING DATA

-- COMMAND ----------

DROP TABLE IF EXISTS DCDataRaw;
CREATE TABLE DCDataRaw
USING parquet                           
OPTIONS (
    PATH "/mnt/training/iot-devices/data-centers/2019-q2-q3"
    )

-- COMMAND ----------

DESCRIBE EXTENDED DCDataRaw;

-- COMMAND ----------

SELECT * FROM DCDataRaw
ORDER BY RAND()
LIMIT 3;

-- COMMAND ----------

SELECT EXPLODE (source)
FROM DCDataRaw;

-- COMMAND ----------

WITH ExplodeSource  -- specify the name of the result set we will query
AS                  
(                   -- wrap a SELECT statement in parentheses
  SELECT            -- this is the temporary result set you will query
    dc_id,
    to_date(date) AS date,
    EXPLODE (source)
  FROM
    DCDataRaw
)
SELECT             -- write a select statment to query the result set
  key,
  dc_id,
  date,
  value.description,  
  value.ip,
  value.temps,
  value.co2_level
FROM               -- this query is coming from the CTE we named
  ExplodeSource;  
                  

-- COMMAND ----------

DROP TABLE IF EXISTS DeviceData;
CREATE TABLE DeviceData                 
USING parquet
WITH ExplodeSource                       -- The start of the CTE from the last cell
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM DCDataRaw
  )
SELECT 
  dc_id,
  key device_type,                       
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
  
FROM ExplodeSource;



-- COMMAND ----------

SELECT * FROM DeviceData

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Manipulating Data

-- COMMAND ----------

DROP TABLE IF EXISTS outdoorProductsRaw;
CREATE TABLE outdoorProductsRaw USING csv OPTIONS (
  path "/mnt/training/online_retail/data-001/data.csv",
  header "true"
)

-- COMMAND ----------

DESCRIBE outdoorProductsRaw

-- COMMAND ----------

SELECT * FROM outdoorProductsRaw TABLESAMPLE (5 ROWS)

-- COMMAND ----------

SELECT * FROM outdoorProductsRaw TABLESAMPLE (2 PERCENT) ORDER BY InvoiceDate 

-- COMMAND ----------

SELECT count(*) FROM outdoorProductsRaw WHERE Description IS NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TEMP0RARY VIEW

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW outdoorProducts AS
SELECT
  InvoiceNo,
  StockCode,
  COALESCE(Description, "Misc") AS Description,
  Quantity,
  SPLIT(InvoiceDate, "/")[0] month,
  SPLIT(InvoiceDate, "/")[1] day,
  SPLIT(SPLIT(InvoiceDate, " ")[0], "/")[2] year,
  UnitPrice,
  Country
FROM
  outdoorProductsRaw

-- COMMAND ----------

SELECT count(*) FROM outdoorProducts WHERE Description = "Misc" 

-- COMMAND ----------

DROP TABLE IF EXISTS standardDate;
CREATE TABLE standardDate

WITH padStrings AS
(
SELECT 
  InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  LPAD(month, 2, 0) AS month,
  LPAD(day, 2, 0) AS day,
  year,
  UnitPrice, 
  Country
FROM outdoorProducts
)
SELECT 
 InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  concat_ws("/", month, day, year) sDate,
  UnitPrice,
  Country
FROM padStrings;






-- COMMAND ----------

SELECT * FROM standardDate LIMIT 5;

-- COMMAND ----------

DESCRIBE standardDate;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW salesDateFormatted AS
SELECT
  InvoiceNo,
  StockCode,
  to_date(sDate, "MM/dd/yy") date,
  Quantity,
  CAST(UnitPrice AS DOUBLE)
FROM
  standardDate

-- COMMAND ----------

SELECT
  date_format(date, "E") day,
  SUM(quantity) totalQuantity
FROM
  salesDateFormatted
GROUP BY (day)
ORDER BY day


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Data Munging

-- COMMAND ----------

DROP TABLE if EXISTS eventsRaw;
create table eventsRaw USING parquet OPTIONS (
  path "/mnt/training/ecommerce/events/events.parquet",
  header "true"
)

-- COMMAND ----------


describe extended eventsRaw ;

-- COMMAND ----------


SELECT * FROM eventsRaw TABLESAMPLE (1 PERCENT)

-- COMMAND ----------


DROP TABLE IF EXISTS purchaseEvents;
CREATE TABLE purchaseEvents 
WITH tempTable AS (
  SELECT
    ecommerce.purchase_revenue_in_usd purchases,
    event_name,
    CAST(event_previous_timestamp/1000000 AS timestamp) previous_event,
    CAST(event_timestamp/1000000 AS timestamp) event_time,
    geo.city city,
    geo.state state,
    CAST(user_first_touch_timestamp/1000000 AS timestamp) first_touch_time,
    user_id
  FROM
    eventsRaw
)
SELECT
  purchases,
  event_name eventName,
  to_date(previous_event) previousEventDate,
  to_date(event_time) eventDate,
  city,
  state,
  user_id userId
FROM
  tempTable
WHERE
  purchases IS NOT NULL

-- COMMAND ----------

SELECT count(*) FROM purchaseEvents

-- COMMAND ----------

SELECT city, state FROM purchaseEvents ORDER BY purchases DESC LIMIT 1

-- COMMAND ----------

-- Total purchases by day of week
SELECT round(sum(purchases), 2) totalPurchases, date_format(eventDate, "E") day FROM purchaseEvents GROUP BY (day) 


-- COMMAND ----------

-- Average purchases by date of purchase
SELECT ROUND(avg(purchases),2) avgPurchases, eventDate FROM purchaseEvents GROUP BY (eventDate)

-- COMMAND ----------

--Total purchases by state
SELECT
  ROUND(SUM(purchases), 2) totalPurchases,
  state
FROM
  purchaseEvents
GROUP BY
  state

-- COMMAND ----------

DROP TABLE IF EXISTS usersRaw;
CREATE TABLE usersRaw USING parquet OPTIONS (
  path "/mnt/training/ecommerce/users/users.parquet",
  header "true"
);

-- COMMAND ----------

-- Create a confirmed email list
SELECT
  purchases, eventDate, city, state, email
FROM
  purchaseEvents
  JOIN usersRaw
WHERE
  email IS NOT NULL
  AND usersRaw.user_id = purchaseEvents.userId


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Sharing Insights

-- COMMAND ----------


DROP TABLE IF EXISTS DeviceRaw;
create table DeviceRaw
using json options(
  path "/mnt/training/iot-devices/data-centers/energy.json"
);

-- COMMAND ----------

select * from DeviceRaw tablesample (5 rows)

-- COMMAND ----------

--Create view
CREATE
OR REPLACE TEMPORARY VIEW DCDevices AS
SELECT
  device_id deviceId,
  device_type deviceType,
  battery_level batteryLevel,
  co2_level co2Level,
  signal,
  temps,
  to_timestamp(timestamp, 'yyyy/MM/dd HH:mm:ss') time
FROM
  DeviceRaw;

-- COMMAND ----------

--Flag records with defective batteries
SELECT
  deviceId,
  batteryLevel,
  EXISTS(batteryLevel, level -> level < 0) needService
FROM
  DCDevices
ORDER BY deviceId, batteryLevel 

-- COMMAND ----------

--Display high CO2 levels
SELECT
  deviceId,
  deviceType,
  highCO2,
  time
FROM (SELECT * , FILTER(co2Level, level -> level > 1400) highCO2 FROM DCDevices)
WHERE size(highCO2) > 0
ORDER BY deviceId, highCO2


-- COMMAND ----------

--Create a partitioned table
DROP TABLE IF EXISTS byDevice;
CREATE TABLE byDevice
USING
  parquet
PARTITIONED BY
  (p_deviceId) AS
SELECT
  deviceType,
  batteryLevel,
  co2Level,
  signal,
  temps,
  time,
  deviceId AS p_deviceId
FROM
  DCDevices;

-- COMMAND ----------

--Visualize average temperatures
SELECT 
  deviceType,
  REDUCE(temps, BIGINT(0), (t, a) -> t + a, a -> (a div size(temps))) AS avg_temps,
   to_date(time) AS dte
FROM 
  byDevice


-- COMMAND ----------

--Create a widget
CREATE WIDGET DROPDOWN selectedDeviceId DEFAULT "0" CHOICES
SELECT
  DISTINCT deviceId
FROM
  DCDevices
ORDER BY deviceId

-- COMMAND ----------

--Use the widget in a query
SELECT 
  getArgument("selectedDeviceId") AS selectedDeviceId,
  REDUCE(temps, BIGINT(0), (t, a) -> t + a, a -> (a div size(temps))) AS avg_temps,
   to_date(time) AS dte
FROM 
  byDevice

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delta Lab

-- COMMAND ----------

--Create a table from json files.

DROP TABLE IF EXISTS health_tracker_data_2020;              

CREATE TABLE health_tracker_data_2020                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json",
  inferSchema "true"
  );

-- COMMAND ----------

-- Preview the data
select * from health_tracker_data_2020 tablesample (5 rows)

-- COMMAND ----------

-- Count Records
select count(*) from health_tracker_data_2020

-- COMMAND ----------

-- Delta table that transforms and restructures your table
CREATE OR REPLACE TABLE health_tracker_silver 
USING DELTA
PARTITIONED BY (p_device_id)
LOCATION "dbfs:/health_tracker/silver" AS (
SELECT
  value.name,
  value.heartrate,
  CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
  CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
  value.device_id p_device_id
FROM
  health_tracker_data_2020
)

-- COMMAND ----------

--Register table to the metastore
DROP TABLE IF EXISTS health_tracker_silver;
CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/health_tracker/silver"

-- COMMAND ----------

--the number of records
SELECT p_device_id, COUNT(*) FROM health_tracker_silver GROUP BY p_device_id--Plot records
SELECT * FROM health_tracker_silver WHERE p_device_id NOT IN (3,4)

-- COMMAND ----------

--Plot records
SELECT * FROM health_tracker_silver WHERE p_device_id NOT IN (3,4)

-- COMMAND ----------

--Check for Broken Readings
CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
);

SELECT * FROM broken_readings;

-- COMMAND ----------

--Repair records
CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
);

SELECT COUNT(*) FROM updates;


-- COMMAND ----------

--Read late-arriving data
DROP TABLE IF EXISTS late_arriving_data;              

CREATE TABLE late_arriving_data                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw-late.json",
  inferSchema "true"
  );
  

SELECT COUNT(*) FROM late_arriving_data

-- COMMAND ----------

--Prepare inserts
CREATE OR REPLACE TEMPORARY VIEW inserts AS (
  SELECT
    value.name,
    value.heartrate,
    CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
    CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
    value.device_id p_device_id
  FROM
    late_arriving_data
);

-- COMMAND ----------

--Prepare upserts
CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    );

SELECT COUNT(*) FROM upserts;

-- COMMAND ----------

--Perform upserts

MERGE INTO health_tracker_silver                             
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id   
   
WHEN MATCHED THEN                                            
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                        
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id);

-- COMMAND ----------

-- Write to gold
--DROP TABLE IF EXISTS health_tracker_golds;              

CREATE TABLE health_tracker_gold1                        
USING DELTA
LOCATION "/health_tracker/gold1"
AS 
SELECT 
  AVG(heartrate) AS meanHeartrate,
  STD(heartrate) AS stdHeartrate,
  MAX(heartrate) AS maxHeartrate
FROM health_tracker_silver
GROUP BY p_device_id;

-- COMMAND ----------


