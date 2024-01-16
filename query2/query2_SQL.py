# Create a Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query2_SQL").getOrCreate()

df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)

#create  tables
df1.createOrReplaceTempView("crime_data_2010_2019a")
df3.createOrReplaceTempView("crime_data_2010_2019b")
df4.createOrReplaceTempView("crime_data_2010_2019c")
df2.createOrReplaceTempView("crime_data_2020_present")

import time
start_time = time.time()

spark.sql("""
    WITH combined_crime_data AS (
        SELECT * FROM crime_data_2010_2019a
        WHERE `Premis Desc` = 'STREET'
        UNION
        SELECT * FROM crime_data_2010_2019b
        WHERE `Premis Desc` = 'STREET'
        UNION
        SELECT * FROM crime_data_2010_2019c
        WHERE `Premis Desc` = 'STREET'
        UNION
        SELECT * FROM crime_data_2020_present
        WHERE `Premis Desc` = 'STREET'
    ),
    street_crimes AS (
        SELECT *,
            CASE
                WHEN CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) >= 5 AND CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) <= 11 THEN 'Πρωί'
                WHEN CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) >= 12 AND CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) <= 16 THEN 'Απόγευμα'
                WHEN CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) >= 17 AND CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) <= 20 THEN 'Βράδυ'
                WHEN CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) >= 21 OR CAST(SUBSTRING(`TIME OCC`, 1, 2) AS INT) <= 4 THEN 'Νύχτα'
                ELSE 'Unknown'
            END AS time_periods
        FROM combined_crime_data
    ),countPeriods AS (
        SELECT time_periods, COUNT(*) AS countPeriods
        FROM street_crimes
        GROUP BY time_periods
    )
    SELECT *
    FROM countPeriods
    ORDER BY countPeriods DESC
""").show(50)
print(f"Execution time: {time.time() - start_time}")