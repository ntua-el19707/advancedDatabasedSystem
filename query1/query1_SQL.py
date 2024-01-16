from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Query1_SQL").getOrCreate()
df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)


# Register the DataFrames as temporary SQL views
df1.createOrReplaceTempView("crime_data_2010_2019a")
df3.createOrReplaceTempView("crime_data_2010_2019b")
df4.createOrReplaceTempView("crime_data_2010_2019c")
df2.createOrReplaceTempView("crime_data_2020_present")


import time
start_time = time.time()

# Step 1: Combine data from both tables into a single table
spark.sql("""
    WITH combined_crime_data AS (
        SELECT * FROM crime_data_2010_2019a
        UNION
        SELECT * FROM crime_data_2010_2019b
        UNION
        SELECT * FROM crime_data_2010_2019c
        UNION
        SELECT * FROM crime_data_2020_present
    )
    , converted_crime_data AS (
        SELECT *,
               TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a') AS converted_date_occ
        FROM combined_crime_data
    )
    , extracted_dates AS (
        SELECT converted_date_occ,
               YEAR(converted_date_occ) AS Year,
               MONTH(converted_date_occ) AS Month
        FROM converted_crime_data
    )
    , ranked_crimes AS (
        SELECT Year,
               Month,
               COUNT(*) AS crime_count,
               ROW_NUMBER() OVER (PARTITION BY Year ORDER BY COUNT(*) DESC) AS crime_rank
        FROM extracted_dates
        GROUP BY Year, Month
    )
    , top3_crimes AS (
        SELECT Year,
               Month,
               crime_count AS Total_Crimes,
               crime_rank AS Rank
        FROM ranked_crimes
        WHERE crime_rank <= 3
    )
    SELECT *
    FROM top3_crimes
    WHERE Year IS NOT NULL
""").show(50)

# Stop the Spark session
spark.stop()

print(f"Execution time: {time.time() - start_time}")
