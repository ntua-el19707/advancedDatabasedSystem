# Create a Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query3_SQL").getOrCreate()

# Read CSV files into DataFrames
df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df5 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df6 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/LA_income_2015.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/revgecoding.csv", header=True)

# Register the DataFrames as temporary SQL views
df1.createOrReplaceTempView("crime_data_2010_2019a")
df5.createOrReplaceTempView("crime_data_2010_2019b")
df6.createOrReplaceTempView("crime_data_2010_2019c")
df2.createOrReplaceTempView("crime_data_2020_present")
df3.createOrReplaceTempView("LA_income_2015")
df4.createOrReplaceTempView("revgecoding")

import time
start_time = time.time()

# Combine data from both tables into a single table
result = spark.sql("""
    WITH combined_crime_data AS (
        SELECT * FROM crime_data_2010_2019a
        UNION
        SELECT * FROM crime_data_2010_2019b
        UNION
        SELECT * FROM crime_data_2010_2019c
        UNION
        SELECT * FROM crime_data_2020_present
    ),
    crime_Data_Per_Zip AS (
        SELECT SUBSTRING(rc.ZIPcode, 1, 5) AS zip_code,
               count(*) AS total_vict_descent_count, cd.`Vict Descent`
        FROM combined_crime_data as cd
        JOIN revgecoding as rc
        ON rc.LAT = cd.LAT AND rc.LON = cd.LON
        WHERE cd.`Vict Descent` IS NOT NULL AND rc.ZIPcode IS NOT NULL
        GROUP BY zip_code ,  cd.`Vict Descent`
    ) ,
 sorted_LA_Income AS (
        SELECT *,
                CAST( SUBSTRING( regexp_replace(
                        `Estimated Median Income`,
                       ',', '' ) , 2 ) AS Float )  AS estimated_median_income_float
        FROM LA_income_2015 WHERE `Community`  LIKE '%Los Angeles (%'
        ORDER BY estimated_median_income_float DESC
)  ,  TOPANDLAST3 AS (
    SELECT * FROM (
        SELECT * FROM sorted_LA_Income ORDER BY estimated_median_income_float DESC LIMIT 3
    )
    UNION ALL
    SELECT * FROM (
        SELECT * FROM sorted_LA_Income ORDER BY estimated_median_income_float ASC LIMIT 3
    )
    ),
    finalTable AS (
    SELECT
        t.`Zip Code`,
        cz.total_vict_descent_count AS `#`,
        cz.`Vict Descent`,
        CASE
            WHEN cz.`Vict Descent` = 'A' THEN 'Other Asian'
            WHEN cz.`Vict Descent` = 'B' THEN 'Black'
            WHEN cz.`Vict Descent` = 'C' THEN 'Chinese'
            WHEN cz.`Vict Descent` = 'D' THEN 'Cambodian'
            WHEN cz.`Vict Descent` = 'F' THEN 'Filipino'
            WHEN cz.`Vict Descent` = 'G' THEN 'Guamanian'
            WHEN cz.`Vict Descent` = 'H' THEN 'Hispanic/Latin/Mexican'
            WHEN cz.`Vict Descent` = 'I' THEN 'American Indian/Alaskan Native'
            WHEN cz.`Vict Descent` = 'J' THEN 'Japanese'
            WHEN cz.`Vict Descent` = 'K' THEN 'Korean'
            WHEN cz.`Vict Descent` = 'L' THEN 'Laotian'
            WHEN cz.`Vict Descent` = 'O' THEN 'Other'
            WHEN cz.`Vict Descent` = 'P' THEN 'Pacific Islander'
            WHEN cz.`Vict Descent` = 'S' THEN 'Samoan'
            WHEN cz.`Vict Descent` = 'U' THEN 'Hawaiian'
            WHEN cz.`Vict Descent` = 'V' THEN 'Vietnamese'
            WHEN cz.`Vict Descent` = 'W' THEN 'White'
            WHEN cz.`Vict Descent` = 'X' THEN 'Unknown'
            WHEN cz.`Vict Descent` = 'Z' THEN 'Asian Indian'
            ELSE cz.`Vict Descent`
        END AS victim_category
    FROM
        TOPANDLAST3 AS t
        JOIN crime_Data_Per_Zip AS cz ON cz.zip_code = t.`Zip Code`
    ORDER BY
        t.`Zip Code`,
        cz.total_vict_descent_count DESC
)
select * from  finalTable
    """)
result.show(500)
#) ,  crimesPerMoneyDistrict  AS (
 #   select   cz.* , t3.*  from  crime_Data_Per_Zip as  cz join   TOPANDLAST3 as t3  on cz.zip_code = t3.ZIPcode
#)
print(f"Execution time: {time.time() - start_time}")
