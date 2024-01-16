# Create a Spark session
import geopy.distance
import time
from pyspark.sql import SparkSession

def get_distance(lat1, long1, lat2, long2):
    lat1, long1, lat2, long2 = map(float, [lat1, long1, lat2, long2])
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km

spark = SparkSession.builder.appName("Query4a_SQL").getOrCreate()

spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)
# Read CSV files into DataFrames
df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df5 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/LAPD_Police_Stations.csv", header=True)

# Register the DataFrames as temporary SQL views
df1.createOrReplaceTempView("crime_data_2010_2019a")
df4.createOrReplaceTempView("crime_data_2010_2019b")
df5.createOrReplaceTempView("crime_data_2010_2019c")
df2.createOrReplaceTempView("crime_data_2020_present")
df3.createOrReplaceTempView("policeStations")

start_time = time.time()

# Combine data from both tables into a single table
result = spark.sql("""
    WITH combined_crime_data AS (
         SELECT *
        FROM crime_data_2010_2019a
        WHERE CAST(`Weapon Used Cd` AS INT) >= 100 AND CAST(`Weapon Used Cd` AS INT) < 200
            AND `LAT` != '0' AND `LON` != '0'
        UNION
        SELECT *
        FROM crime_data_2010_2019b
        WHERE CAST(`Weapon Used Cd` AS INT) >= 100 AND CAST(`Weapon Used Cd` AS INT) < 200
            AND `LAT` != '0' AND `LON` != '0'

        UNION
        SELECT *
        FROM crime_data_2010_2019c
        WHERE CAST(`Weapon Used Cd` AS INT) >= 100 AND CAST(`Weapon Used Cd` AS INT) < 200
            AND `LAT` != '0' AND `LON` != '0'
        UNION
        SELECT *
        FROM crime_data_2020_present
        WHERE CAST(`Weapon Used Cd` AS INT) >= 100 AND CAST(`Weapon Used Cd` AS INT) < 200
            AND `LAT` != '0' AND `LON` != '0'
    ), crimesInLa AS (
        SELECTS /*+ MERGEJOIN(crimes)*/  
            crimes.`Date OCC`,
            crimes.`LAT` AS crime_Lat,
            crimes.`LON` AS crime_Lon,
            stations.`X` AS stations_Lon,
            stations.`Y` AS stations_Lat,
            stations.`DIVISION`
        FROM combined_crime_data AS crimes
        JOIN policeStations AS stations
        ON stations.`FID` = crimes.`AREA `
    )
    SELECT * FROM crimesInLa
""")
table = result.collect()
result_pandas = result.toPandas()

# Apply the get_distance function to the collected data
result_pandas['distance'] = result_pandas.apply(lambda row: get_distance(row['crime_Lat'], row['crime_Lon'], row['stations_Lat'], row['stations_Lon']), axis=1)
df4 = spark.createDataFrame(result_pandas)
df4.createOrReplaceTempView("data_used_geopy")

result2 =  spark.sql("""
with queryA AS (
    SELECT  
        YEAR(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS year ,
        AVG(distance) AS average_distance ,
        COUNT(*) as count  
    FROM data_used_geopy 
    GROUP BY year
    ORDER BY year
    ) 
select *  from queryA
""")

result2.show(50)
result3 =  spark.sql("""
    WITH  queryB AS (
    SELECT  
        DIVISION  AS division ,
        AVG(distance) AS average_distance ,
        COUNT(*) as count  
    FROM data_used_geopy 
    GROUP BY division
    ORDER BY count desc 
    )
    SELECT *  FROM queryB
"""
)
result3.show(50)
print(f"Execution time: {time.time() - start_time}")
