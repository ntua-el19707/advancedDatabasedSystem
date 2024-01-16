# Create a Spark session
import geopy.distance
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# Get the current timestamp
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
def get_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

def getMinimum(stations, lat, lon):
    dist = float('inf')  # Initialize dist as infinity
    minimum = None  # Initialize minimum as None
    for s in stations:
        d1 = get_distance(lat, lon, s.stations_Lat, s.stations_Lon)
        if d1 < dist:
            dist = d1
            minimum = s

    return minimum , dist 
queryName =  "query_b" + timestamp
spark = SparkSession.builder.appName("Query4bSQL").getOrCreate()

# Read CSV files into DataFrames
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

import time
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
        SELECT
            crimes.`Date OCC`,
            crimes.`LAT` AS crime_Lat,
            crimes.`LON` AS crime_Lon
        FROM combined_crime_data AS crimes
    )
    SELECT * FROM crimesInLa
""")
policeStations = spark.sql("""
    SELECT `X` as stations_Lon, `Y` as stations_Lat, `DIVISION` FROM policeStations
""")

# Collect data from the Spark DataFrame into a Python list
policeStationsTable = policeStations.collect()

# Apply the get_distance function to the collected data
result_pandas = result.toPandas()
result_pandas['distance'] = None
result_pandas['nearest_station_division'] = None

for index, row in result_pandas.iterrows():
    crime_lat = row['crime_Lat']
    crime_lon = row['crime_Lon']
    nearest_station  , dist = getMinimum(policeStationsTable, crime_lat, crime_lon)
    result_pandas.at[index, 'distance'] = dist 
    result_pandas.at[index, 'nearest_station_division'] = nearest_station.DIVISION

# Create a new Spark DataFrame from the modified pandas DataFrame
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
        nearest_station_division  AS division ,
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

