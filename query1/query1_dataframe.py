from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col,year,month,row_number
from pyspark.sql.functions import to_date,to_timestamp
from pyspark.sql.window import Window



# Create a Spark session
spark = SparkSession \
    .builder \
    .appName("Query1_Dataframe") \
    .getOrCreate()

df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)

import time
start_time = time.time()


combined_df = df1.union(df2).union(df3).union(df4).dropDuplicates()    #One single Dataframe

combined_df = combined_df.withColumn("Date Rptd", to_timestamp(combined_df["Date Rptd"], "MM/dd/yyyy hh:mm:ss a"))
combined_df = combined_df.withColumn("DATE OCC", to_timestamp(combined_df["Date OCC"], "MM/dd/yyyy hh:mm:ss a"))
combined_df = combined_df.withColumn("Vict Age", col("Vict Age").cast("int"))
combined_df = combined_df.withColumn("LAT", col("LAT").cast("double"))
combined_df = combined_df.withColumn("LON", col("LON").cast("double"))

#combined_df = combined_df.withColumn("Date Rptd", to_date(combined_df["Date Rptd"], "MM/dd/yyyy"))
#combined_df = combined_df.withColumn("DATE OCC", to_date(combined_df["DATE OCC"], "MM/dd/yyyy"))

selected_columns = ["DATE OCC"]
df = combined_df.select(*selected_columns)
df = df.withColumn("Year", year(df["DATE OCC"]))
df = df.withColumn("Month", month(df["DATE OCC"]))

result_df = df.groupBy("Year", "Month").count()

# Define a window specification partitioned by "Year" and ordered by "Value" in descending order
window_spec = Window.partitionBy("Year").orderBy(col("count").desc())

# Add a "rank" column to the DataFrame based on the window specification
df_ranked = result_df.withColumn("rank", row_number().over(window_spec))

# Keep only the top 3 rows for each year
df_top3 = df_ranked.filter(col("rank") <= 3)

final_df = df_top3.select(
    col("year").alias("Year"),
    col("month").alias("Month"),
    col("count").alias("Total Crimes"),
    col("rank").alias("#")
)

filtered_final_df = final_df.filter(col("year").isNotNull())

filtered_final_df.printSchema()
# Show the updated DataFrame
filtered_final_df.show(50)

# Stop the Spark session
spark.stop()



print(f"Execution time: {time.time() - start_time}")

