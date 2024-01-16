from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col,year,month,row_number,when
from pyspark.sql.functions import to_date,to_timestamp
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession \
    .builder \
    .appName("Query2_DataFrame") \
    .getOrCreate()

df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)


import time
start_time = time.time()

combined_df = df1.union(df2).union(df3).union(df4).dropDuplicates()    #One single Dataframe

combined_df = combined_df.withColumn("Date Rptd", to_timestamp(combined_df["Date Rptd"], "MM/dd/yyyy hh:mm:ss a"))
combined_df = combined_df.withColumn("Date OCC", to_timestamp(combined_df["Date OCC"], "MM/dd/yyyy hh:mm:ss a"))
combined_df = combined_df.withColumn("Vict Age", col("Vict Age").cast("int"))
combined_df = combined_df.withColumn("LAT", col("LAT").cast("double"))
combined_df = combined_df.withColumn("LON", col("LON").cast("double"))


Street_df = combined_df.filter(col("Premis Desc") == "STREET")

# Define conditions for categorizing salary

condition1 = ((col("TIME OCC") >= "0500") & (col("TIME OCC") <= "1159"))
condition2 =((col("TIME OCC") >= "1200") & (col("TIME OCC") <= "1659"))
condition3 =((col("TIME OCC") >= "1700") & (col("TIME OCC") <= "2059"))
condition4 =((col("TIME OCC") >= "2100") | (col("TIME OCC") <= "0459"))


# Create a new column "salary_category" based on the conditions
categorized_Street_df = Street_df.withColumn("TIME_OF_THE_DAY", 
                                                          when(condition1,"PRWI")
                                                          .when(condition2, "APOGEVMA")
                                                          .when(condition3, "VRADY")
                                                          .when(condition4, "NIXTA")
                                                          .otherwise("UNKNOWN"))


selected_columns = ["TIME_OF_THE_DAY"]
df = categorized_Street_df.select(*selected_columns)

result_df = df.groupBy("TIME_OF_THE_DAY").count()
sorted_result_df = result_df.orderBy(col("count").desc())

sorted_result_df.printSchema()
# Show the updated DataFrame
sorted_result_df.show(10)
#UNKNOWN_df = categorized_Street_df.filter(col("TIME_OF_THE_DAY") == "UNKNOWN")

#UNKNOWN_df.printSchema()
# Show the updated DataFrame
#UNKNOWN_df.show(50)

# Stop the Spark session
spark.stop()


print(f"Execution time: {time.time() - start_time}")

