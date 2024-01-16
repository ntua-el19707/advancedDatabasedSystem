from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date,to_timestamp

# Create a Spark session
spark = SparkSession \
    .builder \
    .appName("Combine_CSV_Files") \
    .getOrCreate()

#crimes_schema = StructType([
#    StructField("DR_NO", StringType()),
#    StructField("Date Rptd", TimestampType()),
#    StructField("DATE OCC", DateType()),
#    StructField("Vict Age", IntegerType()),
#    StructField("LAT", DoubleType()),
#    StructField("LON", DoubleType()),

#])

#df1 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2010_to_2019.csv", header=True, schema=crimes_schema)
#df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True, schema=crimes_schema)
df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)
combined_df = df1.union(df2).union(df3).union(df4).dropDuplicates()    #One single Dataframe

combined_df = combined_df.withColumn("Date Rptd", to_timestamp(combined_df["Date Rptd"], "MM/dd/yyyy hh:mm:ss a"))
combined_df = combined_df.withColumn("Date OCC", to_timestamp(combined_df["Date OCC"], "MM/dd/yyyy hh:mm:ss a"))
combined_df = combined_df.withColumn("Vict Age", col("Vict Age").cast("int"))
combined_df = combined_df.withColumn("LAT", col("LAT").cast("double"))
combined_df = combined_df.withColumn("LON", col("LON").cast("double"))

#combined_df = combined_df.withColumn("Date Rptd", to_date(combined_df["Date Rptd"], "MM/dd/yyyy"))
#combined_df = combined_df.withColumn("DATE OCC", to_date(combined_df["DATE OCC"], "MM/dd/yyyy"))

# Show the Number of Rows
print("Number of Rows: ", combined_df.count())

#Show the Type of each field
combined_df.printSchema()

combined_df.show(10)  # Display the first 10 rows


# Stop the Spark session
spark.stop()
