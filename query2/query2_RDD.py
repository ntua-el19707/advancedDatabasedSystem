from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("RDD Execution").getOrCreate()
#Cast   The data  from  Dataframes  to RDD
df1 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part1.csv", header=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part2.csv", header=True)
df4 = spark.read.csv("hdfs://okeanos-master:54310/crimesCsv_part3.csv", header=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/Crime_Data_from_2020_to_Present.csv", header=True)

import time
start_time = time.time()

rdd1 = df1.rdd
rdd2 = df2.rdd
rdd3 = df3.rdd
rdd4 = df4.rdd
#discard  The header  name of the fields  
header1 = rdd1.first()
parsed_rdd1 = rdd1.filter(lambda line: line != header1)#.map(lambda line: line.split(','))

header2 = rdd2.first()
parsed_rdd2 = rdd2.filter(lambda line: line != header2)#.map(lambda line: line.split(","))
header3 = rdd3.first()
parsed_rdd3 = rdd3.filter(lambda line: line != header3)#.map(lambda line: line.split(','))

header4 = rdd4.first()
parsed_rdd4 = rdd4.filter(lambda line: line != header4)#.map(lambda line: line.split(","))

# Combine all data from all the  RDDs into a single RDD
combined_rdd = parsed_rdd1.union(parsed_rdd2.union(parsed_rdd3.union(parsed_rdd4)))

# Filter for STREET crimes
street_crimes_rdd = combined_rdd.filter(lambda row:  row["Premis Desc"] == "STREET").distinct()

# Define a function for time period
def get_time_period(row):
    hour = int(row[3][:2])
    if 5 <= hour <= 11:
        time_period = "Πρωί"
    elif 12 <= hour <= 16:
        time_period = "Απόγευμα"
    elif 17 <= hour <= 20:
        time_period = "Βράδυ"
    elif 21 <= hour or hour <= 4:
        time_period = "Νύχτα"
    else:
        time_period = "Unknown"
    return tuple(row) + (time_period,)

# Apply the time period function
result_rdd = street_crimes_rdd.map(get_time_period)

# Order by the "TIME OCC" field
ordered_rdd = result_rdd.sortBy(lambda row: int(row[3]) , ascending=False)

def get_time_period_count(row):
    time_period = row[-1]  # Get the last element
    return time_period, 1  # Return a tuple with the time period and count 1

# Apply the function and reduce by key to count occurrences
counted_rdd = result_rdd.map(get_time_period_count).reduceByKey(lambda x, y: x + y)

sorted_counted_rdd = counted_rdd.sortBy(lambda x: x[1],ascending=False)

# Collect the result
result = sorted_counted_rdd.collect()

# Print the result
for time_period, count in result:
    print(f"{time_period}: {count}")


print(f"Execution time: {time.time() - start_time}")