import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import findspark
findspark.init()

# start a session in pyspark
spark = (SparkSession
         .builder
         .appName("Tabular data")
         .getOrCreate())

# no show log INFO
spark.sparkContext.setLogLevel("WARN")
print("list of list")
print("------------------")
my_list = [
    ["house", 9, 3.74],
    ["car", 6, 1.34],
    ["table", 7, 1.29],
    ["chair", 2, 12.99],
]

df_list = spark.createDataFrame(
    my_list, ["Item", "Units", "Price"])

df_list.printSchema()
df_list.show()

# Display 2 rows and full column contents
df_list.show(2, truncate=False)

# Display df_list rows & columns vertically
df_list.show(n=3, truncate=20, vertical=True)

Directory = "/media/javier/LINUX"
data = spark.read.csv(
    os.path.join(Directory, "csv_examples"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",)

data.printSchema()

# ways of getting columns
data.select("LogServiceID", "ProgramClassID", "LogEntryDate").show(10, False)

data.select(*["LogServiceID", "ProgramClassID",
            "LogEntryDate"]).show(10, False)

data.select(
    F.col("LogServiceID"), F.col("ProgramClassID"), F.col("LogEntryDate")
).show(10, False)

data.select(
    *[F.col("LogServiceID"), F.col("ProgramClassID"), F.col("LogEntryDate")]
).show(10, False)

# drop a column
data.drop("LogServiceID").show(2, truncate=10, vertical=False)
# test if we get rid of the columns
print("LogServiceID" in data.columns)  # => true

data.select(F.col("Duration")).show(5)

# display in columns
data.select(
    F.col("Duration"),
    F.col("Duration").substr(1, 2).cast("int").alias("hours"),
    F.col("Duration").substr(4, 2).cast("int").alias("minutes"),
    F.col("Duration").substr(7, 2).cast("int").alias("seconds"),
    F.col("Duration").substr(10, 2).cast("int").alias("mls"),
).distinct().show(
    5)

# create a new column
data = data.withColumn(
    "Duration_in_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)
data.printSchema()

# rename column
data = data.withColumnRenamed("Duration_in_seconds", "duration_in_seconds")
data.printSchema()

# toDF(*cols) => convert to a new dataframe
data = data.toDF(*[x.lower() for x in data.columns])
data.printSchema()

# describe()
for i in data.columns:
    data.describe(i).show()

# summary() => 25%, 50%, 75%
for i in data.columns:
    data.select(i).summary().show()
