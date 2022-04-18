from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    split,
    lower,
    regexp_extract,
    col,
    explode
)

import findspark
findspark.init()

# start a session in pyspark
spark = (SparkSession
         .builder
         .appName("Reading data")
         .getOrCreate())


bk = spark.read.text("./data/cervantes.txt")

print("schema")
print("-------------------------")
bk.printSchema()

print("types")
print("-------------------------")
print(bk.dtypes)

bk.show()
bk.show(10, truncate=50)
bk.select(bk.value)
bk.select(col("value"))

ln = bk.select(split(bk.value, " ").alias("line"))
ln.show(5)

print("explode: list in rows")
print("-------------------------")
wds = ln.select(explode(col("line")).alias("wd"))
wds.show(15)

print("lower case")
print("------------------------")
wds_lower = wds.select(lower(col("wd")).alias("wd_lower"))
wds_lower.show()

print("punctation")
print("------------------------")
wds_clean = wds_lower.select(
    regexp_extract(col("wd_lower"), "[a-z]+", 0).alias("wd")
)
wds_clean.show()

print("empty lines")
print("------------------------")
wds_clean_empty = wds_clean.filter(col("wd") != "")
wds_clean_empty.show()


print("count")
print("---------")
spark.sparkContext.setLogLevel("WARN")
result = (
    spark.read.text("./data/cervantes.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("wd"))
    .select(F.lower(F.col("wd")).alias("wd"))
    .select(F.regexp_extract(F.col("wd"), "[a-z']*", 0).alias("wd"))
    .where(F.col("wd") != "")
    .groupby(F.col("wd"))
    .count()
)
result.orderBy("count", ascending=False).show(20)

# export to a file. this isn't orderby, cos it's expensive
# -one per partition
# result.write.csv("./one_partition_cervantes.csv")
# -under a single partition
# result.coalesce(1).write.csv("single_partition_cervantes.csv")
