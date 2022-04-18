import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Analyzing data.")
         .getOrCreate())

results = (spark.read.csv("./data/store.csv", header=True)
           .withColumn("new_column", F.when(F.col("quantity") > 10, 10).otherwise(0))
           .where("quantity > 8")
           .groupby("new_column")
           .count()
           .write.csv("updated.csv", mode="overwrite")
           )
