"""Optional distributed Spark workflow. Requires pyspark and a configured Spark runtime."""
import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TransitIQ route analytics").getOrCreate()
source = sys.argv[1]
output = sys.argv[2]
trips = spark.read.option("header", True).option("inferSchema", True).csv(source)
trips.groupBy("route_id").agg(F.sum("passenger_count").alias("passengers"), F.avg("passenger_count").alias("avg_occupancy")).orderBy(F.desc("passengers")).write.mode("overwrite").option("header", True).csv(output)
spark.stop()
