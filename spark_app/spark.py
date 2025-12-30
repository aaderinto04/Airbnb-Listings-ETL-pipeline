from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName("Airbnb Spark application") \
    .getOrCreate()

listings = spark.read.csv("s3://airbnb-proj-raw-data/listings.csv.gz", 
    header=True,
    inferSchema=True,
    sep=",", 
    quote='"',
    escape='"', 
    multiLine=True,
    mode="PERMISSIVE" 
)

listings_clean = listings.withColumn(
    "price_num",
    regexp_replace("price", "[$,]", "").cast("float")
)

host_stats = listings_clean.groupBy("host_id").agg(
    F.count("*").alias("host_total_listings"),
    F.avg("price_num").alias("host_avg_price"),
    F.avg("number_of_reviews").alias("host_avg_reviews"),
    F.avg("minimum_nights").alias("host_avg_min_nights")
)

final_df = listings_clean.join(
    host_stats,
    on="host_id",
    how="left"
)

final_df.write.mode("overwrite").parquet(
    "s3://processed-airbnb-data/output/"
)

spark.stop()