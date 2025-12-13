from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Airbnb Spark application") \
    .getOrCreate()

listings = spark.read.csv("/Users/abdullahaderinto/Documents/Airbnb-Listings-ETL-pipeline/data/listings.csv.gz", 
    header=True,
    inferSchema=True,
    sep=",", 
    quote='"',
    escape='"', 
    multiLine=True,
    mode="PERMISSIVE" 
)

listings.printSchema()

