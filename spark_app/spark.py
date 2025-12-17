from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import pyspark.sql.functions as F


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

#listings.printSchema()

#Add a column to normalize prices into float type removing $ and ,
norm_price = listings\
.withColumn('price_num', regexp_replace('price', '[$,]', '').cast('float'))
norm_price.schema['price_num']
norm_price.select('price_num')\
.show(0, truncate=False)

#Calculates the avg price of all listings per each host
avg_price_per_host = norm_price.groupBy('host_id') \
.agg(F.avg('price_num')) \
.orderBy(F.avg('price_num').desc()) \
.show(0, truncate=False)

#Total amount of listings that each host has
total_listings = listings.groupBy('host_id') \
.agg(F.count('listing_url')) \
.orderBy(F.count('listing_url').desc()) \
.show(0, truncate=False)

#Avg price of all listings per host
avg_price_per_host = norm_price.groupBy('host_id') \
.agg(F.avg('price_num')) \
.orderBy(F.avg('price_num').desc()) \
.show(0, truncate=False)

# Average number of reviews per host
avg_num_of_reviews = listings.groupBy('host_id') \
.agg(F.avg('number_of_reviews')) \
.orderBy(F.avg('number_of_reviews').desc()) \
.show(0, truncate=False)

# Average min of nights per host
avg_min_of_nights = listings.groupBy('host_id') \
.agg(F.avg('minimum_nights')) \
.orderBy(F.avg('number_of_reviews').desc()) \
.show(0, truncate=False)

#Number of reviews
num_of_reviews = listings.agg(F.count('number_of_reviews')) \
.show()

#Max price
max_price = norm_price.agg(F.max('price_num')) \
.show()

#Min price
min_price = norm_price.agg(F.min('price_num')) \
.show()



