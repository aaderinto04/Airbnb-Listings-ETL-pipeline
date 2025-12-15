from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

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
.show(5, truncate=False)




