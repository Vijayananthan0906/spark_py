from pyspark.sql import SparkSession

mongo_uri = "mongodb://localhost:27017/local.startup_log"

spark = SparkSession.builder \
    .appName("ReadFromDB") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.format("mongo").load()

df.show(5)

