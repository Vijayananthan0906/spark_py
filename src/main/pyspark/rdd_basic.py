from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDDFirstExample") \
    .getOrCreate()
    
data = [1, 2, 3, 4, 5]

rdd = spark.sparkContext.parallelize(data)

print("RDD Elements:", rdd.collect())

spark.stop()