from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("PySparkFirstExample") \
    .getOrCreate()

data = [("Alice", 29), ("Bob", 31), ("Cathy", 25)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()
spark.stop()
