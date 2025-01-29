from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FileReader") \
    .getOrCreate()
    
df_csv = spark.read.csv("src/main/resources/simple-zipcodes.csv", header=True, inferSchema=True)

df_json = spark.read.json("src/main/resources/zipcodes.json")

df_text = spark.read.text("src/main/resources/test.txt")

df_parquet = spark.read.parquet("src/main/resources/loan-risks.snappy.parquet")

print("CSV Dataframe")
df_csv.show(5)
print("CSV Dataframe Schema")
df_csv.printSchema()

print("JSON Dataframe")
df_json.show(5)
print("JSON Dataframe Schema")
df_json.printSchema()

print("Text Dataframe")
df_text.show(5)
print("Text Dataframe Schema")
df_text.printSchema()

print("Parquet Dataframe")
df_parquet.show(5) 
print("Parquet Dataframe Schema")
df_parquet.printSchema()
