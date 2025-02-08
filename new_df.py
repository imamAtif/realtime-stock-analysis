from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("spark://localhost:7077") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "name"])
df.show()
