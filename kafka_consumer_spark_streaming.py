import os

from pyspark.sql import SparkSession
import dotenv
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

dotenv.load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# spark = (
#     SparkSession.builder.master("local:7077").getOrCreate()
#     # .config(
#     #     "spark.jars.packages",
#     #     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
#     #     "org.postgresql:postgresql:42.2.5",
#     # )
#     # .getOrCreate()
# )

spark = (
    SparkSession.builder.appName("Kafka Streaming-1")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
        # "org.postgresql:postgresql:42.2.5"
    )
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.2.5",
    )
    .master("spark://localhost:7077")
    .getOrCreate()
)
schema = (
    StructType()
    .add("timestamp", StringType())
    .add("symbol", StringType())
    .add("bid_price", DoubleType())
    .add("ask_price", DoubleType())
    .add("trade_price", DoubleType())
    .add("trade_volume", DoubleType())
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)
parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)
console_query = parsed_df.writeStream.outputMode("append").format("console").start()

df_parsed = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

query = (
    df_parsed.writeStream.foreachBatch(
        lambda batch_df, batch_id: batch_df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "kafka.kafka.stock_price")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()
# console_query.awaitTermination()