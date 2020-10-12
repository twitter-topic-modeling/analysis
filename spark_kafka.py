from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *

# def fun(avg_senti_val):
# 	try:
# 		if avg_senti_val < 0: return 'NEGATIVE'
# 		elif avg_senti_val == 0: return 'NEUTRAL'
# 		else: return 'POSITIVE'
# 	except TypeError:
# 		return 'NEUTRAL'

if __name__ == "__main__":
    schema = StructType(
        [
            StructField("title", StringType(), True),
            StructField("summary", StringType(), True)
        ]
    )
    spark = SparkSession.builder.appName("Newsfeed").getOrCreate()
    kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "kafka-test-9").load()
    kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")
    newsfeed_table = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")
    text_table = newsfeed_table.select('title')
#     new_df = text_table.withColumn("title")
    query = text_table.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
