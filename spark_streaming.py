from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *

KAFKA_TOPIC = 'rss-test-1'

def fun(avg_senti_val):
	try:
		if avg_senti_val < 0: return 'NEGATIVE'
		elif avg_senti_val == 0: return 'NEUTRAL'
		else: return 'POSITIVE'
	except TypeError:
		return 'NEUTRAL'

if __name__ == "__main__":

    schema = StructType([
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("source", StringType(), True),
        StructField("pubDate", DateType(), True)
    ])

    spark = SparkSession.builder.appName("RSS-Streaming").getOrCreate()

    kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", KAFKA_TOPIC).load()

    kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")

    df = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

    to_predict_table = df.select('title', 'summary')

	to_predict_table = table_predict_table.withcomlumn("")

    #to_predict_table.show()
#     sum_val_table = tweets_table.select(avg('senti_val').alias('avg_senti_val'))

    # udf = USER DEFINED FUNCTION
#     udf_avg_to_status = udf(fun, StringType())

    # avarage of senti_val column to status column
#     new_df = sum_val_table.withColumn("status", udf_avg_to_status("avg_senti_val"))

    query = to_predict_table.writeStream.outputMode("append").format("console").start()

    query.awaitTermination()
