from pre_processing import pre_process

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel

lrModel = None
KAFKA_INPUT_TOPIC = 'rss-raw-1'
KAFKA_OUTPUT_TOPIC = 'rss-analysis-1'
vals = ['ในประเทศ', 'การเมือง', 'กีฬา', 'อาชญากรรม', 'ต่างประเทศ', 'เศรษฐกิจ', 'บันเทิง', 'ไลฟ์สไตล์', 'สิ่งแวดล้อม', 'เทคโนโลยี']

def convert_label(prediction):
    return vals[int(prediction)]

if __name__ == "__main__":

    schema = StructType([
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("source", StringType(), True),
        StructField("pubDate", StringType(), True)
    ])

    spark = SparkSession.builder.appName("RSS-Streaming").getOrCreate()
    lrModel = LogisticRegressionModel.load('/project/lrmodel')

    kafka_df = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "localhost:9092")\
            .option("subscribe", KAFKA_INPUT_TOPIC).option("failOnDataLoss","false").load()
    kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")

    udf_pre_process = udf(pre_process, ArrayType(StringType()))
    udf_convert_label = udf(convert_label, StringType())

    df = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")
    new_df = df.withColumn("tokens", udf_pre_process("title", "summary")).na.drop()
    pipeline = PipelineModel.load('/project/pipeline')
    new_df = pipeline.transform(new_df)
    new_df = lrModel.transform(new_df)

    # convert label (index) to label (string)
    new_df = new_df.withColumn('category', udf_convert_label('prediction'))
    new_df = new_df.select('url', 'title', 'summary', 'source', 'pubDate', 'category')
    new_df = new_df.select(to_json(struct('url', 'title', 'summary', 'source', 'pubDate', 'category')).alias('value'))
    query = new_df.writeStream\
        .outputMode("append")\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint")\
        .option("truncate","false")\
		.option("failOnDataLoss","false")\
        .option("topic", KAFKA_OUTPUT_TOPIC).start()

    query.awaitTermination()


