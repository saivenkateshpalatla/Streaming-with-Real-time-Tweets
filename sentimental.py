from textblob import TextBlob
from config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def polarity(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity < 0:
        sentiment = "negative"
    elif polarity == 0:
        sentiment = "neutral"
    else:
        sentiment = "positive"
    return sentiment


def text_classification(df):
    polarity_udf = udf(polarity, StringType())
    df = df.withColumn("polarity", polarity_udf("value"))
    return df

spark = SparkSession.builder\
.master("local[*]")\
.appName("Sentiment")\
.config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:8.1.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7')\
.getOrCreate()

df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", kafka_url) \
.option("subscribe", topic_name) \
.load().selectExpr("CAST(value AS STRING)") \


text = text_classification(df)

query = text.writeStream \
.outputMode("append") \
.queryName("es") \
.format("org.elasticsearch.spark.sql") \
.option("es.nodes", host) \
.option("es.port", port)\
.option("es.nodes.wan.only", "true") \
.option("es.net.ssl", "false") \
.option("checkpointLocation", "checkpoint") \
.start("new_tweets")

query.awaitTermination()

