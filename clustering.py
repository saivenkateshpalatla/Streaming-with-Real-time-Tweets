import numpy as np
from pyspark.sql import SparkSession
from textblob import TextBlob
from config import *
import os
import time
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd


labels = np.array(["red", "blue", "green"])

def features(text):
    polarity = TextBlob(text).sentiment.polarity
    return [polarity, text.split()]

def feature_extraction(df):
      feature_udf = udf(features, ArrayType(FloatType(), containsNull=False))
      df = df.withColumn("features", feature_udf("value"))

      return df

spark = SparkSession.builder\
.master("local[*]")\
.appName("clustering")\
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7')\
.getOrCreate()



kmeans = KMeans(k=3, featuresCol='features')
kmeans.setSeed(1000)

runs = 0

plot_path = "plots/"

isExist = os.path.exists(plot_path)

if not isExist:
      os.makedirs(plot_path)

while True: 

      df = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_url) \
      .option("subscribe", topic_name) \
      .load().selectExpr("CAST(value AS STRING)") 

      start = time.time() 

      print("processing..")

      df = feature_extraction(df)

      fit = kmeans.fit(df)

      output = fit.transform(df)

      output = output.toPandas()

      df1 = np.array(output['prediction'].to_list()).astype(int).flatten()

      df2 = pd.DataFrame(output['features'].to_list(), columns=['x','y'])

      fig = df2.plot.scatter(x = 'x', y = 'y', c = labels[df1]).get_figure()
      fig.savefig('plots/'+str(runs)+'.png')

      runs = runs + 1

      end = time.time()
      duration = end - start

      if(duration <= 30):
            print("Wait..")
            time.sleep(30 - duration)

    


