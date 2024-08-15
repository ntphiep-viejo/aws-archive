from pyspark.sql import SparkSession
from pyspark import SparkConf

import sys, os

conf = SparkConf() \
        .setAppName("1g_process") \
        .set("spark.executor.cores", "2") \
        .set("spark.executor.memory", "4g") \
        # .set("spark.executor.memory", "4g") \
        # .set("spark.driver.host", "


spark = SparkSession.builder.config(conf=conf).getOrCreate()

# file_path = "file:///data/" if os.name != 'nt' else r""

df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("samplingRatio", .1) \
        .load("songs_with_attributes_and_lyrics.csv")
        

df.printSchema()


print("--------------------------")
print(df.rdd.getNumPartitions())
print(df.count())
print(df.show(10))

