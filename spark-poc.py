
from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .getOrCreate()
)

print('===== This is a test =====')

print(os.listdir('/mnt/spark/work'))

spark.stop()
