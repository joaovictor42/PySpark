
from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .getOrCreate()
)

print('===== TEST BRANCH =====')

print(os.listdir('/mnt/spark/work'))

spark.stop()
