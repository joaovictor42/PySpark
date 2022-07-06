
# CREATE SPARK SESSION
from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame
from pyspark.sql.functions import *
import os, time, json

spark = (
    SparkSession.builder
    .getOrCreate()
)

print('===== This is a test =====')
print(spark)

spark.stop()
