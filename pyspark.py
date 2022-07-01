
# CREATE SPARK SESSION
from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame
from pyspark.sql.functions import *
import os, time, json, boto3

# DEFINE ANOTHER SESSIONS
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

spark = (
    SparkSession.builder.appName(
    'Pyspark - Insert Delete Tables')
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # Delta config
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") # Delta config
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # s3a config
    .config("spark.hadoop.fs.s3a.fast.upload", "true") # s3a config
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) # permissions
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) # permissions
    .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") 
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

print('===== This is a test =====')
print(spark)

spark.stop()
