import sys

from pprint import pprint

# each JSON is small, there's no need in iterative processing
import orjson
import json
import sys
import os
import xml
import time

import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType

import copy
import uuid

citation_dir        = "s3://recsys-bucket-1/data_lake/arnet/tables/citation/merge-0"
file_prefix = "s3://recsys-bucket-1/data_lake/arnet/tables/citation/parts/"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

sources = []
# 119 files recorded
for i in range(0, 438):
    sources.append(file_prefix + "part-" + str(i))

schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('cite', StringType(), False),
    StructField('year', FloatType(), False),
])

file_df = spark.read.schema(schema).parquet(*sources)
file_df.createOrReplaceTempView("file_df")

spark.sql("""
    select count(distinct author_id)
    from file_df
""").show()

deptSchema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('cite', StringType(), False),
    StructField('year', FloatType(), False),
])

df = spark.read.schema(deptSchema).parquet(citation_dir)
df.createOrReplaceTempView("df")

spark.sql("""
    select count(distinct author_id) 
    from df
""").show()