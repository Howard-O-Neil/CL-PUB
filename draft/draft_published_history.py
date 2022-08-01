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

published_history_dir = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

deptSchema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('year', FloatType(), False),
])

deptDf = spark.read.schema(deptSchema).parquet(published_history_dir)
deptDf.createOrReplaceTempView("published_history")

deptDf.write.mode("overwrite").parquet("gs://clpub/data_lake/arnet/tables/published_history/backup-0")

spark.sql("""
    select *
    from published_history
""").show(vertical=True)

spark.sql("""
    select count(distinct author_id)
    from published_history
    where author_id != ""
""").show()

spark.sql("""
    select count(distinct paper_id)
    from published_history
""").show()
