import sys

from pprint import pprint

# each JSON is small, there's no need in iterative processing
import json
import sys
import os
import xml
import time

import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

import copy
import uuid

author_org_rank_dir = "gs://clpub/data_lake/arnet/tables/author_org_rank/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

ranking_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("org_rank", FloatType(), False),
])

author_org_rank_df = spark.read.schema(ranking_schema).parquet(author_org_rank_dir)
author_org_rank_df.createOrReplaceTempView("author_org_rank_df")

author_org_rank_df.count()

# tin huynh id  : 53f47e76dabfaec09f299f95
# kiem hoang id : 53f466c4dabfaeb22f53ee97 

spark.sql("""
    select *
    from author_org_rank_df
    where author_id = '53f47e76dabfaec09f299f95'
        or author_id = '53f466c4dabfaeb22f53ee97'
""").show()
