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

author_org_rank_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/author_org_rank/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

ranking_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("author_org", StringType(), False),
    StructField("org_rank", FloatType(), False),
    StructField("computed", IntegerType(), False),
])

author_org_rank_df = spark.read.schema(ranking_schema).parquet(author_org_rank_dir)
author_org_rank_df.createOrReplaceTempView("author_org_rank_df")

author_org_rank_df.count()

spark.sql("""
    select *
    from author_org_rank_df
    where computed = 1
""").show()
