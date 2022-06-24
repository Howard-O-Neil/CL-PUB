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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType, DoubleType
import pyspark.sql.functions as sparkf

import copy
import uuid

org_rank_dir        = "s3://recsys-bucket/data_lake/arnet/tables/org_rank_algo/iter-14"
org_vertex_dir      = "s3://recsys-bucket/data_lake/arnet/tables/org_vertex/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

org_vertex_schema = StructType([
    StructField("_id", StringType(), False),
    StructField("_status", IntegerType(), False),
    StructField("_order", IntegerType(), False),
    StructField("org_name", StringType(), False),
    StructField("row_order", LongType(), False)
])

org_node_schema = StructType([
    StructField("_1", FloatType(), False),
    StructField("_2", IntegerType(), False),
    StructField("_3", IntegerType(), False),
    StructField("_4", LongType(), False),
    StructField("_5", FloatType(), False),
])
org_rank_schema = StructType([
    StructField("id", LongType(), False),
    StructField("node", org_node_schema, False),
])

org_rank_df = spark.read.schema(org_rank_schema).parquet(org_rank_dir)
org_rank_df.createOrReplaceTempView("org_rank_df")

org_vertex_df = spark.read.schema(org_vertex_schema).parquet(org_vertex_dir)
org_vertex_df.createOrReplaceTempView("org_vertex_df")

spark.sql("""
    select *
    from org_rank_df
    where node._1 <= 0
""").show()
