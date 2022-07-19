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

org_vertex_dir  = "gs://clpub/data_lake/arnet/tables/org_vertex/merge-0"
org_edge_dir    = "gs://clpub/data_lake/arnet/tables/org_edge/merge-0"
org_group_dir   = "gs://clpub/data_lake/arnet/tables/org_group/merge-0"

org_vertex_df   = spark.read.parquet(org_vertex_dir)
org_edge_df     = spark.read.parquet(org_edge_dir)
org_group_df    = spark.read.parquet(org_group_dir)

org_vertex_df.createOrReplaceTempView("org_vertex_df")
org_edge_df.createOrReplaceTempView("org_edge_df")
org_group_df.createOrReplaceTempView("org_group_df")

spark.sql("""
    select org_name
    from org_vertex_df
    group by org_name
    having count(_id) > 1
""").show()

spark.sql("""
    select count(*)
    from org_vertex_df
""").show()

spark.sql("""
    select count(*)
    from org_edge_df
""").show()

spark.sql("""
    select count(*)
    from org_group_df
""").show()

spark.sql("""
    select count(*)
    from org_group_df
    where num_item > 3
""").show()

spark.sql("""
    select count(distinct group)
    from org_group_df
""").show()

spark.sql("""
    select count(*)
    from (
        select group
        from org_group_df
        group by group
        having count(item) > 3
    ) draft_df
""").show()
