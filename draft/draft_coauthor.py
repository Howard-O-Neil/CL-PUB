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

count_parquet       = 0
# source              = "/home/hadoop/virtual/data/dataset/arnet/citation/dblpv13.json"
source_prefix       = "/home/hadoop/spark/arnet/tables/coauthor/parts/"
coauthor_dir        = "gs://clpub/data_lake/arnet/tables/coauthor/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

deptSchema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('author1_id', StringType(), False),
    StructField('author1_name', StringType(), False),
    StructField('author1_org', StringType(), False),
    StructField('author2_id', StringType(), False),
    StructField('author2_name', StringType(), False),
    StructField('author2_org', StringType(), False),
    StructField('year', FloatType(), False),
])

coauthor_df = spark.read.schema(deptSchema).parquet(coauthor_dir)
coauthor_df.createOrReplaceTempView("coauthor_df")

spark.sql("""
    select count(distinct tp_tab.author_id)
    from (
        select author1_id as author_id
        from coauthor_df
        union
        select author2_id as author_id
        from coauthor_df
    ) as tp_tab
""").show()

spark.sql("""
    select count(*)
    from (
        select count(_id)
        from coauthor_df
        group by author1_id, author2_id
    ) count_df
""").show()

spark.sql("""
    select count(*)
    from (
        select author1_id
        from df
        group by author1_id, author2_id, author1_org, author2_org
    )
""").show()


spark.sql("""
    select count(*)
    from (
        select author1_id
        from df
        where author1_org = "" or author2_org = ""
        group by author1_id, author2_id, author1_org, author2_org
    )
""").show()