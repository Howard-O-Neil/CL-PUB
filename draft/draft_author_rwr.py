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

author_rwr_dir          = "s3://recsys-bucket/data_lake/arnet/tables/author_rwr/merge-0"
author_rwr_bias_dir     = "s3://recsys-bucket/data_lake/arnet/tables/author_rwr_bias/merge-0"
published_history_dir   = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"
citation_vertex_dir     = "s3://recsys-bucket/data_lake/arnet/tables/citation_vertex/merge-0"
citation_rwr_dir        = "s3://recsys-bucket/data_lake/arnet/tables/citation_rwr/iter-14"
citation_rwr_bias_dir   = "s3://recsys-bucket/data_lake/arnet/tables/citation_rwr_bias/iter-14"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

ranking_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("ranking", FloatType(), False),
    StructField("computed", IntegerType(), False),
])

published_history_schema = StructType([
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

citation_rwr_node_schema = StructType([
    StructField("_1", FloatType(), False),
    StructField("_2", IntegerType(), False),
    StructField("_3", IntegerType(), False),
])
citation_rwr_schema = StructType([
    StructField('id', LongType(), False),
    StructField('node', citation_rwr_node_schema, False)
])

citation_rwr_bias_node_schema = StructType([
    StructField("_1", FloatType(), False),
    StructField("_2", IntegerType(), False),
    StructField("_3", IntegerType(), False),
    StructField("_4", FloatType(), False),
])
citation_rwr_bias_schema = StructType([
    StructField('id', LongType(), False),
    StructField('node', citation_rwr_bias_node_schema, False)
])


author_rwr_df           = spark.read.schema(ranking_schema).parquet(author_rwr_dir)
author_rwr_bias_df      = spark.read.schema(ranking_schema).parquet(author_rwr_bias_dir)
citation_vertex_df      = spark.read.parquet(citation_vertex_dir)
citation_rwr_df         = spark.read.schema(citation_rwr_schema).parquet(citation_rwr_dir)
citation_rwr_bias_df    = spark.read.schema(citation_rwr_bias_schema).parquet(citation_rwr_bias_dir)
published_history_df    = spark.read.schema(published_history_schema).parquet(published_history_dir)

author_rwr_df.createOrReplaceTempView("author_rwr_df")
author_rwr_bias_df.createOrReplaceTempView("author_rwr_bias_df")
citation_vertex_df.createOrReplaceTempView("citation_vertex_df")
citation_rwr_df.createOrReplaceTempView("citation_rwr_df")
citation_rwr_bias_df.createOrReplaceTempView("citation_rwr_bias_df")
published_history_df.createOrReplaceTempView("published_history_df")

spark.sql("""
    select *
    from author_rwr_bias_df
""").count()

spark.sql("""
    select *
    from author_rwr_bias_df
    where computed=0
""").count()

spark.sql("""
    select count(distinct author_id)
    from published_history_df
""").show()

spark.sql("""
    select count(*)
    from citation_vertex_df
""").show()

spark.sql("""
    select count(*)
    from author_rwr_df
    where computed = 0
""").show()
