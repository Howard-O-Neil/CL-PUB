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

org_vertex_dir          = "s3://recsys-bucket/data_lake/arnet/tables/org_vertex/merge-0"
org_rank_dir            = "s3://recsys-bucket/data_lake/arnet/tables/org_discrete_algo/merge-0"
published_history_dir   = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"
dst_dir                 = "s3://recsys-bucket/data_lake/arnet/tables/author_org_discrete/merge-0"

spark                   = (pyspark.sql.SparkSession.builder.getOrCreate())

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

org_vertex_df           = spark.read.schema(org_vertex_schema).parquet(org_vertex_dir)
org_rank_df             = spark.read.schema(org_rank_schema).parquet(org_rank_dir)
published_history_df    = spark.read.schema(published_history_schema).parquet(published_history_dir)

org_vertex_df.createOrReplaceTempView("org_vertex_df")
org_rank_df.createOrReplaceTempView("org_rank_df")
published_history_df.createOrReplaceTempView("published_history_df")

unique_author_df = spark.sql("""
    select author_id, author_org
    from published_history_df
    group by author_id, author_org
""")
unique_author_df.createOrReplaceTempView("unique_author_df")

max_val = spark.sql("select max(node._1) as max_val from org_rank_df").collect()[0]['max_val']
min_val = spark.sql("select min(node._1) as min_val from org_rank_df").collect()[0]['min_val']

author_org_rank_df = spark.sql(f"""
    select phd.author_id, phd.author_org,
        cast((CASE WHEN org_r.org_name IS NULL THEN 0 
            ELSE (((org_r.rank - {min_val}) / ({max_val} - {min_val})) + 0.005) END) as float) org_rank,
        (CASE WHEN org_r.org_name IS NULL THEN 0 ELSE 1 END) computed
    from unique_author_df as phd
        left join (
            select ovd.org_name as org_name, ord.node._1 as rank
            from org_vertex_df as ovd
                inner join org_rank_df as ord on ovd.row_order = ord.id
        ) org_r on phd.author_org = org_r.org_name
""")
author_org_rank_df.show()

author_org_rank_df.write.mode("overwrite").save(dst_dir)
