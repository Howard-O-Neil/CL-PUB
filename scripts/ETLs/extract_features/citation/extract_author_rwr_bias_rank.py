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

citation_rwr_bias_dir   = "gs://clpub/data_lake/arnet/tables/citation_rwr_bias/iter-14"
citation_vertex_dir     = "gs://clpub/data_lake/arnet/tables/citation_vertex/merge-0"
published_history_dir   = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"
dst_dir                 = "gs://clpub/data_lake/arnet/tables/author_rwr_bias/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

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
citation_vertex_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author_id', StringType(), False),
    StructField('row_order', LongType(), False),
])

citation_node_schema = StructType([
    StructField("_1", FloatType(), False),
    StructField("_2", IntegerType(), False),
    StructField("_3", IntegerType(), False),
    StructField("_4", FloatType(), False),
])
citation_rwr_bias_schema = StructType([
    StructField('id', LongType(), False),
    StructField('node', citation_node_schema, False)
])

published_df = spark.read.schema(published_history_schema).parquet(published_history_dir)
citation_vertex_df = spark.read.schema(citation_vertex_schema).parquet(citation_vertex_dir)
citation_rwr_bias_df = spark.read.schema(citation_rwr_bias_schema).parquet(citation_rwr_bias_dir)

published_df.createOrReplaceTempView("published_df")
citation_vertex_df.createOrReplaceTempView("citation_vertex_df")
citation_rwr_bias_df.createOrReplaceTempView("citation_rwr_bias_df")

author_unique_df = spark.sql("""
    select distinct author_id
    from published_df
""")
author_unique_df.createOrReplaceTempView("author_unique_df")

author_rwr_rank_df = spark.sql("""
    select aud.author_id, 
        cast((CASE WHEN cvd.authorid IS NULL THEN 0.15 * (1 / 3306895) ELSE cvd.node._1 END) as float) as ranking,
        (CASE WHEN cvd.authorid IS NULL THEN 0 ELSE 1 END) as computed
    from author_unique_df as aud
        left join (
            select cvd2.author_id as authorid, cvd2.row_order as row_order, crd.node as node
            from citation_vertex_df as cvd2
                inner join citation_rwr_bias_df as crd on cvd2.row_order = crd.id
        ) as cvd on cvd.authorid = aud.author_id
""")
author_rwr_rank_df.write.mode("overwrite").save(dst_dir)
