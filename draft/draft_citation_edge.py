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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

import copy
import uuid

citation_edge_dir   = "gs://clpub/data_lake/arnet/tables/citation_edge/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate()) 

citation_edge_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author1_id', StringType(), False),
    StructField('author2_id', StringType(), False),
    StructField('author1_row', LongType(), False),
    StructField('author2_row', LongType(), False),
    StructField('collab_count', LongType(), False),
])
citation_edge_df = spark.read.schema(citation_edge_schema).parquet(citation_edge_dir)
citation_edge_df.createOrReplaceTempView("citation_edge_df")

spark.sql("""
    select *
    from citation_edge_df
    where collab_count > 1
""").show()

spark.sql("""
    select count(*)
    from citation_edge_df
""").show()
