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

citation_vertex_dir     = "s3://recsys-bucket/data_lake/arnet/tables/citation_vertex/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate()) 

citation_vertex_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author_id', StringType(), False),
    StructField('row_order', LongType(), False),
])
citation_vertex_df = spark.read.schema(citation_vertex_schema).parquet(citation_vertex_dir)
citation_vertex_df.createOrReplaceTempView("citation_vertex_df")

spark.sql("""
    select row_order
    from citation_vertex_df
    group by row_order
    having count(_id) > 1
""").show()

spark.sql("""
    select author_id
    from citation_vertex_df
    group by author_id
    having count(_id) > 1
""").show()

spark.sql("""
    select count(*)
    from citation_vertex_df
""").show()
