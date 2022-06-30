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

citation_dir            = "s3://recsys-bucket/data_lake/arnet/tables/citation_2/merge-0"
published_history_dir   = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"
dst_dir                 = "s3://recsys-bucket/data_lake/arnet/tables/citation_vertex/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate()) 

published_history_schema = StructType([
    StructField("_id", StringType(), False),
    StructField("_status", IntegerType(), False),
    StructField("_order", IntegerType(), False),
    StructField("author_id", StringType(), False),
    StructField("author_name", StringType(), False),
    StructField("author_org", StringType(), False),
    StructField("paper_id", StringType(), False),
    StructField("paper_title", StringType(), False),
    StructField("year", FloatType(), False)
])

published_history_df = spark.read.schema(published_history_schema).parquet(published_history_dir)
published_history_df.createOrReplaceTempView("published_history_df")

@sparkf.udf
def gen_uuid():
    return str(uuid.uuid1())

spark.udf.register("gen_uuid", gen_uuid)

new_df = spark.sql("""
    select
        gen_uuid() as _id,
        0 as _status,
        0 as _order,
        group_cid.author_id as author_id,
        cast(monotonically_increasing_id() as long) as row_order
    from (
        select distinct author_id
        from published_history_df
    ) as group_cid
""")

new_df.write.mode("overwrite").parquet(dst_dir)
