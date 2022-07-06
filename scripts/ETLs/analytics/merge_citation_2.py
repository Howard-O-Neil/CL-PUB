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

published_history_dir   = "s3://recsys-bucket-1/data_lake/arnet/tables/published_history/merge-0"
citation_dir            = "s3://recsys-bucket-1/data_lake/arnet/tables/citation/merge-0"
dst_dir                 = "s3://recsys-bucket-1/data_lake/arnet/tables/citation_2/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

publish_history_schema = StructType([
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

citation_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('cite', StringType(), False),
    StructField('year', FloatType(), False),
])

publish_df  = spark.read.schema(publish_history_schema).parquet(published_history_dir)
citation_df = spark.read.schema(citation_schema).parquet(citation_dir)

publish_df.createOrReplaceTempView("publish_df")
citation_df.createOrReplaceTempView("citation_df")

@sparkf.udf
def gen_uuid():
    return str(uuid.uuid1())

spark.udf.register("gen_uuid", gen_uuid)

new_df = spark.sql("""
    select 
        gen_uuid() as _id,
        0 as _status,
        0 as _order,
        cf.paper_id as paper_id,
        cf.paper_title as paper_title,
        cf.author_id as author1_id,
        cf.author_name as author1_name,
        cf.author_org as author1_org,
        pf.author_id as author2_id,
        pf.author_name as author2_name,
        pf.author_org as author2_org,
        cf.year as year
    from citation_df as cf inner join publish_df as pf on cf.cite = pf.paper_id
    where pf.author_id != cf.author_id
""")

# 4*3 = 12 core cluster, 12*5 partition
new_df.repartition(60).write.mode("overwrite").format("parquet").save(dst_dir)
