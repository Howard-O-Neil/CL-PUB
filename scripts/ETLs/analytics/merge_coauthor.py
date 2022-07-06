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

import copy
import uuid

file_prefix = "s3://recsys-bucket-1/data_lake/arnet/tables/coauthor/parts/"
dst_dir     = "s3://recsys-bucket-1/data_lake/arnet/tables/coauthor/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

schema = StructType([
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

sources = []

# 119 files recorded
for i in range(0, 119):
    sources.append(file_prefix + "part-" + str(i))

merge_df = spark.read.schema(schema).parquet(*sources)
merge_df.createOrReplaceTempView("coauthor_merge")

new_df = spark.sql("""
    select 
        cam._id,
        cam._status,
        cam._order,
        cam.paper_id,
        cam.paper_title,
        cam.author1_id,
        cam.author1_name,
        cam.author1_org,
        cam.author2_id,
        cam.author2_name,
        cam.author2_org,
        cam.year
    from coauthor_merge as cam, 
        (select first_value(cam2._id) as _id
            from coauthor_merge as cam2
            group by cam2.paper_id, cam2.author1_id, cam2.author2_id) as cau
    where cau._id = cam._id
""")

# 4*3 = 12 core cluster, 12*5 partition
new_df.repartition(60).write.mode("overwrite").format("parquet").save(dst_dir)
