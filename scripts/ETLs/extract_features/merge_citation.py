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

file_prefix = "gs://clpub/data_lake/arnet/tables/citation/parts/"
dst_dir     = "gs://clpub/data_lake/arnet/tables/citation/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

schema = StructType([
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

sources = []

# 438 files recorded
for i in range(0, 438):
    sources.append(file_prefix + "part-" + str(i))

merge_df = spark.read.schema(schema).parquet(*sources)
merge_df.createOrReplaceTempView("citation_merge")

new_df = spark.sql("""
    select
        mc._id,
        mc._status,
        mc._order,
        mc.paper_id,
        mc.paper_title,
        mc.author_id,
        mc.author_name,
        mc.author_org,
        mc.cite,
        mc.year
    from citation_merge mc, (
            select first_value(mc2._id) as _id
            from citation_merge mc2
            group by mc2.paper_id, mc2.author_id, mc2.cite
        ) as mu
    where mc._id = mu._id
""")

# 4*3 = 12 core cluster, 12*5 partition
new_df.write.mode("overwrite").format("parquet").save(dst_dir)
