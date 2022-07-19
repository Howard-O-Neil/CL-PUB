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

file_prefix = "gs://clpub/data_lake/arnet/tables/published_history/parts/"
dst_dir     = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

schema = StructType([
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

sources = []

# 71 files recorded
for i in range(0, 71):
    sources.append(file_prefix + "part-" + str(i))

merge_df = spark.read.schema(schema).parquet(*sources)
merge_df.createOrReplaceTempView("publish_history_merge")

new_df = spark.sql("""
    Select phm._id, phm._status, phm._order, phm.author_id, phm.author_name, phm.author_org, phm.paper_id, phm.paper_title, phm.year
    from publish_history_merge as phm, 
        (select first_value(phm_2._id) as _id
            from publish_history_merge as phm_2
            group by phm_2.author_id, phm_2.paper_id) as phu
    where phu._id = phm._id
""")

check = False
if check:
    new_df.createOrReplaceTempView("newdf")

    spark.sql("""
    select count(_id)
    from newdf
    where author_org = ""
    """).show(vertical=True)

    spark.sql("""
    select first_value(_id)
    from newdf
    group by _id
    having count(_id) > 1
    """).show(vertical=True)

# 4*3 = 12 core cluster, 12*5 partition
new_df.write.mode("overwrite").format("parquet").save(dst_dir)
