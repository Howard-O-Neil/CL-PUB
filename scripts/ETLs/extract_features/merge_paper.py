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

import copy
import uuid

file_prefix = "gs://clpub/data_lake/arnet/tables/papers/parts/"
dst_dir     = "gs://clpub/data_lake/arnet/tables/papers/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('title', StringType(), False),
    StructField('abstract', StringType(), False),
    StructField('authors_id', ArrayType(StringType(), False), False),
    StructField('authors_name', ArrayType(StringType(), False), False),
    StructField('authors_org', ArrayType(StringType(), False), False),
    StructField('year', FloatType(), False),
    StructField('venue_raw', StringType(), False),
    StructField('issn', StringType(), False),
    StructField('isbn', StringType(), False),
    StructField('doi', StringType(), False),
    StructField('pdf', StringType(), False),
    StructField('url', ArrayType(StringType(), False), False)
])

sources = []

# 18 files recorded
for i in range(0, 18):
    sources.append(file_prefix + "part-" + str(i))

merge_df = spark.read.schema(schema).parquet(*sources)
merge_df.createOrReplaceTempView("paper_merge")

new_df = spark.sql("""
    Select pm._id, pm._status, pm._order, pm.paper_id, pm.title, pm.abstract, 
        pm.authors_id, pm.authors_name, pm.authors_org, 
        pm.year, pm.venue_raw, pm.issn, pm.isbn, pm.doi, pm.pdf, pm.url
    from paper_merge as pm, 
        (select first_value(pm_2._id) as _id
            from paper_merge as pm_2
            group by pm_2.paper_id) as pu
    where pu._id = pm._id
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

new_df.write.mode("overwrite").format("parquet").save(dst_dir)
