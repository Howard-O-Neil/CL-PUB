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

citation_dir = "s3://recsys-bucket/data_lake/arnet/tables/citation_2/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

citation_schema = StructType([
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

citation_df = spark.read.schema(citation_schema).parquet(citation_dir)
citation_df.createOrReplaceTempView("citation_df")

spark.sql("""
    select count(*)
    from (
        select author1_id, author2_id, count(_id) as collab_count
        from citation_df
        group by author1_id, author2_id
    ) count_df
""").show()

spark.sql("""
    select count(distinct count_df.author_id)
    from (
        (select cd.author1_id as author_id
        from citation_df as cd)
        union distinct
        (select cd.author2_id as author_id
        from citation_df as cd)
    ) count_df
""").show()

spark.sql("""
    select count(cf._id)
    from citation_df as cf
    group by cf.author1_id, cf.author2_id
    having count(cf._id) > 1
""").show(vertical=True)

