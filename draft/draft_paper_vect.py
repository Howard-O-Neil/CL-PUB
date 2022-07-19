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

paper_vect_dir    = "gs://clpub/data_lake/arnet/tables/content_v2/paper_vect/merge-0"

paper_vect_schema = StructType([
    StructField('id', StringType(), False),
    StructField('feature', StringType(), False)
])

paper_vect_df = spark.read.schema(paper_vect_schema).parquet(paper_vect_dir)
paper_vect_df.createOrReplaceTempView("paper_vect_df")

spark.sql("""
    select id
    from paper_vect_df
    group by id
    having count(id) > 1
""").show(vertical=True)

spark.sql("""
    select count(distinct id)
    from paper_vect_df
""").show(vertical=True)

spark.sql("""
    select count(*)
    from (
        select feature
        from paper_vect_df
        group by feature
        having count(id) > 1
    )
""").show(vertical=True)

spark.sql("""
    select count(*)
    from (
        select id
        from paper_vect_df
        group by id
        having count(id) > 1
    )
""").show(vertical=True)
