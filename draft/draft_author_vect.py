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

author_vect_dir    = "gs://clpub/data_lake/arnet/tables/author_vect/merge-0"

author_vect_schema = StructType([
    StructField('author_id', StringType(), False),
    StructField('feature', StringType(), False)
])

author_vect_df = spark.read.schema(author_vect_schema).parquet(author_vect_dir)
author_vect_df.createOrReplaceTempView("author_vect_df")

spark.sql("""
    select count(author_id)
    from author_vect_df
""").show()

spark.sql("""
    select count(*)
    from (
        select feature
        from author_vect_df
        group by feature
        having count(author_id) > 1
    ) as count_df
""").show()
