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

citation_rwr_dir    = "gs://clpub/data_lake/arnet/tables/citation_rwr/iter-"

test_df1 = spark.read.parquet(citation_rwr_dir + "1")
test_df2 = spark.read.parquet(citation_rwr_dir + "2")
test_df3 = spark.read.parquet(citation_rwr_dir + "14")

test_df1.createOrReplaceTempView("test_df1")
test_df2.createOrReplaceTempView("test_df2")
test_df3.createOrReplaceTempView("test_df3")

spark.sql("""
    select id, node._1 as ranking
    from test_df1
    where id < 10
""").show()

spark.sql("""
    select id, node._1 as ranking
    from test_df2
    where id < 10
""").show()

spark.sql("""
    select id, node._1 as ranking
    from test_df3
    where id < 10
""").show()
