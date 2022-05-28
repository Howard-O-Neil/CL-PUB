import sys

sys.path.append("..")

from pprint import pprint
import numpy as np
import pandas as pd

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
import pyspark.sql.functions as pyf

import copy
import uuid

os.environ["JAVA_HOME"] = "/opt/corretto-8"
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
    .getOrCreate()

# Your playground here


coauthor_df = spark.read.parquet("/data/recsys/arnet/tables/coauthor/production/merge-1")
coauthor_df.createOrReplaceTempView("coauthor")
query_df = spark.sql("""
select * from coauthor
where author2_name = ""
""")
query_df.show(vertical=True)
# print(query_df.count())
# query_df.show(vertical=True, n=1000000, truncate=100)


# coauthor_df = spark.read.parquet("/data/recsys/arnet/tables/coauthor/production/merge-1")
# coauthor_df.createOrReplaceTempView("coauthor")
# query_df = spark.sql("""
# select * from coauthor
# where author1_name like "%Kiem%"
# """)
# query_df.show(vertical=True, n=1000)

# uni_df = spark.read.parquet("/data/recsys/arnet/tables/organization/production/merge-0")
# uni_df.createOrReplaceTempView("uni")
# query_df = spark.sql("""
# select * from uni
# where name like "%University of Information Technology, Vietnam%"
# """)
# query_df.show(vertical=True, n=1000000, truncate=100)
