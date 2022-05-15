import sys

sys.path.append("../../..")

from pprint import pprint
import numpy as np
import pandas as pd
from prototype.crawl_init_data.increase_id import cal_next_id

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

published_history_schema = StructType([       
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_timestamp', LongType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('year', FloatType(), False),
])

merge_df = spark.read.schema(published_history_schema) \
    .parquet("/data/recsys/arnet/tables/published_history/production/merge-0")

merge_df.createOrReplaceTempView("publish_history_merge")

print("===== Count all")
new_df = spark.sql("""
    select *
    from publish_history_merge
""").show(20, False)

print("===== Count duplicates")
new_df = spark.sql("""
    select count(_id)
    from publish_history_merge
    group by author_id, paper_id
    having count(_id) > 1
""").show()

print("===== Count duplicates uuid")
new_df = spark.sql("""
    select count(_status)
    from publish_history_merge
    group by _id
    having count(_status) > 1
""").show()
