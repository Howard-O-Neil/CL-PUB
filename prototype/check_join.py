import sys

sys.path.append("..")

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

userSchema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
])
user_data = [
    ("1", "alisa"),
    ("2", "grande"),
    ("3", "alex"),
    ("4", "john"),
    ("5", "mia")
]
user_df = spark.createDataFrame(data=user_data, schema=userSchema)
user_df.createOrReplaceTempView("user")

userBuySchema = StructType([
    StructField("user_id", StringType(), False),
    StructField("item_id", StringType(), False),
])
userBuy_data = [
    ("1", "2"),
    ("3", "5"),
    ("6", "4")
]
userBuy_df = spark.createDataFrame(data=userBuy_data, schema=userBuySchema)
userBuy_df.createOrReplaceTempView("userBuy")

spark.sql("""
select *
from user as u left join userBuy as ub on u.id = ub.user_id
""").show()

spark.sql("""
select *
from userBuy as ub left join user as u on u.id = ub.user_id
""").show()