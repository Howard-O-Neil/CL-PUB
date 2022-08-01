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

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .getOrCreate()

# Your playground here

coauthor_df = spark.read.parquet("/home/hadoop/spark/arnet/tables/coauthor/production/merge-0")
new_df = coauthor_df.withColumn('_timestamp', coauthor_df["_timestamp"] * 0).withColumnRenamed('_timestamp', '_order')
new_df.repartition(240).write.mode("overwrite") \
    .format("parquet").save(f"/home/hadoop/spark/arnet/tables/coauthor/production/merge-1")