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

import copy
import uuid

month_index = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5, 'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12}

os.environ["JAVA_HOME"] = "/opt/corretto-8"
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .getOrCreate()

# df = spark.read.option("delimiter", ",") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv("/data/adult_data.csv")

# print("===== Start =====")
# df.groupby('marital-status').agg({'capital-gain': 'mean'}).show()

# Int 0 -> normal
# Int 1 -> deleted
spark.sql("""
CREATE TABLE IF NOT EXISTS bibtex (
    _id LONG, _status INT, _timestamp LONG, 
    id LONG, 
    entry STRING, 
    author ARRAY<LONG>, 
    title STRING,
    year LONG, 
    month LONG, 
    url ARRAY<STRING>, 
    ee ARRAY<STRING>, 
    address ARRAY<STRING>,
    journal STRING,
    isbn STRING)

    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/tables/bibtex/test/";
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS author (_id LONG, _status INT, _timestamp LONG, id LONG, name STRING, urls ARRAY<STRING>, affiliations ARRAY<STRING>)
    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/tables/author/production/";
""")

spark.sql("""
select count(title) from bibtex

""").show(1000, False)