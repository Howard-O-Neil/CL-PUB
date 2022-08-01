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
    .config("spark.StorageLevel.DISK_ONLY", "true") \
    .getOrCreate()

coauthor_schema = StructType([     
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_timestamp', LongType(), False),
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

merge_df = spark.read.schema(coauthor_schema) \
    .parquet("/data/recsys/arnet/tables/coauthor/production/merge-0")

merge_df.repartition(500).createOrReplaceTempView("coauthor_merge")

paper_schema = StructType([       
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_timestamp', LongType(), False),
    StructField('id', StringType(), False),
    StructField('title', StringType(), False),
    StructField('year', FloatType(), False),
])

bibtex_df = spark.read.schema(paper_schema) \
    .parquet("/data/recsys/arnet/tables/bibtex/production/merge-0")

bibtex_df.repartition(500).createOrReplaceTempView("bibtex_merge")

print("===== Count all")
new_df = spark.sql("""
    select count(*)
    from coauthor_merge
""").show(20, False)

print("===== Count duplicates")
new_df = spark.sql("""
    select count(_id)
    from coauthor_merge
    group by paper_id, author1_id, author2_id
    having count(_id) > 1
""").show()

print("===== Count duplicates uuid")
new_df = spark.sql("""
    select count(_status)
    from coauthor_merge
    group by _id
    having count(_status) > 1
""").show()

print("===== Count paper not found")
new_df = spark.sql("""
    select count(cm._id)
    from coauthor_merge as cm left join bibtex_merge as bm on cm.paper_id = bm.id
    where bm.id is NULL
""")
new_df.show()