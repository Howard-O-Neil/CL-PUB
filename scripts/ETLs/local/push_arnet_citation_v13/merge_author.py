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

author_schema = StructType([       
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_timestamp', LongType(), False),
    StructField('id', StringType(), False),
    StructField('name', StringType(), False),
])
merge_df = spark.read.schema(author_schema) \
    .parquet(
        "/data/recsys/arnet/tables/author/production/part-0",
        "/data/recsys/arnet/tables/author/production/part-1",
        "/data/recsys/arnet/tables/author/production/part-10",
        "/data/recsys/arnet/tables/author/production/part-11",
        "/data/recsys/arnet/tables/author/production/part-12",
        "/data/recsys/arnet/tables/author/production/part-13",
        "/data/recsys/arnet/tables/author/production/part-14",
        "/data/recsys/arnet/tables/author/production/part-15",
        "/data/recsys/arnet/tables/author/production/part-16",
        "/data/recsys/arnet/tables/author/production/part-17",
        "/data/recsys/arnet/tables/author/production/part-18",
        "/data/recsys/arnet/tables/author/production/part-19",
        "/data/recsys/arnet/tables/author/production/part-2",
        "/data/recsys/arnet/tables/author/production/part-20",
        "/data/recsys/arnet/tables/author/production/part-21",
        "/data/recsys/arnet/tables/author/production/part-22",
        "/data/recsys/arnet/tables/author/production/part-23",
        "/data/recsys/arnet/tables/author/production/part-24",
        "/data/recsys/arnet/tables/author/production/part-25",
        "/data/recsys/arnet/tables/author/production/part-26",
        "/data/recsys/arnet/tables/author/production/part-27",
        "/data/recsys/arnet/tables/author/production/part-28",
        "/data/recsys/arnet/tables/author/production/part-29",
        "/data/recsys/arnet/tables/author/production/part-3",
        "/data/recsys/arnet/tables/author/production/part-30",
        "/data/recsys/arnet/tables/author/production/part-31",
        "/data/recsys/arnet/tables/author/production/part-32",
        "/data/recsys/arnet/tables/author/production/part-33",
        "/data/recsys/arnet/tables/author/production/part-34",
        "/data/recsys/arnet/tables/author/production/part-35",
        "/data/recsys/arnet/tables/author/production/part-36",
        "/data/recsys/arnet/tables/author/production/part-37",
        "/data/recsys/arnet/tables/author/production/part-38",
        "/data/recsys/arnet/tables/author/production/part-39",
        "/data/recsys/arnet/tables/author/production/part-4",
        "/data/recsys/arnet/tables/author/production/part-40",
        "/data/recsys/arnet/tables/author/production/part-41",
        "/data/recsys/arnet/tables/author/production/part-42",
        "/data/recsys/arnet/tables/author/production/part-43",
        "/data/recsys/arnet/tables/author/production/part-44",
        "/data/recsys/arnet/tables/author/production/part-45",
        "/data/recsys/arnet/tables/author/production/part-46",
        "/data/recsys/arnet/tables/author/production/part-47",
        "/data/recsys/arnet/tables/author/production/part-48",
        "/data/recsys/arnet/tables/author/production/part-49",
        "/data/recsys/arnet/tables/author/production/part-5",
        "/data/recsys/arnet/tables/author/production/part-50",
        "/data/recsys/arnet/tables/author/production/part-51",
        "/data/recsys/arnet/tables/author/production/part-52",
        "/data/recsys/arnet/tables/author/production/part-53",
        "/data/recsys/arnet/tables/author/production/part-54",
        "/data/recsys/arnet/tables/author/production/part-55",
        "/data/recsys/arnet/tables/author/production/part-56",
        "/data/recsys/arnet/tables/author/production/part-57",
        "/data/recsys/arnet/tables/author/production/part-58",
        "/data/recsys/arnet/tables/author/production/part-59",
        "/data/recsys/arnet/tables/author/production/part-6",
        "/data/recsys/arnet/tables/author/production/part-60",
        "/data/recsys/arnet/tables/author/production/part-61",
        "/data/recsys/arnet/tables/author/production/part-62",
        "/data/recsys/arnet/tables/author/production/part-63",
        "/data/recsys/arnet/tables/author/production/part-64",
        "/data/recsys/arnet/tables/author/production/part-65",
        "/data/recsys/arnet/tables/author/production/part-66",
        "/data/recsys/arnet/tables/author/production/part-67",
        "/data/recsys/arnet/tables/author/production/part-68",
        "/data/recsys/arnet/tables/author/production/part-69",
        "/data/recsys/arnet/tables/author/production/part-7",
        "/data/recsys/arnet/tables/author/production/part-70",
        "/data/recsys/arnet/tables/author/production/part-71",
        "/data/recsys/arnet/tables/author/production/part-72",
        "/data/recsys/arnet/tables/author/production/part-73",
        "/data/recsys/arnet/tables/author/production/part-74",
        "/data/recsys/arnet/tables/author/production/part-75",
        "/data/recsys/arnet/tables/author/production/part-76",
        "/data/recsys/arnet/tables/author/production/part-77",
        "/data/recsys/arnet/tables/author/production/part-78",
        "/data/recsys/arnet/tables/author/production/part-79",
        "/data/recsys/arnet/tables/author/production/part-8",
        "/data/recsys/arnet/tables/author/production/part-80",
        "/data/recsys/arnet/tables/author/production/part-81",
        "/data/recsys/arnet/tables/author/production/part-82",
        "/data/recsys/arnet/tables/author/production/part-83",
        "/data/recsys/arnet/tables/author/production/part-84",
        "/data/recsys/arnet/tables/author/production/part-85",
        "/data/recsys/arnet/tables/author/production/part-86",
        "/data/recsys/arnet/tables/author/production/part-87",
        "/data/recsys/arnet/tables/author/production/part-88",
        "/data/recsys/arnet/tables/author/production/part-89",
        "/data/recsys/arnet/tables/author/production/part-9",
        "/data/recsys/arnet/tables/author/production/part-90",
        "/data/recsys/arnet/tables/author/production/part-91",
        "/data/recsys/arnet/tables/author/production/part-92",
        "/data/recsys/arnet/tables/author/production/part-93",
        "/data/recsys/arnet/tables/author/production/part-94",
        "/data/recsys/arnet/tables/author/production/part-95",
        "/data/recsys/arnet/tables/author/production/part-96",
        "/data/recsys/arnet/tables/author/production/part-97",
        "/data/recsys/arnet/tables/author/production/part-98"
    )

merge_df.createOrReplaceTempView("author_merge")

new_df = spark.sql("""
    select am._id, am._status, am._timestamp, am.id, am.name
    from author_merge as am, 
        (select first_value(am._id) as _id
            from author_merge as am
            group by am.id) as au
    where au._id = am._id
""")

new_df.write.mode("overwrite") \
    .format("parquet").save(f"/data/recsys/arnet/tables/author/production/merge-0")
