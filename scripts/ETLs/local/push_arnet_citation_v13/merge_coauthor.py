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

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
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
    .parquet(
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-0",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-1",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-10",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-11",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-12",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-13",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-14",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-15",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-16",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-17",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-18",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-19",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-2",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-20",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-21",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-22",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-23",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-24",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-25",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-26",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-27",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-28",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-29",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-3",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-30",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-31",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-32",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-33",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-34",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-35",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-36",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-37",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-38",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-39",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-4",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-40",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-41",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-42",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-43",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-44",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-5",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-6",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-7",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-8",
        "/home/hadoop/spark/arnet/tables/coauthor/production/part-9"
    )

merge_df.createOrReplaceTempView("coauthor_merge")

new_df = spark.sql("""
    select 
        cam._id,
        cam._status,
        cam._timestamp,
        cam.paper_id,
        cam.paper_title,
        cam.author1_id,
        cam.author1_name,
        cam.author1_org,
        cam.author2_id,
        cam.author2_name,
        cam.author2_org,
        cam.year
    from coauthor_merge as cam, 
        (select first_value(cam._id) as _id
            from coauthor_merge as cam
            group by cam.paper_id, cam.author1_id, cam.author2_id) as cau
    where cau._id = cam._id
""")

new_df.repartition(240).write.mode("overwrite") \
    .format("parquet").save(f"/home/hadoop/spark/arnet/tables/coauthor/production/merge-0")
