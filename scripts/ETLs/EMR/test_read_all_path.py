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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType, DoubleType
import pyspark.sql.functions as sparkf

import copy
import uuid

all_path_dir = "s3://recsys-bucket/data_lake/arnet/tables/test_all_path/merge-0"

spark = (pyspark.SparkSession.builder.getOrCreate())

path_struct = StructType([
    StructField("_1", ArrayType(LongType(), False), False),
    StructField("_2", DoubleType(), False)
])

path_schema = StructType([
    StructField("id", LongType(), False),
    StructField("path", path_struct, False)
])

test_df = spark.read.schema(path_schema).parquet(all_path_dir)
test_df.createOrReplaceTempView("test_df")

spark.sql("""
    select *
    from test_df
    where id = 1 and element_at(path._1, 1) = 2
""").show(vertical=True, truncate=100, n=1000)

@sparkf.udf
def cal_path_unique():
    return str(uuid.uuid1())

spark.udf.register("gen_uuid", gen_uuid)

res_df = spark.sql("""
    select 
        element_at(element_at(path, 0)) as src,
        id as dst,
        weight 
    from test_df
    where size(element_at(path, 0)) > 1
""")
