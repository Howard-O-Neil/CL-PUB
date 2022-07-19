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

org_group_path = "gs://clpub/data_lake/arnet/tables/org_group/merge-0"

group_struct = StructType([
    StructField("group", LongType(), False),
    StructField("item", LongType(), False)
])

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

org_group = spark.read.schema(group_struct).parquet(org_group_path)
org_group.createOrReplaceTempView("org_group_df")

spark.sql("""
    select count(*)
    from (
        select count(item) as num_item
        from org_group_df
        group by group
    ) as group_org
""").show()

spark.sql("""
    select count(*)
    from (
        select count(item) as num_item
        from org_group_df
        group by group
    ) as group_org
    where group_org.num_item > 3
""").show()
