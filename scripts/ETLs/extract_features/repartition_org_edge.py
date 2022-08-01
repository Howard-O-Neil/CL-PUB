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

org_edge_dir        = "gs://clpub/data_lake/arnet/tables/org_edge/merge-0"
dst_dir             = "gs://clpub/data_lake/arnet/tables/org_edge/merge-1"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

edge_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('org1_name', StringType(), False),
    StructField('org2_name', StringType(), False),
    StructField('org1_row', LongType(), False),
    StructField('org2_row', LongType(), False),
    StructField('collab_count', LongType(), False),
])
org_edge_df = spark.read.schema(edge_schema).parquet(org_edge_dir)
org_edge_df.createOrReplaceTempView("org_edge_df")

# org_edge_df.show(vertical=True)

# org_edge_df.count()

org_edge_df.repartition(200).write.mode("overwrite").parquet(dst_dir)
