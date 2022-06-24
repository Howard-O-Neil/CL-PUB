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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

import copy
import uuid

published_history_dir   = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"
paper_vect_dir          = "s3://recsys-bucket/data_lake/arnet/tables/paper_vect/repartition-Hash-500"
dst_dir                 = "s3://recsys-bucket/data_lake/arnet/tables/author_vect/merge-0"

published_history_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('year', FloatType(), False),
])

paper_vect_schema = StructType([
    StructField('id', StringType(), False),
    StructField('feature', StringType(), False)
])

published_history_df = spark.read.schema(published_history_schema).parquet(published_history_dir)
published_history_df.createOrReplaceTempView("published_history_df")

paper_vect_df = spark.read.schema(paper_vect_schema).parquet(paper_vect_dir)
paper_vect_df.createOrReplaceTempView("paper_vect_df")

author_vect_join = spark.sql("""
    select phd.author_id as author_id, pvd.feature as paper_feature
    from published_history_df as phd 
        inner join paper_vect_df as pvd on phd.paper_id = pvd.id
""")

from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import pyarrow as pa
import numpy as np

@pandas_udf("string", PandasUDFType.GROUPED_AGG)  
def avg_vect_df(v: pd.DataFrame):
    list_form   = list(map(lambda x: list(map(lambda x: float(x), x.split(";"))), v.values.tolist()))
    list_np     = np.mean(np.array(list_form), axis=0)
    return ';'.join(map(str, list_np))

calculated_feature = author_vect_join.groupBy(sparkf.col("author_id")).agg(avg_vect_df(sparkf.col("paper_feature")).alias("feature"))

calculated_feature.write.mode("overwrite").parquet(dst_dir)
