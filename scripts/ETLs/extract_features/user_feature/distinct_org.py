from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

import tensorflow as tf

published_history_dir = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"
dst_dir = "gs://clpub/data_lake/arnet/tables/distinct_org/merge-0"

spark = SparkSession.builder.getOrCreate()

spark.read.parquet(published_history_dir).createOrReplaceTempView("published_df")

org_df = spark.sql("""
    select distinct author_org as name
    from published_df
""")

org_df.write.mode("overwrite").save(dst_dir)
