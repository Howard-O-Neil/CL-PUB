from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_dir          = "gs://clpub/data_lake/arnet/tables/train_samples/merge-0"
author_activ_dir    = "gs://clpub/data_lake/arnet/tables/author_activity/merge-Wnd-2"
dst_dir             = "gs://clpub/data_lake/arnet/tables/activity_sample_train/merge-0"

optimized_partition_num = 2500

sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

activity_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("freq", FloatType(), False),
])

sample_df       = spark.read.schema(sample_schema).parquet(sample_dir).repartition(optimized_partition_num)
activity_df     = spark.read.schema(activity_schema).parquet(author_activ_dir)

sample_df.createOrReplaceTempView("sample_df")
activity_df.createOrReplaceTempView("activity_df")

freq_samples = spark.sql("""
    select sd.author1, sd.author2, 
        add1.freq as author1_freq, add2.freq as author2_freq, 
        sd.label
    from sample_df as sd
        inner join activity_df as add1 on add1.author_id = sd.author1
        inner join activity_df as add2 on add2.author_id = sd.author2
""")

from pyspark.sql.functions import pandas_udf, PandasUDFType
from scipy.spatial import distance
import numpy as np
import pandas as pd


def proximity_freq_func(v1: pd.Series, v2: pd.Series) -> pd.Series:
    v1_log = np.log(v1 + 10.)
    v2_log = np.log(v2 + 10.)
    return ((v1_log - v2_log).abs()) / (pd.concat([v1_log, v2_log], axis=1).max(axis=1))

proximity_freq = pandas_udf(proximity_freq_func, returnType=FloatType())

proximity_freq_samples = freq_samples.repartition(optimized_partition_num).select( \
    sparkf.col("author1"), sparkf.col("author2"), \
    proximity_freq(sparkf.col("author1_freq"), sparkf.col("author2_freq")).alias("freq_proximity"), \
    sparkf.col("label") \
)

proximity_freq_samples.write.mode("overwrite").parquet(dst_dir)
