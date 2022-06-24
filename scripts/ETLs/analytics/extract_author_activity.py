from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField
from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkf
import math

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

published_history_dir = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"
dst_dir               = "s3://recsys-bucket/data_lake/arnet/tables/author_activity/merge-0"

published_history_schema = StructType([
    StructField("_id", StringType(), False),
    StructField("_status", IntegerType(), False),
    StructField("_order", IntegerType(), False),
    StructField("author_id", StringType(), False),
    StructField("author_name", StringType(), False),
    StructField("author_org", StringType(), False),
    StructField("paper_id", StringType(), False),
    StructField("paper_title", StringType(), False),
    StructField("year", FloatType(), False)
])

published_history_df = spark.read.schema(published_history_schema).parquet(published_history_dir)
published_history_df.createOrReplaceTempView("published_history_df")

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import pyarrow as pa
import numpy as np

def pd_freg(v: pd.Series):
    list_years = list(map(lambda x: float(x), v.values.tolist()))
    list_years.sort() \

    increament      = 2
    t_i             = list_years[0]
    t_0             = 2021
    count_published = 0
    result          = 0. \

    for i in range(0, len(list_years)):
        if list_years[i] > t_i + increament:
            result += count_published * (1 / math.log( abs(t_0 - t_i) + 10)) \

            count_published = 0
            t_i             = list_years[i]
        if list_years[i] <= t_i + increament:
            count_published += 1 \

    result += count_published * (1 / math.log( abs(t_0 - t_i) + 10))
    return result

def cal_activity_frequency(key, v: pd.DataFrame):
    series = v.groupby(['author_id'])["year"].agg([pd_freg]) \

    return pd.DataFrame({
        "author_id": [key[0]],
        "freq": [series.values[0][0]]
    })

filtered_df = published_history_df.filter((sparkf.col("year") > 1000) & (sparkf.col("year") < 2023))

filtered_df.repartition(500).write.mode("overwrite").parquet("hdfs:///temp/recsys/filter_paper_time")

filtered_df_read = spark.read.schema(published_history_schema).parquet("hdfs:///temp/recsys/filter_paper_time").repartition(500)

author_freq_df = filtered_df_read.groupBy(sparkf.col("author_id")).applyInPandas(
                    cal_activity_frequency, schema="author_id string, freq float")

author_freq_df.write.mode("overwrite").parquet(dst_dir)
