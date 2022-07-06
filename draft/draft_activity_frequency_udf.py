from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField
from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkf
import math

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import pyarrow as pa
import numpy as np

spark = SparkSession.builder.getOrCreate()

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
    series = v.groupby(['author_id'])["years"].agg([pd_freg]) \

    return pd.DataFrame({
        "author_id": [key[0]],
        "freq": [series.values[0][0]]
    })

df_list = [
    ("alice", 1985.),
    ("alice", 1988.),
    ("alice", 1985.),
    ("alice", 1987.),
    ("alice", 1997.),
    ("alice", 2002.),
    ("brunete", 2002.),
]
df = spark.createDataFrame(df_list, ["author_id", "years"])
df.createOrReplaceTempView("temp_v")

new_df = spark.sql("""
    select *
    from temp_v
""")

res_df = new_df.groupBy(sparkf.col("author_id")).applyInPandas(
    cal_activity_frequency, schema="author_id string, freq float")
res_df.show()
