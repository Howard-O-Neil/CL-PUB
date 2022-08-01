from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_dir      = "gs://clpub/data_lake/arnet/tables/test_samples/merge-0"
org_rank_dir    = "gs://clpub/data_lake/arnet/tables/author_org_rank/merge-0"
dst_dir         = "gs://clpub/data_lake/arnet/tables/org_rank_sample_test/merge-0"

optimized_partition_num = 2000

sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

org_rank_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("author_org", StringType(), False),
    StructField("org_rank", FloatType(), False),
    StructField("computed", IntegerType(), False),
])

# ===== Init DAG =====

sample_df       = spark.read.schema(sample_schema).parquet(sample_dir).repartition(100)
org_rank_df     = spark.read.schema(org_rank_schema).parquet(org_rank_dir)

sample_df.createOrReplaceTempView("sample_df")
org_rank_df.createOrReplaceTempView("org_rank_df")

ranking_samples_writer = spark.sql("""
    select sd.author1, sd.author2, ord1.author_org as org1, ord2.author_org as org2,
        ord1.org_rank as author1_org_rank, ord2.org_rank as author2_org_rank, 
        sd.label
    from sample_df as sd
        inner join org_rank_df as ord1 on ord1.author_id = sd.author1
        inner join org_rank_df as ord2 on ord2.author_id = sd.author2
""")

ranking_samples_writer.repartition(optimized_partition_num).write.mode("overwrite").parquet("hdfs:///temp/recsys/org_rank_test")

# ===== Break DAG =====

from pyspark.sql.functions import pandas_udf, PandasUDFType
from scipy.spatial import distance
import numpy as np
import pandas as pd

@pandas_udf("float", PandasUDFType.SCALAR)
def node_proximity(v1, v2):
    list_r1     =  v1.values.tolist()
    list_r2     =  v2.values.tolist() \
    
    list_res    = []
    for idx in range(0, len(list_r1)):
        proximity = 1
        if list_r1[idx] > 0 or list_r2[idx] > 0:
            proximity = abs(list_r1[idx] - list_r2[idx]) \
                / max(abs(list_r1[idx]), abs(list_r2[idx]))
        list_res.append(proximity) \
    
    return pd.Series(list_res)

org_rank_sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("org1", StringType(), False),
    StructField("org2", StringType(), False),
    StructField("author1_org_rank", FloatType(), False),
    StructField("author2_org_rank", FloatType(), False),
    StructField("label", IntegerType(), False),
])
ranking_samples = spark.read.schema(org_rank_sample_schema).parquet("hdfs:///temp/recsys/org_rank_test")
org_rank_samples = ranking_samples.select( \
    sparkf.col("author1"), sparkf.col("author2"), sparkf.col("org1"), sparkf.col("org2"), \
    node_proximity(sparkf.col("author1_org_rank"), sparkf.col("author2_org_rank")).alias("org_rank_proximity"), \
    sparkf.col("label") \
)

org_rank_samples.write.mode("overwrite").parquet(dst_dir)
