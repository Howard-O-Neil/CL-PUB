from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_dir          = "gs://clpub/data_lake/arnet/tables/test_samples/merge-0"
org_discrete_dir    = "gs://clpub/data_lake/arnet/tables/author_org_discrete/merge-0"
dst_dir             = "gs://clpub/data_lake/arnet/tables/org_discrete_sample_test/merge-0"

optimized_partition_num = 2000

sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

org_discrete_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("author_org", StringType(), False),
    StructField("org_rank", FloatType(), False),
    StructField("computed", IntegerType(), False),
])

# ===== Init DAG =====

sample_df       = spark.read.schema(sample_schema).parquet(sample_dir).repartition(100)
org_discrete_df = spark.read.schema(org_discrete_schema).parquet(org_discrete_dir)

sample_df.createOrReplaceTempView("sample_df")
org_discrete_df.createOrReplaceTempView("org_discrete_df")

ranking_samples_writer = spark.sql("""
    select sd.author1, sd.author2, odd1.author_org as org1, odd2.author_org as org2,
        odd1.org_rank as author1_org_rank, odd2.org_rank as author2_org_rank, 
        sd.label
    from sample_df as sd
        inner join org_discrete_df as odd1 on odd1.author_id = sd.author1
        inner join org_discrete_df as odd2 on odd2.author_id = sd.author2
""")
ranking_samples_writer.repartition(optimized_partition_num).write.mode("overwrite").parquet("hdfs:///temp/recsys/org_discrete_test")

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

org_discrete_sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("org1", StringType(), False),
    StructField("org2", StringType(), False),
    StructField("author1_org_rank", FloatType(), False),
    StructField("author2_org_rank", FloatType(), False),
    StructField("label", IntegerType(), False),
])
ranking_samples = spark.read.schema(org_discrete_sample_schema).parquet("hdfs:///temp/recsys/org_discrete_test")
org_discrete_samples = ranking_samples.select( \
    sparkf.col("author1"), sparkf.col("author2"), sparkf.col("org1"), sparkf.col("org2"), \
    node_proximity(sparkf.col("author1_org_rank"), sparkf.col("author2_org_rank")).alias("org_discrete_proximity"), \
    sparkf.col("label") \
)

org_discrete_samples.write.mode("overwrite").parquet(dst_dir)
