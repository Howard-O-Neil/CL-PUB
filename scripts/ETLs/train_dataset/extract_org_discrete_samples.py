from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_dir          = "s3://recsys-bucket/data_lake/arnet/tables/train_samples/merge-0"
org_discrete_dir    = "s3://recsys-bucket/data_lake/arnet/tables/author_org_discrete/merge-0"
dst_dir             = "s3://recsys-bucket/data_lake/arnet/tables/org_discrete_sample_train/merge-0"

optimized_partition_num = 2500

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

sample_df       = spark.read.schema(sample_schema).parquet(sample_dir).repartition(optimized_partition_num)
org_discrete_df = spark.read.schema(org_discrete_schema).parquet(org_discrete_dir)

sample_df.createOrReplaceTempView("sample_df")
org_discrete_df.createOrReplaceTempView("org_discrete_df")

ranking_samples = spark.sql("""
    select sd.author1, sd.author2, 
        odd1.org_rank as author1_org_rank, odd2.org_rank as author2_org_rank, 
        sd.label
    from sample_df as sd
        inner join org_discrete_df as odd1 on odd1.author_id = sd.author1
        inner join org_discrete_df as odd2 on odd2.author_id = sd.author2
""")

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

org_discrete_samples = ranking_samples.repartition(optimized_partition_num).select( \
    sparkf.col("author1"), sparkf.col("author2"), \
    node_proximity(sparkf.col("author1_org_rank"), sparkf.col("author2_org_rank")).alias("org_discrete_proximity"), \
    sparkf.col("label") \
)

org_discrete_samples.write.mode("overwrite").parquet(dst_dir)
