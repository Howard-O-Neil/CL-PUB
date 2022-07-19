from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_dir      = "gs://clpub/data_lake/arnet/tables/test_samples/merge-0"
author_vect_dir = "gs://clpub/data_lake/arnet/tables/author_vect/merge-0"
dst_dir         = "gs://clpub/data_lake/arnet/tables/content_sample_test/merge-0"

optimized_partition_num = 2500

sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

author_vect_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("feature", StringType(), False),
])

sample_df       = spark.read.schema(sample_schema).parquet(sample_dir).repartition(optimized_partition_num)
author_vect_df  = spark.read.schema(author_vect_schema).parquet(author_vect_dir)

sample_df.createOrReplaceTempView("sample_df")
author_vect_df.createOrReplaceTempView("author_vect_df")

content_samples = spark.sql("""
    select sd.author1, sd.author2, 
        avd1.feature as author1_feature, avd2.feature as author2_feature, 
        sd.label
    from sample_df as sd
        inner join author_vect_df as avd1 on avd1.author_id = sd.author1
        inner join author_vect_df as avd2 on avd2.author_id = sd.author2
""")

from pyspark.sql.functions import pandas_udf, PandasUDFType
from scipy.spatial import distance
import numpy as np
import pandas as pd

@pandas_udf("float", PandasUDFType.SCALAR)  
def cosine_distance(v1, v2):
    list_v1     =  list(map(lambda x: list(map(lambda x: float(x), x.split(";"))), v1.values.tolist()))
    list_v2     =  list(map(lambda x: list(map(lambda x: float(x), x.split(";"))), v2.values.tolist())) \
    
    list_res    = []
    for idx in range(0, len(list_v1)):
        cos_dis     = np.float32(distance.cosine(list_v1[idx], list_v2[idx])).astype(float)
        list_res.append(cos_dis) \
    
    return pd.Series(list_res)

cosine_sample_df = content_samples.repartition(optimized_partition_num).select( \
    sparkf.col("author1"), sparkf.col("author2"), \
    cosine_distance(sparkf.col("author1_feature"), sparkf.col("author2_feature")).alias("cos_dist"), \
    sparkf.col("label") \
)

# cosine_sample_df.show()

cosine_sample_df.write.mode("overwrite").parquet(dst_dir)
