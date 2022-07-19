from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_dir      = "gs://clpub/data_lake/arnet/tables/train_samples/merge-0"
rwr_bias_dir    = "gs://clpub/data_lake/arnet/tables/author_rwr_bias/merge-0"
dst_dir         = "gs://clpub/data_lake/arnet/tables/rwr_bias_sample_train/merge-0"

optimized_partition_num = 200

sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

rwr_bias_schema = StructType([
    StructField("author_id", StringType(), False),
    StructField("ranking", FloatType(), False),
    StructField("computed", IntegerType(), False),
])

sample_df       = spark.read.schema(sample_schema).parquet(sample_dir).repartition(optimized_partition_num)
rwr_bias_df     = spark.read.schema(rwr_bias_schema).parquet(rwr_bias_dir)

sample_df.createOrReplaceTempView("sample_df")
rwr_bias_df.createOrReplaceTempView("rwr_bias_df")

ranking_samples = spark.sql("""
    select sd.author1, sd.author2, 
        rbd1.ranking as author1_ranking, rbd2.ranking as author2_ranking, 
        sd.label
    from sample_df as sd
        inner join rwr_bias_df as rbd1 on rbd1.author_id = sd.author1
        inner join rwr_bias_df as rbd2 on rbd2.author_id = sd.author2
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
        proximity =  \
            abs(list_r1[idx] - list_r2[idx]) \
            / max(abs(list_r1[idx]), abs(list_r2[idx])) \

        list_res.append(proximity) \
    
    return pd.Series(list_res)

rwr_bias_sample_df = ranking_samples.repartition(optimized_partition_num).select( \
    sparkf.col("author1"), sparkf.col("author2"), \
    node_proximity(sparkf.col("author1_ranking"), sparkf.col("author2_ranking")).alias("rwr_bias_proximity"), \
    sparkf.col("label") \
)

# rwr_bias_sample_df.filter((sparkf.col("label") == 0)).count()
# rwr_bias_sample_df.filter((sparkf.col("rwr_bias_proximity") <= 0.01) & (sparkf.col("label") == 0)).count()
# rwr_bias_sample_df.filter((sparkf.col("rwr_bias_proximity") > 0.01) & (sparkf.col("rwr_bias_proximity") <= 0.2) & (sparkf.col("label") == 0)).count()
# rwr_bias_sample_df.filter((sparkf.col("label") == 1)).count()
# rwr_bias_sample_df.filter((sparkf.col("rwr_bias_proximity") <= 0.01) & (sparkf.col("label") == 1)).count()
# rwr_bias_sample_df.filter((sparkf.col("rwr_bias_proximity") > 0.01) & (sparkf.col("rwr_bias_proximity") <= 0.2) & (sparkf.col("label") == 1)).count()

# rwr_bias_sample_df.filter((sparkf.col("label") == 0)).select(sparkf.avg(sparkf.col("rwr_bias_proximity"))).show()
# rwr_bias_sample_df.filter((sparkf.col("label") == 1)).select(sparkf.avg(sparkf.col("rwr_bias_proximity"))).show()

rwr_bias_sample_df.write.mode("overwrite").parquet(dst_dir)
