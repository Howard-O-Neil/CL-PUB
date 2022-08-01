# The uglier version of standard model data
# May be better ...

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

content_train_dir = "gs://clpub/data_lake/arnet/tables/content_sample_train/merge-0"
orgrank_train_dir = "gs://clpub/data_lake/arnet/tables/org_rank_sample_train/merge-0"
rwr_bias_train_dir = "gs://clpub/data_lake/arnet/tables/rwr_bias_sample_train/merge-0"

train_dst_dir = "gs://clpub/data_lake/arnet/tables/training/merge-0"

spark = SparkSession.builder.getOrCreate()

spark.read.parquet(content_train_dir).createOrReplaceTempView("content_train_view")
spark.read.parquet(rwr_bias_train_dir).createOrReplaceTempView("rwrbias_train_view")
spark.read.parquet(orgrank_train_dir).createOrReplaceTempView("orgrank_train_view")

train_writer = spark.sql("""
    select otv.author1, otv.author2, ctv.cos_dist, otv.org_rank_proximity, rtv.rwr_bias_proximity, otv.label
    from orgrank_train_view as otv
        inner join content_train_view as ctv on ctv.author1 = otv.author1 and ctv.author2 = otv.author2 
        inner join rwrbias_train_view as rtv on rtv.author1 = otv.author1 and rtv.author2 = otv.author2
""")

train_writer.write.mode("overwrite").parquet(train_dst_dir)
