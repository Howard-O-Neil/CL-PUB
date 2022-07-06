from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

content_train_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/content_sample_train/merge-0"
content_test_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/content_sample_test/merge-0"

orgrank_train_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/org_rank_sample_train/merge-0"
orgrank_test_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/org_rank_sample_test/merge-0"

orgdiscrete_train_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/org_discrete_sample_train/merge-0"
orgdiscrete_test_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/org_discrete_sample_test/merge-0"

rwr_bias_train_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/rwr_bias_sample_train/merge-0"
rwr_bias_test_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/rwr_bias_sample_test/merge-0"

freq_train_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/activity_sample_train/merge-0"
freq_test_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/activity_sample_test/merge-0"

train_hdfs_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/training/merge-0"
test_hdfs_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/testing/merge-0"

spark = SparkSession.builder.getOrCreate()

spark.read.parquet(content_train_dir).createOrReplaceTempView("content_train_view")
spark.read.parquet(content_test_dir).createOrReplaceTempView("content_test_view")
spark.read.parquet(rwr_bias_train_dir).createOrReplaceTempView("rwrbias_train_view")
spark.read.parquet(rwr_bias_test_dir).createOrReplaceTempView("rwrbias_test_view")
spark.read.parquet(freq_train_dir).createOrReplaceTempView("freq_train_view")
spark.read.parquet(freq_test_dir).createOrReplaceTempView("freq_test_view")

orgrank_train_df = spark.read.parquet(orgrank_train_dir).repartition(5000) 
orgrank_train_df.createOrReplaceTempView("orgrank_train_view")

orgrank_test_df = spark.read.parquet(orgrank_test_dir).repartition(5000)
orgrank_test_df.createOrReplaceTempView("orgrank_test_view")

orgdiscrete_train_df = spark.read.parquet(orgdiscrete_train_dir).repartition(5000)
orgdiscrete_train_df.createOrReplaceTempView("orgdiscrete_train_view")

orgdiscrete_test_df = spark.read.parquet(orgdiscrete_test_dir).repartition(5000)
orgdiscrete_test_df.createOrReplaceTempView("orgdiscrete_test_view")

train_writer = spark.sql("""
    select ctv.author1, ctv.author2, ctv.cos_dist, otv.org_rank_proximity, rtv.rwr_bias_proximity, ftv.freq_proximity, ctv.label
    from content_train_view as ctv
        inner join orgrank_train_view TABLESAMPLE(10000000 ROWS) as otv on ctv.author1 = otv.author1 and ctv.author2 = otv.author2
        inner join orgdiscrete_train_view as odtv on ctv.author1 = odtv.author1 and ctv.author2 = odtv.author2
        inner join rwrbias_train_view as rtv on ctv.author1 = rtv.author1 and ctv.author2 = rtv.author2
        inner join freq_train_view as ftv on ctv.author1 = ftv.author1 and ctv.author2 = ftv.author2
""")

test_writer = spark.sql("""
    select ctv.author1, ctv.author2, ctv.cos_dist, otv.org_rank_proximity, rtv.rwr_bias_proximity, ftv.freq_proximity, ctv.label
    from content_test_view as ctv
        inner join orgrank_test_view TABLESAMPLE(1000000 ROWS) as otv on ctv.author1 = otv.author1 and ctv.author2 = otv.author2
        inner join orgdiscrete_test_view as odtv on ctv.author1 = odtv.author1 and ctv.author2 = odtv.author2
        inner join rwrbias_test_view as rtv on ctv.author1 = rtv.author1 and ctv.author2 = rtv.author2
        inner join freq_test_view as ftv on ctv.author1 = ftv.author1 and ctv.author2 = ftv.author2
""")

train_writer.write.mode("overwrite").parquet(train_hdfs_dir)
test_writer.write.mode("overwrite").parquet(test_hdfs_dir)

spark.stop()
