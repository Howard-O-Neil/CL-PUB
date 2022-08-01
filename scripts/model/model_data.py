from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

content_train_dir = "gs://clpub/data_lake/arnet/tables/content_sample_train/merge-0"
content_test_dir = "gs://clpub/data_lake/arnet/tables/content_sample_test/merge-0"

orgrank_train_dir = "gs://clpub/data_lake/arnet/tables/org_rank_sample_train/merge-0"
orgrank_test_dir = "gs://clpub/data_lake/arnet/tables/org_rank_sample_test/merge-0"

orgdiscrete_train_dir = "gs://clpub/data_lake/arnet/tables/org_discrete_sample_train/merge-0"
orgdiscrete_test_dir = "gs://clpub/data_lake/arnet/tables/org_discrete_sample_test/merge-0"

rwr_bias_train_dir = "gs://clpub/data_lake/arnet/tables/rwr_bias_sample_train/merge-0"
rwr_bias_test_dir = "gs://clpub/data_lake/arnet/tables/rwr_bias_sample_test/merge-0"

freq_train_dir = "gs://clpub/data_lake/arnet/tables/activity_sample_train/merge-0"
freq_test_dir = "gs://clpub/data_lake/arnet/tables/activity_sample_test/merge-0"

train_dst_dir = "gs://clpub/data_lake/arnet/tables/training/merge-0"
test_dst_dir = "gs://clpub/data_lake/arnet/tables/testing/merge-0"

spark = SparkSession.builder.getOrCreate()

spark.read.parquet(content_train_dir).createOrReplaceTempView("content_train_view")
spark.read.parquet(content_test_dir).createOrReplaceTempView("content_test_view")
spark.read.parquet(rwr_bias_train_dir).createOrReplaceTempView("rwrbias_train_view")
spark.read.parquet(rwr_bias_test_dir).createOrReplaceTempView("rwrbias_test_view")
spark.read.parquet(freq_train_dir).createOrReplaceTempView("freq_train_view")
spark.read.parquet(freq_test_dir).createOrReplaceTempView("freq_test_view")

spark.read.parquet(orgrank_train_dir).createOrReplaceTempView("orgrank_train_view")
spark.read.parquet(orgrank_test_dir).createOrReplaceTempView("orgrank_test_view")

spark.read.parquet(orgdiscrete_train_dir).createOrReplaceTempView("orgdiscrete_train_view")
spark.read.parquet(orgdiscrete_test_dir).createOrReplaceTempView("orgdiscrete_test_view")

train_writer = spark.sql("""
    select otv.author1, otv.author2, ctv.cos_dist, otv.org_rank_proximity, odtv.org_discrete_proximity, rtv.rwr_bias_proximity, ftv.freq_proximity, otv.label
    from orgrank_train_view TABLESAMPLE(10000000 ROWS) as otv
        inner join orgdiscrete_train_view as odtv on odtv.author1 = otv.author1 and odtv.author2 = otv.author2
            and odtv.org1 = otv.org1 and odtv.org2 = otv.org2
        inner join content_train_view as ctv on ctv.author1 = otv.author1 and ctv.author2 = otv.author2 
        inner join rwrbias_train_view as rtv on rtv.author1 = otv.author1 and rtv.author2 = otv.author2
        inner join freq_train_view as ftv on ftv.author1 = otv.author1 and ftv.author2 = otv.author2
""")

test_writer = spark.sql("""
    select otv.author1, otv.author2, ctv.cos_dist, otv.org_rank_proximity, odtv.org_discrete_proximity, rtv.rwr_bias_proximity, ftv.freq_proximity, otv.label
    from orgrank_test_view TABLESAMPLE(1000000 ROWS) as otv
        inner join orgdiscrete_test_view as odtv on odtv.author1 = otv.author1 and odtv.author2 = otv.author2
            and odtv.org1 = otv.org1 and odtv.org2 = otv.org2
        inner join content_test_view as ctv on ctv.author1 = otv.author1 and ctv.author2 = otv.author2 
        inner join rwrbias_test_view as rtv on rtv.author1 = otv.author1 and rtv.author2 = otv.author2
        inner join freq_test_view as ftv on ftv.author1 = otv.author1 and ftv.author2 = otv.author2
""")

train_writer.write.mode("overwrite").parquet(train_dst_dir)

test_writer.write.mode("overwrite").parquet(test_dst_dir)
