from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

content_train_dir = "s3://recsys-bucket/data_lake/arnet/tables/content_sample_train/merge-0"
content_test_dir = "s3://recsys-bucket/data_lake/arnet/tables/content_sample_test/merge-0"

train_hdfs_dir = "hdfs:///temp/recsys/train/re1"
test_hdfs_dir = "hdfs:///temp/recsys/test/re1"

spark = SparkSession.builder.getOrCreate()

spark.read.parquet(content_train_dir).createOrReplaceTempView("content_train_view")
spark.read.parquet(content_test_dir).createOrReplaceTempView("content_test_view")

train_writer = spark.sql("""
    select author1, author2, cos_dist, label
    from content_train_view TABLESAMPLE(10000000 ROWS)
""")
test_writer = spark.sql("""
    select author1, author2, cos_dist, label
    from content_test_view TABLESAMPLE(1000000 ROWS)
""")

train_writer.write.mode("overwrite").parquet(train_hdfs_dir)
test_writer.write.mode("overwrite").parquet(test_hdfs_dir)

spark.stop()
