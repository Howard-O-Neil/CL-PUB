from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

def activity():
    sample_dir = "s3://recsys-bucket/data_lake/arnet/tables/activity_sample_test/merge-0" \

    sample_schema = StructType([
        StructField("author1", StringType(), False),
        StructField("author2", StringType(), False),
        StructField("freq_proximity", FloatType(), False),
        StructField("label", IntegerType(), False),
    ]) \

    sample_df = spark.read.schema(sample_schema).parquet(sample_dir)
    sample_df.createOrReplaceTempView("sample_df") \

    print(sample_df.count())

def content():
    sample_dir = "s3://recsys-bucket/data_lake/arnet/tables/content_sample_test/merge-0" \

    sample_schema = StructType([
        StructField("author1", StringType(), False),
        StructField("author2", StringType(), False),
        StructField("cos_dist", FloatType(), False),
        StructField("label", IntegerType(), False),
    ]) \

    sample_df = spark.read.schema(sample_schema).parquet(sample_dir)
    sample_df.createOrReplaceTempView("sample_df") \

    print(sample_df.count())

def org_discrete():
    sample_dir = "s3://recsys-bucket/data_lake/arnet/tables/org_discrete_sample_test/merge-0" \

    sample_schema = StructType([
        StructField("author1", StringType(), False),
        StructField("author2", StringType(), False),
        StructField("org_discrete_proximity", FloatType(), False),
        StructField("label", IntegerType(), False),
    ]) \

    sample_df = spark.read.schema(sample_schema).parquet(sample_dir)
    sample_df.createOrReplaceTempView("sample_df") \

    print(sample_df.count())

def org_rank():
    sample_dir = "s3://recsys-bucket/data_lake/arnet/tables/org_rank_sample_test/merge-0" \

    sample_schema = StructType([
        StructField("author1", StringType(), False),
        StructField("author2", StringType(), False),
        StructField("org_rank_proximity", FloatType(), False),
        StructField("label", IntegerType(), False),
    ]) \

    sample_df = spark.read.schema(sample_schema).parquet(sample_dir)
    sample_df.createOrReplaceTempView("sample_df") \

    print(sample_df.count())

def rwr_bias():
    rwr_bias_dir = "s3://recsys-bucket/data_lake/arnet/tables/rwr_bias_sample_test/merge-0" \

    sample_schema = StructType([
        StructField("author1", StringType(), False),
        StructField("author2", StringType(), False),
        StructField("rwr_bias_proximity", FloatType(), False),
        StructField("label", IntegerType(), False),
    ]) \

    sample_df = spark.read.schema(sample_schema).parquet(rwr_bias_dir)
    sample_df.createOrReplaceTempView("sample_df") \

    print(sample_df.count())

org_discrete()
org_rank()
rwr_bias()
activity()
content()
