from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

def count_train():
    sample_neg_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/coauthor_negative_train/merge-0" \

    sample_neg_schema = StructType([ \
        StructField("author1", StringType(), False), \
        StructField("author2", StringType(), False), \
        StructField("label", IntegerType(), False), \
    ]) \

    sample_neg_df = spark.read.schema(sample_neg_schema).parquet(sample_neg_dir) \

    print(f"Train negatives: {sample_neg_df.count()}")
    print(f"Train negatives: {sample_neg_df.filter((sparkf.col('label') == 0)).count()}")

def count_test():
    sample_neg_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/coauthor_negative_test/merge-0" \

    sample_neg_schema = StructType([ \
        StructField("author1", StringType(), False), \
        StructField("author2", StringType(), False), \
        StructField("label", IntegerType(), False), \
    ]) \

    sample_neg_df = spark.read.schema(sample_neg_schema).parquet(sample_neg_dir) \

    print(f"Test negatives: {sample_neg_df.count()}")
    print(f"Test negatives: {sample_neg_df.filter((sparkf.col('label') == 0)).count()}")

count_train()
count_test()
