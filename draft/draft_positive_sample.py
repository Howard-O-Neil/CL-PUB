from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

def count_train():
    sample_pos_dir = "gs://clpub/data_lake/arnet/tables/coauthor_positive_train/merge-0" \

    sample_pos_schema = StructType([ \
        StructField("author1", StringType(), False), \
        StructField("author2", StringType(), False), \
        StructField("label", IntegerType(), False), \
    ]) \

    sample_pos_df = spark.read.schema(sample_pos_schema).parquet(sample_pos_dir) \

    print(f"Train positives: {sample_pos_df.count()}")
    print(f"Train positives: {sample_pos_df.filter((sparkf.col('label') == 1)).count()}")

def count_test():
    sample_pos_dir = "gs://clpub/data_lake/arnet/tables/coauthor_positive_test/merge-0" \

    sample_pos_schema = StructType([ \
        StructField("author1", StringType(), False), \
        StructField("author2", StringType(), False), \
        StructField("label", IntegerType(), False), \
    ]) \

    sample_pos_df = spark.read.schema(sample_pos_schema).parquet(sample_pos_dir) \

    print(f"Test positives: {sample_pos_df.count()}")
    print(f"Test positives: {sample_pos_df.filter((sparkf.col('label') == 1)).count()}")

count_train()
count_test()
