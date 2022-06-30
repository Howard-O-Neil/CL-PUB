from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

sample_pos_dir = "s3://recsys-bucket/data_lake/arnet/tables/coauthor_positive_train/merge-0"

sample_pos_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

sample_pos_df = spark.read.schema(sample_pos_schema).parquet(sample_pos_dir)

sample_pos_df.count()
sample_pos_df.filter((sparkf.col("label") == 1)).count()
