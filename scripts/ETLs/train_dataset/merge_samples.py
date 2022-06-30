from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

positive_dir    = "s3://recsys-bucket/data_lake/arnet/tables/coauthor_positive_train/merge-0"
negative_dir    = "s3://recsys-bucket/data_lake/arnet/tables/coauthor_negative_train/merge-0"
dst_dir         = "s3://recsys-bucket/data_lake/arnet/tables/train_samples/merge-0"

sample_schema = StructType([
    StructField("author1", StringType(), False),
    StructField("author2", StringType(), False),
    StructField("label", IntegerType(), False),
])

positive_df = spark.read.schema(sample_schema).parquet(positive_dir)
negative_df = spark.read.schema(sample_schema).parquet(negative_dir)

sample_df = positive_df.union(negative_df)

sample_shuffle_df = sample_df.withColumn("rand_order", sparkf.rand()) \
                    .orderBy(sparkf.col("rand_order")) \
                    .select(sparkf.col("author1"), sparkf.col("author2"), sparkf.col("label")) \

sample_shuffle_df.filter((sparkf.col("label") == 0)).count()
sample_shuffle_df.filter((sparkf.col("label") == 1)).count()

sample_shuffle_df.show()

sample_shuffle_df.write.mode("overwrite").parquet(dst_dir)
