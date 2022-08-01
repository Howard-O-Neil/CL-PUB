from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

coauthor_dir    = "gs://clpub/data_lake/arnet/tables/coauthor/merge-0"
dst_dir         = "gs://clpub/data_lake/arnet/tables/coauthor_positive_test/merge-0"

coauthor_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('author1_id', StringType(), False),
    StructField('author1_name', StringType(), False),
    StructField('author1_org', StringType(), False),
    StructField('author2_id', StringType(), False),
    StructField('author2_name', StringType(), False),
    StructField('author2_org', StringType(), False),
    StructField('year', FloatType(), False),
])

coauthor_df = spark.read.schema(coauthor_schema).parquet(coauthor_dir) \
                .filter((sparkf.col("year") >= 2016) & (sparkf.col("year") <= 2021)) \
                .filter((sparkf.col("author1_id") != "") & (sparkf.col("author2_id") != "")) \

coauthor_df.createOrReplaceTempView("coauth_df")

positive_samples = spark.sql("""
    select author1_id as author1, author2_id as author2, 1 as label
    from coauth_df
    group by author1_id, author2_id
""")
positive_samples.write.mode("overwrite").parquet(dst_dir)
