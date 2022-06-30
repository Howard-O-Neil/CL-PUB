from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession

published_history_dir = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"

spark = SparkSession.builder.getOrCreate()

published_history_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('year', FloatType(), False),
])

published_history_df = spark.read.schema(published_history_schema).parquet(published_history_dir)
published_history_df.createOrReplaceTempView("published_df")

spark.sql("""
    select max(year), min(year)
    from published_df
    where year > 1000 and year < 2300
""").show()
