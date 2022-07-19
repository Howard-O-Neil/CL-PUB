import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

published_history_dir   = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"
dst_dir                 = "gs://clpub/data_lake/arnet/tables/published_history/draft-0"

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
published_history_df.show(vertical=True)

published_history_df.createOrReplaceTempView("pub_df")

sample_df = spark.sql("""
    select pd._id, pd._status, pd._order, 
        pd.author_id, pd.author_name, pd.author_org, 
        pd.paper_id, pd.paper_title, pd.year
    from pub_df TABLESAMPLE(1000 ROWS) as pd
""")

sample_df.write.mode("overwrite").parquet(dst_dir)
