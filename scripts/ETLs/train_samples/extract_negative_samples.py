from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

spark = SparkSession.builder.getOrCreate()

coauthor_dir    = "gs://clpub/data_lake/arnet/tables/coauthor/merge-0"
dst_dir         = "gs://clpub/data_lake/arnet/tables/coauthor_negative_train/merge-0"

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
                .filter((sparkf.col("year") >= 1980) & (sparkf.col("year") <= 2015)) \
                .filter((sparkf.col("author1_id") != "") & (sparkf.col("author2_id") != "")) \

coauthor_df.createOrReplaceTempView("coauth_df")

positive_samples = spark.sql("""
    select author1_id as author1, author2_id as author2, 1 as label
    from coauth_df
    group by author1_id, author2_id
""")
positive_samples.createOrReplaceTempView("positive_samples")

unique_df = spark.sql("""
    select distinct author_id
    from (
        select distinct author1_id as author_id
        from coauth_df
        where author1_id != ""
        union
        select distinct author2_id as author_id
        from coauth_df
        where author2_id != ""
    )
""").repartition(50)

unique_df.createOrReplaceTempView("unique_df")

cross_join_df = spark.sql("""
    select ud1.author_id as author1, ud2.author_id as author2
    from unique_df as ud1 
        cross join unique_df TABLESAMPLE (10 ROWS) as ud2
    where ud1.author_id !=  ud2.author_id
""")
cross_join_df.createOrReplaceTempView("cross_join_df")

negative_samples = spark.sql("""
    select cjd.author1, cjd.author2, 0 as label
    from cross_join_df as cjd
        left join positive_samples as ps on cjd.author1 = ps.author1 and cjd.author2 = ps.author2
    where ps.author1 is null and ps.author2 is null
""")

# positive_samples.count()
# negative_samples.count()

negative_samples.write.mode("overwrite").parquet(dst_dir)
