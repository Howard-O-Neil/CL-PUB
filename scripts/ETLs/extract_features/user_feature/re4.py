from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

author_vect_dir = "gs://clpub/data_lake/arnet/tables/author_vect/merge-0"
author_rwr_bias_dir = "gs://clpub/data_lake/arnet/tables/author_rwr_bias/merge-0"
author_org_rank_dir = "gs://clpub/data_lake/arnet/tables/author_org_rank/merge-0"
dst_dir = "gs://clpub/data_lake/arnet/tables/user_feature/re4/merge-0"

spark = SparkSession.builder.getOrCreate()

spark.read.parquet("gs://clpub/data_lake/arnet/tables/published_history/merge-0").createOrReplaceTempView("published_history")
spark.read.parquet(author_vect_dir).createOrReplaceTempView("author_vect")
spark.read.parquet(author_rwr_bias_dir).createOrReplaceTempView("author_rwr_bias")
spark.read.parquet(author_org_rank_dir).createOrReplaceTempView("author_org_rank")

spark.sql("""
    select first_value(author_id) as author_id, author_name
    from published_history
    group by author_name
""").createOrReplaceTempView("unique_author")

user_feature = spark.sql("""
    select ua.author_id, ua.author_name, av.feature, arb.ranking, aor.org_rank
    from unique_author as ua
        inner join author_vect as av on ua.author_id = av.author_id
        inner join author_org_rank as aor on ua.author_id = aor.author_id
        inner join author_rwr_bias as arb on ua.author_id = arb.author_id
""")

user_feature.write.mode("overwrite").parquet(dst_dir)
