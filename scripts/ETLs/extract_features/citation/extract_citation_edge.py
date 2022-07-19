import sys

from pprint import pprint

# each JSON is small, there's no need in iterative processing
import json
import sys
import os
import xml
import time

import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as sparkf

import copy
import uuid

citation_dir        = "gs://clpub/data_lake/arnet/tables/citation_2/merge-0"
citation_vertex     = "gs://clpub/data_lake/arnet/tables/citation_vertex/merge-0"
dst_dir             = "gs://clpub/data_lake/arnet/tables/citation_edge/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate()) 

citation_schema = StructType([
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
citation_df = spark.read.schema(citation_schema).parquet(citation_dir)
citation_df.createOrReplaceTempView("citation_df")

citation_vertex_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('author_id', StringType(), False),
    StructField('row_order', LongType(), False),
])
citation_vertex_df = spark.read.schema(citation_vertex_schema).parquet(citation_vertex)
citation_vertex_df.createOrReplaceTempView("citation_vertex_df")

@sparkf.udf
def gen_uuid():
    return str(uuid.uuid1())

spark.udf.register("gen_uuid", gen_uuid)

new_df = spark.sql("""
    select
        gen_uuid() as _id,
        0 as _status,
        0 as _order,
        cvd1.author_id as author1_id,
        cvd2.author_id as author2_id,
        cast(cvd1.row_order as long) as author1_row,
        cast(cvd2.row_order as long) as author2_row,
        cast(group_author.collab_count as long) as collab_count
    from (
        select author1_id, author2_id, count(_id) as collab_count
        from citation_df
        group by author1_id, author2_id
    ) as group_author
        inner join citation_vertex_df as cvd1 on cvd1.author_id = group_author.author1_id
        inner join citation_vertex_df as cvd2 on cvd2.author_id = group_author.author2_id
""")

new_df.write.mode("overwrite").parquet(dst_dir)
