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

coauthor_dir            = "gs://clpub/data_lake/arnet/tables/coauthor/merge-0"
org_vertex_dir          = "gs://clpub/data_lake/arnet/tables/org_vertex/merge-0"
dst_dir                 = "gs://clpub/data_lake/arnet/tables/org_edge/merge-0"

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

org_vertex_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('org_name', StringType(), False),
    StructField('row_order', LongType(), False),
])

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

org_vertex_df   = spark.read.schema(org_vertex_schema).parquet(org_vertex_dir)
coauthor_df     = spark.read.schema(coauthor_schema).parquet(coauthor_dir)

org_vertex_df.createOrReplaceTempView("org_vertex_df")
coauthor_df.createOrReplaceTempView("coauthor_df")

@sparkf.udf
def gen_uuid():
    return str(uuid.uuid1())

spark.udf.register("gen_uuid", gen_uuid)

group_org_df = spark.sql("""
    select
        cd.author1_org as org1,
        cd.author2_org as org2,
        count(cd._id) as collab_count
    from coauthor_df as cd
    where cd.author1_org != "" and cd.author2_org != ""
    group by cd.author1_org, cd.author2_org
""")
group_org_df.createOrReplaceTempView("group_org_df")

# not including blank org
org_edge_df = spark.sql("""
    select
        gen_uuid() as _id,
        0 as _status,
        0 as _order,
        ovd1.org_name as org1_name,
        ovd2.org_name as org2_name,
        cast(ovd1.row_order as long) as org1_row,
        cast(ovd2.row_order as long) as org2_row,
        cast(god.collab_count as long) as collab_count
    from group_org_df as god
        inner join org_vertex_df ovd1 on god.org1 = ovd1.org_name
        inner join org_vertex_df ovd2 on god.org2 = ovd2.org_name
""")
org_edge_df.show(vertical=True)

# 4*3 = 12 core cluster, 12*5 partition
org_edge_df.write.mode("overwrite").format("parquet").save(dst_dir)
