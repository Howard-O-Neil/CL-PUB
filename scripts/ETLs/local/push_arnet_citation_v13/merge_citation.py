import sys

from pprint import pprint
import numpy as np
import pandas as pd

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

import copy
import uuid

os.environ["JAVA_HOME"] = "/opt/corretto-8"

JAVA_LIB = "/opt/corretto-8/lib"

list_part = [
    "/home/hadoop/spark/arnet/tables/citation/production/part-0",
    "/home/hadoop/spark/arnet/tables/citation/production/part-1",
    "/home/hadoop/spark/arnet/tables/citation/production/part-10",
    "/home/hadoop/spark/arnet/tables/citation/production/part-100",
    "/home/hadoop/spark/arnet/tables/citation/production/part-101",
    "/home/hadoop/spark/arnet/tables/citation/production/part-102",
    "/home/hadoop/spark/arnet/tables/citation/production/part-103",
    "/home/hadoop/spark/arnet/tables/citation/production/part-104",
    "/home/hadoop/spark/arnet/tables/citation/production/part-105",
    "/home/hadoop/spark/arnet/tables/citation/production/part-106",
    "/home/hadoop/spark/arnet/tables/citation/production/part-107",
    "/home/hadoop/spark/arnet/tables/citation/production/part-108",
    "/home/hadoop/spark/arnet/tables/citation/production/part-109",
    "/home/hadoop/spark/arnet/tables/citation/production/part-11",
    "/home/hadoop/spark/arnet/tables/citation/production/part-110",
    "/home/hadoop/spark/arnet/tables/citation/production/part-111",
    "/home/hadoop/spark/arnet/tables/citation/production/part-112",
    "/home/hadoop/spark/arnet/tables/citation/production/part-113",
    "/home/hadoop/spark/arnet/tables/citation/production/part-114",
    "/home/hadoop/spark/arnet/tables/citation/production/part-115",
    "/home/hadoop/spark/arnet/tables/citation/production/part-116",
    "/home/hadoop/spark/arnet/tables/citation/production/part-117",
    "/home/hadoop/spark/arnet/tables/citation/production/part-118",
    "/home/hadoop/spark/arnet/tables/citation/production/part-119",
    "/home/hadoop/spark/arnet/tables/citation/production/part-12",
    "/home/hadoop/spark/arnet/tables/citation/production/part-120",
    "/home/hadoop/spark/arnet/tables/citation/production/part-121",
    "/home/hadoop/spark/arnet/tables/citation/production/part-122",
    "/home/hadoop/spark/arnet/tables/citation/production/part-123",
    "/home/hadoop/spark/arnet/tables/citation/production/part-124",
    "/home/hadoop/spark/arnet/tables/citation/production/part-125",
    "/home/hadoop/spark/arnet/tables/citation/production/part-126",
    "/home/hadoop/spark/arnet/tables/citation/production/part-127",
    "/home/hadoop/spark/arnet/tables/citation/production/part-128",
    "/home/hadoop/spark/arnet/tables/citation/production/part-129",
    "/home/hadoop/spark/arnet/tables/citation/production/part-13",
    "/home/hadoop/spark/arnet/tables/citation/production/part-130",
    "/home/hadoop/spark/arnet/tables/citation/production/part-131",
    "/home/hadoop/spark/arnet/tables/citation/production/part-132",
    "/home/hadoop/spark/arnet/tables/citation/production/part-133",
    "/home/hadoop/spark/arnet/tables/citation/production/part-134",
    "/home/hadoop/spark/arnet/tables/citation/production/part-135",
    "/home/hadoop/spark/arnet/tables/citation/production/part-136",
    "/home/hadoop/spark/arnet/tables/citation/production/part-137",
    "/home/hadoop/spark/arnet/tables/citation/production/part-138",
    "/home/hadoop/spark/arnet/tables/citation/production/part-139",
    "/home/hadoop/spark/arnet/tables/citation/production/part-14",
    "/home/hadoop/spark/arnet/tables/citation/production/part-140",
    "/home/hadoop/spark/arnet/tables/citation/production/part-141",
    "/home/hadoop/spark/arnet/tables/citation/production/part-142",
    "/home/hadoop/spark/arnet/tables/citation/production/part-143",
    "/home/hadoop/spark/arnet/tables/citation/production/part-144",
    "/home/hadoop/spark/arnet/tables/citation/production/part-145",
    "/home/hadoop/spark/arnet/tables/citation/production/part-146",
    "/home/hadoop/spark/arnet/tables/citation/production/part-147",
    "/home/hadoop/spark/arnet/tables/citation/production/part-148",
    "/home/hadoop/spark/arnet/tables/citation/production/part-149",
    "/home/hadoop/spark/arnet/tables/citation/production/part-15",
    "/home/hadoop/spark/arnet/tables/citation/production/part-150",
    "/home/hadoop/spark/arnet/tables/citation/production/part-151",
    "/home/hadoop/spark/arnet/tables/citation/production/part-152",
    "/home/hadoop/spark/arnet/tables/citation/production/part-153",
    "/home/hadoop/spark/arnet/tables/citation/production/part-154",
    "/home/hadoop/spark/arnet/tables/citation/production/part-155",
    "/home/hadoop/spark/arnet/tables/citation/production/part-156",
    "/home/hadoop/spark/arnet/tables/citation/production/part-157",
    "/home/hadoop/spark/arnet/tables/citation/production/part-158",
    "/home/hadoop/spark/arnet/tables/citation/production/part-159",
    "/home/hadoop/spark/arnet/tables/citation/production/part-16",
    "/home/hadoop/spark/arnet/tables/citation/production/part-160",
    "/home/hadoop/spark/arnet/tables/citation/production/part-161",
    "/home/hadoop/spark/arnet/tables/citation/production/part-162",
    "/home/hadoop/spark/arnet/tables/citation/production/part-163",
    "/home/hadoop/spark/arnet/tables/citation/production/part-164",
    "/home/hadoop/spark/arnet/tables/citation/production/part-17",
    "/home/hadoop/spark/arnet/tables/citation/production/part-18",
    "/home/hadoop/spark/arnet/tables/citation/production/part-19",
    "/home/hadoop/spark/arnet/tables/citation/production/part-2",
    "/home/hadoop/spark/arnet/tables/citation/production/part-20",
    "/home/hadoop/spark/arnet/tables/citation/production/part-21",
    "/home/hadoop/spark/arnet/tables/citation/production/part-22",
    "/home/hadoop/spark/arnet/tables/citation/production/part-23",
    "/home/hadoop/spark/arnet/tables/citation/production/part-24",
    "/home/hadoop/spark/arnet/tables/citation/production/part-25",
    "/home/hadoop/spark/arnet/tables/citation/production/part-26",
    "/home/hadoop/spark/arnet/tables/citation/production/part-27",
    "/home/hadoop/spark/arnet/tables/citation/production/part-28",
    "/home/hadoop/spark/arnet/tables/citation/production/part-29",
    "/home/hadoop/spark/arnet/tables/citation/production/part-3",
    "/home/hadoop/spark/arnet/tables/citation/production/part-30",
    "/home/hadoop/spark/arnet/tables/citation/production/part-31",
    "/home/hadoop/spark/arnet/tables/citation/production/part-32",
    "/home/hadoop/spark/arnet/tables/citation/production/part-33",
    "/home/hadoop/spark/arnet/tables/citation/production/part-34",
    "/home/hadoop/spark/arnet/tables/citation/production/part-35",
    "/home/hadoop/spark/arnet/tables/citation/production/part-36",
    "/home/hadoop/spark/arnet/tables/citation/production/part-37",
    "/home/hadoop/spark/arnet/tables/citation/production/part-38",
    "/home/hadoop/spark/arnet/tables/citation/production/part-39",
    "/home/hadoop/spark/arnet/tables/citation/production/part-4",
    "/home/hadoop/spark/arnet/tables/citation/production/part-40",
    "/home/hadoop/spark/arnet/tables/citation/production/part-41",
    "/home/hadoop/spark/arnet/tables/citation/production/part-42",
    "/home/hadoop/spark/arnet/tables/citation/production/part-43",
    "/home/hadoop/spark/arnet/tables/citation/production/part-44",
    "/home/hadoop/spark/arnet/tables/citation/production/part-45",
    "/home/hadoop/spark/arnet/tables/citation/production/part-46",
    "/home/hadoop/spark/arnet/tables/citation/production/part-47",
    "/home/hadoop/spark/arnet/tables/citation/production/part-48",
    "/home/hadoop/spark/arnet/tables/citation/production/part-49",
    "/home/hadoop/spark/arnet/tables/citation/production/part-5",
    "/home/hadoop/spark/arnet/tables/citation/production/part-50",
    "/home/hadoop/spark/arnet/tables/citation/production/part-51",
    "/home/hadoop/spark/arnet/tables/citation/production/part-52",
    "/home/hadoop/spark/arnet/tables/citation/production/part-53",
    "/home/hadoop/spark/arnet/tables/citation/production/part-54",
    "/home/hadoop/spark/arnet/tables/citation/production/part-55",
    "/home/hadoop/spark/arnet/tables/citation/production/part-56",
    "/home/hadoop/spark/arnet/tables/citation/production/part-57",
    "/home/hadoop/spark/arnet/tables/citation/production/part-58",
    "/home/hadoop/spark/arnet/tables/citation/production/part-59",
    "/home/hadoop/spark/arnet/tables/citation/production/part-6",
    "/home/hadoop/spark/arnet/tables/citation/production/part-60",
    "/home/hadoop/spark/arnet/tables/citation/production/part-61",
    "/home/hadoop/spark/arnet/tables/citation/production/part-62",
    "/home/hadoop/spark/arnet/tables/citation/production/part-63",
    "/home/hadoop/spark/arnet/tables/citation/production/part-64",
    "/home/hadoop/spark/arnet/tables/citation/production/part-65",
    "/home/hadoop/spark/arnet/tables/citation/production/part-66",
    "/home/hadoop/spark/arnet/tables/citation/production/part-67",
    "/home/hadoop/spark/arnet/tables/citation/production/part-68",
    "/home/hadoop/spark/arnet/tables/citation/production/part-69",
    "/home/hadoop/spark/arnet/tables/citation/production/part-7",
    "/home/hadoop/spark/arnet/tables/citation/production/part-70",
    "/home/hadoop/spark/arnet/tables/citation/production/part-71",
    "/home/hadoop/spark/arnet/tables/citation/production/part-72",
    "/home/hadoop/spark/arnet/tables/citation/production/part-73",
    "/home/hadoop/spark/arnet/tables/citation/production/part-74",
    "/home/hadoop/spark/arnet/tables/citation/production/part-75",
    "/home/hadoop/spark/arnet/tables/citation/production/part-76",
    "/home/hadoop/spark/arnet/tables/citation/production/part-77",
    "/home/hadoop/spark/arnet/tables/citation/production/part-78",
    "/home/hadoop/spark/arnet/tables/citation/production/part-79",
    "/home/hadoop/spark/arnet/tables/citation/production/part-8",
    "/home/hadoop/spark/arnet/tables/citation/production/part-80",
    "/home/hadoop/spark/arnet/tables/citation/production/part-81",
    "/home/hadoop/spark/arnet/tables/citation/production/part-82",
    "/home/hadoop/spark/arnet/tables/citation/production/part-83",
    "/home/hadoop/spark/arnet/tables/citation/production/part-84",
    "/home/hadoop/spark/arnet/tables/citation/production/part-85",
    "/home/hadoop/spark/arnet/tables/citation/production/part-86",
    "/home/hadoop/spark/arnet/tables/citation/production/part-87",
    "/home/hadoop/spark/arnet/tables/citation/production/part-88",
    "/home/hadoop/spark/arnet/tables/citation/production/part-89",
    "/home/hadoop/spark/arnet/tables/citation/production/part-9",
    "/home/hadoop/spark/arnet/tables/citation/production/part-90",
    "/home/hadoop/spark/arnet/tables/citation/production/part-91",
    "/home/hadoop/spark/arnet/tables/citation/production/part-92",
    "/home/hadoop/spark/arnet/tables/citation/production/part-93",
    "/home/hadoop/spark/arnet/tables/citation/production/part-94",
    "/home/hadoop/spark/arnet/tables/citation/production/part-95",
    "/home/hadoop/spark/arnet/tables/citation/production/part-96",
    "/home/hadoop/spark/arnet/tables/citation/production/part-97",
    "/home/hadoop/spark/arnet/tables/citation/production/part-98",
    "/home/hadoop/spark/arnet/tables/citation/production/part-99"
]

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .getOrCreate()

citation_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('_status', IntegerType(), False),
    StructField('_order', IntegerType(), False),
    StructField('paper_id', StringType(), False),
    StructField('paper_title', StringType(), False),
    StructField('author_id', StringType(), False),
    StructField('author_name', StringType(), False),
    StructField('author_org', StringType(), False),
    StructField('cite', StringType(), False),
    StructField('year', FloatType(), False),
])

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

merge_df = spark.read.schema(citation_schema).parquet(*list_part)
merge_df.createOrReplaceTempView("merge_citation")

unique_df = spark.sql("""
select
    mc._id,
    mc._status,
    mc._order,
    mc.paper_id,
    mc.paper_title,
    mc.author_id,
    mc.author_name,
    mc.author_org,
    mc.cite,
    mc.year
from merge_citation mc, (
        select first_value(mc._id) as _id
        from merge_citation mc
        group by mc.paper_id, mc.author_id, mc.cite
    ) as mu
where mc._id = mu._id
""")

unique_df.repartition(240).write.mode("overwrite") \
    .format("parquet").save(f"/home/hadoop/spark/arnet/tables/citation/production/merge-0")