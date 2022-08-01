import sys

sys.path.append("../../..")

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

from prototype.crawl_init_data.increase_id import cal_next_id

# import requests
# response = requests.get("https://dblp.org/search/author/api?q=Tin Huynh&format=json")
# print(json.dumps(response.json(), indent=4))

# exit()

os.environ["JAVA_HOME"] = "/opt/corretto-8"
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"
os.environ["HADOOP_USER_NAME"] = "hadoop"

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .getOrCreate()

# Int 0 -> normal
# Int 1 -> deleted
spark.sql("""
CREATE TABLE IF NOT EXISTS author (_id LONG, _status INT, _timestamp LONG, id LONG, name STRING, urls ARRAY<STRING>, affiliations ARRAY<STRING>)
    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/dblp/tables/author/production/";
""")

# spark.sql("""

# select count(fid)
# from (
#     Select first_value(_id) as fid from author
#     group by name
# )

# """).show()

# spark.sql("""

# Select * from author
# where name in (select * from
#     values
#         ("Majid Nabi"))
# """).show()

spark.sql("""
select * from author where name like '%Weing&auml;rtner%'
""").show()

spark.sql("""
select count(distinct id) from author
""").show()

spark.sql("""
select count(distinct name) from author
""").show()

spark.sql("""
select count(distinct _id) from author
""").show()


res = spark.sql("""
with t(_id, id, name) as (
    select u._id, u.id, u.name
    from author_query u, (
        select a.id, a._id, a._status
        from author_query as a INNER JOIN (select a2._id, row_number() over (
                                        distribute by a2.id 
                                        order by a2._id desc) as row
                                    from author_query a2) as cou ON cou._id = a._id
        where a._id IS NOT NULL AND a._status IS NOT NULL 
            AND a._timestamp IS NOT NULL 
            AND cou.row = 1
    ) t
    where u._id = t._id and t._status = 0
)
select t.name, t.id
from t
where t.name in (
    select * from values
        ("Tin Huynh"),
        ("Mark F. Hornick"),
        ("D., Jaidhar C."),
        ("Da, Simeng")
)
""").collect()

print(res)
