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

import copy
import uuid

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

# df = spark.read.option("delimiter", ",") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv("/data/adult_data.csv")

# print("===== Start =====")
# df.groupby('marital-status').agg({'capital-gain': 'mean'}).show()

# Int 0 -> normal
# Int 1 -> deleted
spark.sql("""
CREATE TABLE IF NOT EXISTS author (_id LONG, _status INT, _timestamp LONG, id LONG, name STRING)
    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/dblp/tables/author/production/";
""")

source = "/recsys/data/dblp/dblp.xml"
test_source = "/recsys/data/dblp/book.xml"

count_idx = -1 # call cal_next_id at the beginning, so id start with 1

def get_unavailable_name(data):
    if len(data) == 0:
        return []

    spark.sql("""
        CREATE OR REPLACE VIEW author_query
        as
        select /*+ COALESCE(4) */ * from author
    """)
    query = ""
    for record in data:
        name = record['name'].replace('"','\\"')
        query += f"""                   ("{name}"),\n"""
    
    query = query[:-2]
    res = spark.sql(f"""
        select name
        from author_query
        where name in (
            select * from 
                values
                    {query}
        )
        """).collect()

    list_res = list(map(lambda x: x.asDict()["name"], res))

    # convert to dictionary for fast search
    return dict(zip(list_res, [True] * len(list_res)))

def get_latest_id():
    res = spark.sql(f"""
            select a._id from author a
            order by a._id desc
            limit 1
        """).collect() \

    res = list(map(lambda x: x.asDict(), res))
    if len(res) == 0:
        return 0
    return int(res[0]["_id"])

def get_latest_uid():
    res = spark.sql(f"""
            select a.id from author a
            order by a.id desc
            limit 1
        """).collect() \

    res = list(map(lambda x: x.asDict(), res))
    if len(res) == 0:
        return 0
    return int(res[0]["id"])


def insert_author(data):
    composed_insert = ""
    flag = False

    new_id = get_latest_id() + 1
    new_uid = get_latest_uid() + 1

    unavai_names = get_unavailable_name(data)

    data = list(filter(lambda x: x["name"] not in unavai_names, data))
    for record in data:
        name = record['name'].replace('"','\\"')
        composed_insert += f"""       ({new_id}, 0, {int(time.time())}, {new_uid}, "{name}", array(), array()),\n"""

        print(f"[ ===== Checked author: {record['name']} ===== ]")

        new_id  += 1
        new_uid += 1
        flag    = True

    if flag == False:
        print(f"[ ===== NO Author Inserted ===== ]")
        return

    composed_insert = composed_insert[:-2] + ";\n"

    print(f"[ ===== BATCH Author Inserting ... ===== ]")

    spark.sql(f"""
    INSERT INTO author VALUES
    {composed_insert}
    """)

    print(f"[ ===== BATCH Author Inserted ===== ]")

INSERT_THRESHOLD = 150000

list_data   = []
map_data    = {}

import re

with open(source, "r") as author_read:
    current_data = {
        "id"            : "",
        "name"          : "",
        "urls"          : [],
        "affiliations"  : []
    }
    for i, line in enumerate(author_read):

        x = re.findall("<author>(.*?)</author>", line)
        if len(x) > 0:
            content = x[0]
            current_data["name"] = content
            value = copy.deepcopy(current_data)

            filter_cond = value["name"] not in map_data
            if filter_cond:
                list_data.append(value)
                map_data[value['name']] = True
                print(f"[ ===== Added author: {value['name']} ===== ]")
                print(f"[ ===== Author batch count: {len(list_data)} ===== ]")

            if len(list_data) >= INSERT_THRESHOLD:
                insert_author(list_data)
                list_data   = []
                map_data    = {}

insert_author(list_data)
