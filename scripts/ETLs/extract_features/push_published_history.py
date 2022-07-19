import sys

from pprint import pprint

# each JSON is small, there's no need in iterative processing
import orjson
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

count_parquet       = 0
# source              = "/home/hadoop/virtual/data/dataset/arnet/citation/dblpv13.json"
source              = "/home/howard/recsys/dataset/dblpv13.json"
save_prefix         = "gs://clpub/data_lake/arnet/tables/published_history/parts/"
INSERT_THRESHOLD    = 300000

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

with open(source, "r") as test_read:
    list_json = []
    map_data = {}
    current_json = ""

    def init_list():
        global list_json
        global map_data
        del list_json
        del map_data

        list_json = []
        map_data = {}

    def init_current():
        global current_json
        del current_json
        
        current_json = ""

    def insert_df(dept):
        global count_parquet

        deptSchema = StructType([
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

        deptDF = spark.createDataFrame(data=dept, schema = deptSchema)
        deptDF.write.mode("overwrite").parquet(f"{save_prefix}part-{count_parquet}")
        count_parquet += 1

    def insert_data(list_data):
        cached_uuid = {}

        for cid, chunk in enumerate(chunks(list_data, INSERT_THRESHOLD)):

            composed_data = []
            flag = False
            for record in chunk:
                uid = str(uuid.uuid1())

                composed_data.append((uid, 0, 0, \
                    record["author_id"], record["author_name"], record["author_org"], \
                    record["paper_id"], record["paper_title"], record["year"]))

                print(f"[ ===== Checked Pair Author-Paper: ({record['author_id']}, {record['paper_id']}) ===== ]")
                flag = True

            if flag == False:
                print(f"[ ===== [Chunk {cid + 1}] NO Pair Author-Paper Inserted ===== ]")
                continue

            print(f"[ ===== [Chunk {cid + 1}] BATCH Pair Author-Paper Inserting ... ===== ]")

            insert_df(composed_data)

            print(f"[ ===== [Chunk {cid + 1}] BATCH Pair Author-Paper Inserted ... ===== ]")

    # State = 0 -> searching start json
    # State = 1 -> searching end json
    state = 0
    for i, line in enumerate(test_read):
        if state == 0: 
            if line[0] == "{": state = 1
        if state == 1:
            if line[0] == "}":
                data = orjson.loads(current_json + "}")
                
                if 'title' in data and '_id' in data:
                    author_data = {}
                    if 'authors' in data:
                        author_data = data['authors']
                    
                        year = -1.0
                        if 'year' in data:
                            year = float(data['year'])

                        for author in author_data:
                            if 'name' not in author or '_id' not in author:
                                continue

                            author_org = ""
                            if "org" in author:
                                author_org = author["org"]

                            list_json.append({
                                "_id": "", "_status": 0, "_order": 0,
                                "author_id"     : author["_id"],
                                "author_name"   : author["name"],
                                "author_org"    : author_org,
                                "paper_id"      : data["_id"],
                                "paper_title"   : data["title"],
                                "year"          : year
                            })

                            print(f"[ ===== Pair Author-Paper batch count: {len(list_json)} ===== ]")

                if len(list_json) >= INSERT_THRESHOLD:
                    insert_data(list_json)
                    init_list()
                
                init_current()
                state = 0

        if state == 0:
            if line[1] == "{":
                line = line[1:]

            if line[0] == "{": state = 1

        if state == 1:
            if line.find("NumberInt(") != -1:
                line = line.replace("NumberInt(", "").replace(")", "")
            current_json += line

    insert_data(list_json)
