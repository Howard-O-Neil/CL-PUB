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
source              = "/recsys/dataset/arnet/citation/dblpv13.json"
save_prefix         = "/home/hadoop/spark/arnet/tables/coauthor/parts/"
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

        deptDF = spark.createDataFrame(data=dept, schema = deptSchema)
        deptDF.write.format("parquet").save(f"{save_prefix}part-{count_parquet}")
        count_parquet += 1

    def insert_data(list_data):
        cached_uuid = {}

        for cid, chunk in enumerate(chunks(list_data, len(list_data))):

            composed_data = []
            flag = False
            for record in chunk:
                uid = str(uuid.uuid1())

                composed_data.append((uid, 0, 0, \
                    record['paper_id'], \
                    record['paper_title'], \
                    record['author1_id'], \
                    record['author1_name'], \
                    record['author1_org'], \
                    record['author2_id'], \
                    record['author2_name'], \
                    record['author2_org'], \
                    record['year']))

                print(f"[ ===== Checked coauthor: ({record['author1_id']}, {record['author2_id']}) ===== ]")
                flag = True

            if flag == False:
                print(f"[ ===== [Chunk {cid + 1}] NO coauthor Inserted ===== ]")
                continue

            print(f"[ ===== [Chunk {cid + 1}] BATCH coauthor Inserting ... ===== ]")

            insert_df(composed_data)

            print(f"[ ===== [Chunk {cid + 1}] BATCH coauthor Inserted ... ===== ]")

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

                            if len(author_data) <= 1:
                                list_json.append({
                                    "_id": "", "_status": 0, "_order": 0,
                                    "paper_id"          : data["_id"],
                                    "paper_title"       : data["title"],
                                    "author1_id"        : author["_id"],
                                    "author1_name"      : author["name"],
                                    "author1_org"       : author_org,
                                    "author2_id"        : "",
                                    "author2_name"      : "",
                                    "author2_org"       : "",
                                    "year"              : year,
                                })
                                print(f"[ ===== Coauthor batch count: {len(list_json)} ===== ]")
                                break

                            for other_author in author_data:
                                if 'name' not in other_author or '_id' not in other_author:
                                    continue
                                if other_author["_id"] == author["_id"]:
                                    continue

                                other_author_org = ""
                                if "org" in other_author:
                                    other_author_org = other_author["org"]

                                list_json.append({
                                    "_id": "", "_status": 0, "_order": 0,
                                    "paper_id"          : data["_id"],
                                    "paper_title"       : data["title"],
                                    "author1_id"        : author["_id"],
                                    "author1_name"      : author["name"],
                                    "author1_org"       : author_org,
                                    "author2_id"        : other_author["_id"],
                                    "author2_name"      : other_author["name"],
                                    "author2_org"       : other_author_org,
                                    "year"              : year,
                                })
                                print(f"[ ===== Coauthor batch count: {len(list_json)} ===== ]")

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
