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

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

count_parquet       = 0
# source              = "/home/hadoop/virtual/data/dataset/arnet/citation/dblpv13.json"
source              = "/recsys/dataset/arnet/citation/dblpv13.json"
save_prefix         = "/home/hadoop/spark/arnet/tables/citation/parts/"
INSERT_THRESHOLD    = 300000

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
            StructField('author_id', StringType(), False),
            StructField('author_name', StringType(), False),
            StructField('author_org', StringType(), False),
            StructField('cite', StringType(), False),
            StructField('year', FloatType(), False),
        ])

        deptDF = spark.createDataFrame(data=dept, schema = deptSchema)
        deptDF.write.format("parquet").save(f"{save_prefix}part-{count_parquet}")
        count_parquet += 1


    def insert_data(list_data):
        for cid, chunk in enumerate(chunks(list_data, len(list_data))):
            composed_data = []
            flag = False

            for record in chunk:
                uid = str(uuid.uuid1())

                composed_data.append((uid, 0, 0, \
                   record['paper_id'], \
                   record['paper_title'], \
                   record['author_id'], \
                   record['author_name'], \
                   record['author_org'], \
                   record['cite'], \
                   record['year']))
                
                # print(f"[ ===== Checked citation: ({record['author_id']}, {record['cite']}) ===== ]")
                flag = True
            
            if flag == False:
                print(f"[ ===== [Chunk {cid + 1}] NO citation Inserted ===== ]")
                continue
            
            print(f"[ ===== [Chunk {cid + 1}] BATCH citation Inserting ... ===== ]")

            insert_df(composed_data)

            print(f"[ ===== [Chunk {cid + 1}] BATCH citation Inserted ... ===== ]")


    # State = 0 -> searching start json
    # State = 1 -> searching end json
    state = 0

    for i, line in enumerate(test_read):
        if state == 0: 
            if line[0] == "{": state = 1
        if state == 1:
            if line[0] == "}":
                data = orjson.loads(current_json + "}")

                if "title" in data and "_id" in data:
                    year = -1.0
                    if 'year' in data:
                        year = float(data['year'])

                    if "references" in data:
                        refs = data["references"]

                        for ref in refs:
                            if 'authors' in data:
                                author_data = data['authors']

                                for author in author_data:
                                    if 'name' not in author or '_id' not in author:
                                        continue

                                    author_org = ""
                                    if "org" in author:
                                        author_org = author["org"]

                                    list_json.append({
                                        "_id": "", "_status": 0, "_order": 0,
                                        "paper_id"      : data["_id"],
                                        "paper_title"   : data["title"],
                                        "author_id"     : author["_id"],
                                        "author_name"   : author["name"],
                                        "author_org"    : author_org,
                                        "cite"          : ref,
                                        "year"          : year
                                    })

                                    print(f"[ ===== Pair Author-Cite batch count: {len(list_json)} ===== ]")


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