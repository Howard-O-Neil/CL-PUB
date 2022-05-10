import sys

sys.path.append("../../..")

from pprint import pprint
import numpy as np
import pandas as pd
from prototype.crawl_init_data.increase_id import cal_next_id

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
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
    .getOrCreate()

count_parquet = 0

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

source = "/recsys/data/arnet/citation/dblpv13.json"

INSERT_THRESHOLD = 250000

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
            StructField('_timestamp', LongType(), False),
            StructField('id', StringType(), False),
            StructField('name', StringType(), False),
        ])

        deptDF = spark.createDataFrame(data=dept, schema = deptSchema)
        deptDF.write.format("parquet").save(f"/data/recsys/arnet/tables/author/production/part-{count_parquet}")
        count_parquet += 1

    def insert_data(list_data):
        cached_uuid = {}

        for cid, chunk in enumerate(chunks(list_data, INSERT_THRESHOLD)):

            composed_data = []
            flag = False
            for record in chunk:
                if 'name' not in record or '_id' not in record:
                    continue

                name = record['name']
                _id = record['_id']
                
                uid = str(uuid.uuid4())
                while uid in cached_uuid:
                    uid = str(uuid.uuid4())
                cached_uuid[uid] = True

                composed_data.append((uid, 0, int(time.time()), _id, name))

                print(f"[ ===== Checked author: {name} ===== ]")
                flag = True

            if flag == False:
                print(f"[ ===== [Chunk {cid + 1}] NO author Inserted ===== ]")
                continue

            print(f"[ ===== [Chunk {cid + 1}] BATCH author Inserting ... ===== ]")

            insert_df(composed_data)

            print(f"[ ===== [Chunk {cid + 1}] BATCH author Inserted ... ===== ]")

    # State = 0 -> searching start json
    # State = 1 -> searching end json
    state = 0
    for i, line in enumerate(test_read):
        if state == 0: 
            if line[0] == "{": state = 1
        if state == 1:
            if line[0] == "}":
                data = json.loads(current_json + "}")
                
                author_data = {}
                if 'authors' in data:
                    author_data = data['authors']
                
                for author in author_data:
                    list_json.append(author)

                print(f"[ ===== Author batch count: {len(list_json)} ===== ]")

                if len(list_json) >= INSERT_THRESHOLD:
                    insert_data(list_json)
                    init_list()
                
                init_current()
                state = 0

        if state == 0: 
            if line[0] == "{": state = 1

        if state == 1:
            if line.find("NumberInt(") != -1:
                line = line.replace("NumberInt(", "").replace(")", "")
            current_json += line

    insert_data(list_json)
