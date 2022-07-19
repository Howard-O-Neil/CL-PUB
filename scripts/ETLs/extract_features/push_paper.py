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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType

import copy
import uuid

count_parquet       = 0
# source              = "/home/hadoop/virtual/data/dataset/arnet/citation/dblpv13.json"
source              = "/home/howard/recsys/dataset/dblpv13.json"
save_prefix         = "gs://clpub/data_lake/arnet/tables/papers/parts/"
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
            StructField('title', StringType(), False),
            StructField('abstract', StringType(), False),
            StructField('authors_id', ArrayType(StringType(), False), False),
            StructField('authors_name', ArrayType(StringType(), False), False),
            StructField('authors_org', ArrayType(StringType(), False), False),
            StructField('year', FloatType(), False),
            StructField('venue_raw', StringType(), False),
            StructField('issn', StringType(), False),
            StructField('isbn', StringType(), False),
            StructField('doi', StringType(), False),
            StructField('pdf', StringType(), False),
            StructField('url', ArrayType(StringType(), False), False)
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
                    record["paper_id"], record["title"], record["abstract"], \
                    record["authors_id"], record["authors_name"], record["authors_org"], \
                    record["year"], record["venue_raw"], \
                    record["issn"], record["isbn"], \
                    record["doi"], record["pdf"], record["url"]))

                print(f"[ ===== Checked Paper: {record['paper_id']} ===== ]")
                flag = True

            if flag == False:
                print(f"[ ===== [Chunk {cid + 1}] NO Paper Inserted ===== ]")
                continue

            print(f"[ ===== [Chunk {cid + 1}] BATCH Paper Inserting ... ===== ]")

            insert_df(composed_data)

            print(f"[ ===== [Chunk {cid + 1}] BATCH Paper Inserted ... ===== ]")

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
                    
                    # Parse abstract
                    abstract = ""
                    if 'abstract' in data and data['abstract'] != None:
                        abstract = data['abstract']
                    
                    # Parse authors
                    authors_id = []
                    authors_name = []
                    authors_org = []
                    author_data = {}
                    if 'authors' in data:
                        author_data = data['authors']
                    
                        for author in author_data:
                            if 'name' not in author or '_id' not in author:
                                continue

                            author_org = ""
                            if "org" in author:
                                author_org = author["org"]

                            authors_id.append(author['_id'])
                            authors_name.append(author['name'])
                            authors_org.append(author_org)

                    # Parse year
                    year = -1.0
                    if 'year' in data:
                        year = float(data['year'])
                    
                    # Parse venue
                    venue_raw = ""
                    if 'venue' in data and data['venue'] != None \
                        and 'raw' in data['venue'] and data['venue']['raw'] != None:
                            venue_raw = data['venue']['raw']
                    
                    # Parse issn
                    issn = ""
                    if 'issn' in data and data['issn'] != None:
                        issn = data['issn']

                    # Parse isbn
                    isbn = ""
                    if 'isbn' in data and data['isbn'] != None:
                        isbn = data['isbn']

                    # Parse doi
                    doi = ""
                    if 'doi' in data and data['doi'] != None:
                        doi = data['doi']
                    
                    # Parse pdf
                    pdf = ""
                    if 'pdf' in data and data['pdf'] != None:
                        pdf = data['pdf']

                    # Parse url
                    url = []
                    if 'url' in data and data['url'] != None:
                        for _url in data['url']:
                            if _url != None and _url != "":
                                url.append(_url)

                    list_json.append({
                        "_id": "", "_status": 0, "_order": 0,
                        "paper_id": data["_id"],
                        "title": data["title"],
                        "abstract": abstract,
                        "authors_id": authors_id,
                        "authors_name": authors_name,
                        "authors_org": authors_org,
                        "year": year,
                        "venue_raw": venue_raw,
                        "issn": issn,
                        "isbn": isbn,
                        "doi": doi,
                        "pdf": pdf,
                        "url": url
                    })
                    print(f"[ ===== Paper batch count: {len(list_json)} ===== ]")

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
