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

import copy
import uuid

month_index = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5, 'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12}

os.environ["JAVA_HOME"] = "/opt/corretto-8"
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
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
CREATE TABLE IF NOT EXISTS bibtex (
    _id LONG, _status INT, _timestamp LONG, 
    id LONG, 
    entry STRING,
    title STRING,
    year LONG, 
    month STRING, 
    journal STRING,
    isbn STRING)

    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/dblp/tables/bibtex/production/";
""")

# spark.sql("""
# select b.title
# from bibtex b
# where b.title in (
#     select * from values
#         ("An index-based joint multilingual / cross-lingual text categorization using topic expansion via BabelNet."),
#         ("Thyristor-based phase \\"hopping frequency\\" conversion technique."),
#         ("Line independency-based network / modelling for backward forward load flowanalysis of electrical power distribution systems.")
# )
# """).show()

# exit()

spark.sql("""
CREATE TABLE IF NOT EXISTS author (_id LONG, _status INT, _timestamp LONG, id LONG, name STRING, urls ARRAY<STRING>, affiliations ARRAY<STRING>)
    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/dblp/tables/author/production/";
""")

spark.sql("""
CREATE TABLE author_query
USING PARQUET
LOCATION "hdfs://128.0.5.3:9000/data/recsys/temps/tables/bibtex/author_query/"
as
Select /*+ COALESCE(8) */ * from author
""")

spark.sql("""
CREATE TABLE author_search
USING PARQUET
LOCATION "hdfs://128.0.5.3:9000/data/recsys/temps/tables/bibtex/author_search/"
as
select /*+ COALESCE(8) */ u._id, u.id, u.name
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
""")

source = "/recsys/data/dblp/dblp.xml"

available_entries = ["article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis", "www"]
available_field = ["author","editor","title","booktitle","pages","year","address","journal","volume","number","month","url","ee","cdrom","cite","publisher","note","crossref","isbn","series","school","chapter","publnr"]

object_field = ["id", "entry", "author", "title", "year", "month", "url", "ee", "address", "journal", "isbn"]

count_idx = "0" # call cal_next_id at the beginning, so id start with 1

def handle_double_quote(value):
    return value.replace('\\"','"').replace('"','\\"')

def get_unavailable_titles(data):
    if len(data) == 0:
        return {}

    query = ""
    for record in data:
        name = handle_double_quote(record['title'])
        query += f"""                   ("{name}"),\n"""
    
    query = query[:-2]
    res = spark.sql(f"""
        select title
        from bibtex
        where title in (
            select * from 
                values
                    {query}
        );
        """).collect()

    list_res = list(map(lambda x: x.asDict()["title"], res))

    # convert to dictionary for fast search
    return dict(zip(list_res, [True] * len(list_res)))

def get_author_ids(author_map):
    author_list = list(author_map.keys())

    if len(author_list) == 0:
        return {}

    query = ""
    for author in author_list:
        name = handle_double_quote(author)
        query += f"""                   ("{name}"),\n"""

    query = query[:-2]
    res = spark.sql(f"""
        select t.name, t.id
        from author_search as t
        where t.name in (
            select * from values
                {query}
            );
    """).collect()

    res = list(map(lambda x: x.asDict(), res))
    res_dict = {}

    for author in res:
        res_dict[author["name"]] = author["id"]

    return res_dict

def get_latest_id():
    res = spark.sql(f"""
            select a._id from bibtex a
            order by a._id desc
            limit 1
        """).collect() \

    res = list(map(lambda x: x.asDict(), res))
    if len(res) == 0:
        return 0
    return int(res[0]["_id"])

def get_latest_uid():
    res = spark.sql(f"""
            select a.id from bibtex a
            order by a.id desc
            limit 1
        """).collect() \

    res = list(map(lambda x: x.asDict(), res))
    if len(res) == 0:
        return 0
    return int(res[0]["id"])

def convert_array(arr, _type):
    if len(arr) == 0:
        return "array()"
    str_arr = "array( "
    for item in arr:
        if _type == "string":
            name = handle_double_quote(item)
            str_arr += f""" "{name}", """
        else: str_arr += f"{item}, "
    
    return str_arr[:-2] + ")"

# Split data into chunks
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def insert_bibtex(data, author_map): 
    for cid, chunk in enumerate(chunks(data, 10000)):
        new_id = get_latest_id() + 1
        new_uid = get_latest_uid() + 1

        unavai_names = get_unavailable_titles(chunk)

        chunk = list(filter(lambda x: x["title"] not in unavai_names, chunk))

        composed_insert = ""
        flag = False

        for record in chunk:
            entry = handle_double_quote(record['entry'])
            title = handle_double_quote(record['title'])
            year = record['year']
            month = handle_double_quote(record['month'])
            journal = handle_double_quote(record['journal'])
            isbn = handle_double_quote(record['isbn'])

            composed_insert += f"""       ({new_id}, 0, {int(time.time())}, {new_uid}, \
                "{entry}", "{title}", {year}, "{month}", \
                "{journal}", "{isbn}"),\n""" \

            print(f"[ ===== Checked bibtex: {title} ===== ]")

            new_id  += 1
            new_uid += 1
            flag    = True
        
        composed_insert = composed_insert[:-2] + ";\n"

        if flag == False:
            print(f"[ ===== [Chunk {cid}] NO bibtex Inserted ===== ]")
            continue


        print(f"[ ===== [Chunk {cid}] BATCH bibtex Inserting ... ===== ]")

        spark.sql(f"""
        INSERT INTO bibtex VALUES
        {composed_insert}
        """)

        print(f"[ ===== [Chunk {cid}] BATCH bibtex Inserted ===== ]")
    
    return

INSERT_THRESHOLD = 150000

list_data   = []
map_data    = {}
map_author  = {}

def check_tag(tag_arr, line, oc_state):
    for tag in tag_arr:
        if oc_state == "c":
            if line.find(f"/{tag}>") != -1:
                return tag
        else:
            if line.find(f"<{tag}") != -1:
                return tag
    return ""

import re

source = "/recsys/data/dblp/dblp.xml"

with open(source, "r") as test_read:
    current_data = {
        "id"        : 0,
        "entry"     : "",
        "author"    : [],
        "title"     : "",
        "year"      : -1,
        "month"     : "",
        "url"       : [],
        "ee"        : [],
        "address"   : [],
        "journal"   : "",
        "isbn"      : ""
    }
    def init():
        global current_data
        del current_data
        current_data = {
            "id"        : 0,
            "entry"     : "",
            "author"    : [],
            "title"     : "",
            "year"      : -1,
            "month"     : "",
            "url"       : [],
            "ee"        : [],
            "address"   : [],
            "journal"   : "",
            "isbn"      : ""
        }

    def data_handle(value):
        global list_data
        global map_data
        global map_author

        filter_cond = value["title"] not in map_data

        if filter_cond:
            list_data.append(value)
            map_data[value["title"]] = True
            
            for author in value["author"]:
                if author not in map_author:
                    map_author[author] = True
            print(f"[ ===== Added bibtex: {value['title']} ===== ]")
            print(f"[ ===== Bibtex batch count: {len(list_data)} ===== ]")


        if len(list_data) >= INSERT_THRESHOLD:
            insert_bibtex(list_data, map_author)
            list_data   = []
            map_data    = {}
            map_author  = {}

    # state = 0 -> search start entry
    # state = 1 -> search end entry
    state = 0
    open_entry = ""
    for i, line in enumerate(test_read):
        if state == 0:
            _t = check_tag(available_entries, line, "o")
            if _t != "":
                open_entry = _t
                state = 1
                init()
        if state == 1:
            _t = check_tag(available_entries, line, "c")
            if _t != "" and _t == open_entry:
                data_handle(copy.deepcopy(current_data))
                init()
                open_entry = ""
                state = 0
        if state == 0:
            _t = check_tag(available_entries, line, "o")
            if _t != "":
                open_entry = _t
                state = 1
                init()
        
        if state == 1:
            _t = check_tag(object_field, line, "o")
            if _t != "" and _t in object_field:
                res = re.findall(f"<{_t}>(.*?)</{_t}>", line)

                if len(res) > 0:
                    content = res[0]
                    if isinstance(current_data[_t], list):
                        current_data[_t].append(content)
                    elif isinstance(current_data[_t], int):
                        current_data[_t] = int(content)
                    elif isinstance(current_data[_t], str):
                        current_data[_t] = content


insert_bibtex(list_data, map_author)
