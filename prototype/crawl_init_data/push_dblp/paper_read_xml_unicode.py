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

available_entries = ["article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis", "www"]
available_field = ["author","editor","title","booktitle","pages","year","address","journal","volume","number","month","url","ee","cdrom","cite","publisher","note","crossref","isbn","series","school","chapter","publnr"]

object_field = ["id", "entry", "author", "title", "year", "month", "url", "ee", "address", "journal", "isbn"]

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
        "month"     : -1,
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
            "month"     : -1,
            "url"       : [],
            "ee"        : [],
            "address"   : [],
            "journal"   : "",
            "isbn"      : ""
        }

    def data_handle(data):
        pass

    # state = 0 -> search start entry
    # state = 1 -> search end entry
    state = 0
    open_entry = ""
    for i, line in enumerate(test_read):
        if state == 0:
            _t = check_tag(available_entries, line, "o")
            if _t != "":
                print(f"Open {_t}")
                open_entry = _t
                state = 1
                init()
        if state == 1:
            _t = check_tag(available_entries, line, "c")
            if _t != "" and _t == open_entry:
                print(f"Close {_t}")
                json_str = json.dumps(current_data, indent=4)
                print(json_str)
                init()
                open_entry = ""
                state = 0
        if state == 0:
            _t = check_tag(available_entries, line, "o")
            if _t != "":
                print(f"Open {_t}")
                open_entry = _t
                state = 1
                init()
        
        if state == 1:
            _t = check_tag(object_field, line, "o")
            if _t != "" and _t in object_field:
                print(_t)
                res = re.findall(f"<{_t}>(.*?)</{_t}>", line)

                if len(res) > 0:
                    content = res[0]
                    if isinstance(current_data[_t], list):
                        current_data[_t].append(content)
                    elif isinstance(current_data[_t], int):
                        if content.lower() in month_index:
                            current_data[_t] = month_index[content.lower()]
                        else: current_data[_t] = int(content)
                    elif isinstance(current_data[_t], str):
                        current_data[_t] = content
         
        if i == 1000:
            break
