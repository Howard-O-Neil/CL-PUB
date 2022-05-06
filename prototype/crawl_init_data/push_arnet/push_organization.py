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

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .getOrCreate()

