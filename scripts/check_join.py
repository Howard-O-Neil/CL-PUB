from pprint import pprint
import numpy as np
import pandas as pd

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

userSchema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
])
user_data = [
    ("1", "alisa"),
    ("1", "doom"),
    ("2", "grande"),
    ("3", "alex"),
    ("4", "john"),
    ("5", "mia")
]
user_df = spark.createDataFrame(data=user_data, schema=userSchema)
user_df.createOrReplaceTempView("user")

userBuySchema = StructType([
    StructField("user_id", StringType(), False),
    StructField("item_id", StringType(), False),
])
userBuy_data = [
    ("1", "2"),
    ("1", "4"),
    ("3", "5"),
    ("6", "4")
]
userBuy_df = spark.createDataFrame(data=userBuy_data, schema=userBuySchema)
userBuy_df.createOrReplaceTempView("userBuy")

spark.sql("""
select *
from user as u left join userBuy as ub on ub.user_id = u.id 
""").show()

spark.sql("""
select *
from user as u left join userBuy as ub on u.id = ub.user_id
""").toPandas().to_csv("./out1.csv", index=False)

spark.sql("""
select *
from user as u inner join userBuy as ub on u.id = ub.user_id
""").show() 
# .toPandas().to_csv("./out2.csv", index=False)
