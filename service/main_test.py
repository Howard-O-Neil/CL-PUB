import os
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/temurin-8-jdk-amd64"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

from fastapi import FastAPI
import numpy as np
import tensorflow as tf

from pyhive import trino

trino_conn = trino.Connection(host="localhost", port='8098', catalog='hive', schema='default', protocol='http')
trino_cursor = trino_conn.cursor()

from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://127.0.0.1",
    "http://127.0.0.1:3000",
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"res": "hello world"}

@app.get("/search_author")
async def search_author(name: str):
    low_name = name.lower()

    trino_cursor.execute(f"""
        SELECT author_id, author_name
        FROM author_feature
        WHERE LOWER(author_name) LIKE '%{low_name}%'
        LIMIT 200
    """)
    rows = trino_cursor.fetchall()

    return {
        "result": [{
            "author_id": r[0],
            "author_name": r[1]
        } for r in rows]
    }
