import sys

from pprint import pprint

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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

import copy
import uuid

spark = (pyspark.sql.SparkSession.builder.getOrCreate())

published_history_dir   = "s3://recsys-bucket/data_lake/arnet/tables/published_history/merge-0"
# append H size to the end
dst_dir                 = "s3://recsys-bucket/data_lake/arnet/tables/paper_vect/merge-"

optimized_partition = 50000

publishSchema = StructType([
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

published_history_df = spark.read.schema(publishSchema).parquet(published_history_dir)
published_history_df.createOrReplaceTempView("published_history_df")

paper_df = spark.sql("""
    select phd.paper_id as id, phd.paper_title as sentence
    from published_history_df as phd 
        inner join (
            select first_value(phd2._id) as _id
            from published_history_df as phd2
            group by phd2.paper_id
        ) as group_paper on phd._id = group_paper._id
""")

paper_df.repartition(optimized_partition).write.mode("overwrite").parquet("hdfs:///recsys/temp/paper_df")

# ===== DAG BREAK =====

paper_vect_schema = StructType([
    StructField('id', StringType(), False),
    StructField('sentence', StringType(), False),
])
paper_read_df = spark.read.schema(paper_vect_schema).parquet("hdfs:///recsys/temp/paper_df")

# paper_df.show(vertical=True)

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover

# Tokenize
tokenizer   = Tokenizer(inputCol="sentence", outputCol="words")
wordsData   = tokenizer.transform(paper_read_df)

# Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filterWords = remover.transform(wordsData)

# filterWords.show(vertical=True, truncate=1000)

# explode_words = filterWords.select(sparkf.col("id"), sparkf.explode(sparkf.col("filtered")).alias("word"))
# explode_words.createOrReplaceTempView("explode_word")

# explode_words.show(vertical=True)

# # vocab_size approximate 10^6
# vocab_size = spark.sql("""
#     select count(distinct word) as vocab_size
#     from explode_word
# """).collect()[0]['vocab_size']

max_vocab_size = 1000
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=max_vocab_size)
featurizedData = hashingTF.transform(filterWords)

# featurizedData.show(vertical=True, truncate=1000)

import torch
import torch.nn as nn
import numpy as np
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import pandas_udf, PandasUDFType

def avg_pooling_1D(partitionData):
    list_vect   = []
    list_id     = []
    for row in partitionData:
        list_vect.append(row.rawFeatures)
        list_id.append(row.id) \

    num_feature = max_vocab_size
    out_dimen   = 64
    out_str     = 15
    pool_s      = (num_feature + 1) - (out_dimen * out_str)
    avg_pool_1d = nn.AvgPool1d(pool_s, stride=out_str) \

    list_np     = np.array(list_vect)
    num_batch   = list_np.shape[0] \

    if len(list_np.shape) == 1:
        list_np = np.expand_dims(list_np, axis=0) \

    feature_tf  = torch.tensor(list_np, dtype=torch.float32) \

    list_pooling = torch.reshape( \
        avg_pool_1d(torch.reshape(feature_tf, (num_batch, 1, list_np.shape[1]))), (num_batch, out_dimen) \
    ).numpy().astype(float) \

    list_res = []
    for i, pooling in enumerate(list_pooling.tolist()):
        list_res.append([list_id[i], ";".join(list(map(str, pooling)))]) \

    return iter(list_res)

pooling_schema = StructType([
    StructField("id", StringType(), False),
    StructField("feature", StringType(), False),
])
pooling_data = spark.createDataFrame(
    featurizedData.rdd.repartition(optimized_partition).mapPartitions(avg_pooling_1D), pooling_schema)
pooling_data.write.mode("overwrite").parquet(dst_dir + "Hash-1000")

repartition_dir = "s3://recsys-bucket/data_lake/arnet/tables/paper_vect/repartition-"
repartition_pooling = spark.read.schema(pooling_schema).parquet(dst_dir + "Hash-1000")
repartition_pooling.repartition(120).write.parquet(repartition_dir + "Hash-1000")

