import sys

from pprint import pprint

# each JSON is small, there's no need in iterative processing
import json
import sys
import xml
import time

import copy
import uuid

import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

import tensorflow as tf

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

published_history_dir   = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"
vocab_dir               = "/home/howard/recsys/vocab"
dst_dir                 = "gs://clpub/data_lake/arnet/tables/content_v2/paper_vect/merge-0"

spark.read.parquet(published_history_dir).createOrReplaceTempView("published_df")
paper_df = spark.sql("""
    select phd.paper_id as id, phd.paper_title as sentence
    from published_df as phd 
        inner join (
            select first_value(phd2._id) as _id
            from published_df as phd2
            group by phd2.paper_id
        ) as group_paper on phd._id = group_paper._id
""")

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

f = open(f"{vocab_dir}/merge-0.txt", "r")
unique_words = sc.broadcast(f.read().split("\n"))
f.close()

def create_layers(unique_words):
    vocab_dataset = tf.data.Dataset.from_tensor_slices(unique_words.value)

    vectorize_layer = tf.keras.layers.TextVectorization(
        pad_to_max_tokens=True,
        standardize='lower_and_strip_punctuation',
        split='whitespace',
        max_tokens=len(unique_words.value) + 1,
        output_mode='count')

    vectorize_layer.adapt(vocab_dataset.batch(2048))

    avg_layer = tf.keras.layers.AveragePooling1D(
        pool_size=8258,
        strides=8255)

    return vectorize_layer, avg_layer

def vectorize_title(_iter):
    vectorize_layer, avg_layer = create_layers(unique_words)

    list_title = []
    list_id = [] \

    for x in _iter:
        list_id.append(x["id"])
        list_title.append([ x["sentence"] ]) \

    list_res = [] \
    
    chunk_ids       = list(chunks(list_id, 1000))
    chunk_titles    = list(chunks(list_title, 1000)) \

    for chunk_idx in range(0, len(chunk_ids)):
        titles = chunk_titles[chunk_idx]

        text_bow_vect = vectorize_layer(titles)
        avg_vect = avg_layer(tf.reshape(text_bow_vect, [len(titles), tf.gather(tf.shape(text_bow_vect), 1), 1]))
        output_vect = (tf.reshape(avg_vect, [len(titles), tf.gather(tf.shape(avg_vect), 1)])).numpy().tolist() \

        for i in range(len(chunk_ids[chunk_idx])):
            list_res.append([chunk_ids[chunk_idx][i], ";".join(list(map(str, output_vect[i])))]) \
    
    return iter(list_res)

paper_vect_schema = StructType([
    StructField("id", StringType(), False),
    StructField("feature", StringType(), False),
])

paper_vect_df = spark.createDataFrame(
    paper_df.repartition(200).rdd.mapPartitions(vectorize_title), paper_vect_schema 
)

paper_vect_df.write.mode("overwrite").parquet(dst_dir)
