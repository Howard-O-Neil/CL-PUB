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
from pyspark.sql.functions import pandas_udf, PandasUDFType

import copy
import uuid

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

@pandas_udf("string", PandasUDFType.SCALAR)
def avg_pooling_1D(feature):
    feature_list    = list(map(lambda x: x.toArray(), feature.values.tolist()))
    feature_np      = np.array(feature_list) \

    feature_tf = tf.convert_to_tensor(feature_np, dtype=tf.float32)
    avg_pool_1d = tf.keras.layers.AveragePooling1D(pool_size=3, \
        strides=2 , padding='valid') \

    list_res = tf.reshape(
        avg_pool_1d(tf.reshape(feature_tf, [feature_np.shape[0], feature_np.shape[1], 1])), [9] \
    ).numpy().tolist() \

    return ';'.join(map(str, list_res))

paragraph = spark.createDataFrame([
    (1, "Hi I heard about Spark"),
    (1, "I hear you baby"),
    (2, "This is not cool"),
    (2, "EMR did you hear about that"),
    (3, "Hi, let's get nasty together"),
], ["id", "sentence"])
 
import tensorflow as tf
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.linalg import Vectors, VectorUDT

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(paragraph)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)

feature_res = featurizedData.select(sparkf.col("id"), avg_pooling_1D(sparkf.col("rawFeatures")).alias("feature")).show()

feature_res.write.mode("overwrite").parquet("/draft/merge-0")

read_schema = StructType([
    StructField("id", LongType(), False),
    StructField("feature", StringType(), False)
])

read_df = spark.read.schema(read_schema).parquet("/draft/merge-0")
read_df.show(truncate=200)

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import pyarrow as pa
import numpy as np

@pandas_udf("string", PandasUDFType.GROUPED_AGG)
def avg_vect_df(v: pd.DataFrame):
    list_form   = list(map(lambda x: list(map(lambda x: float(x), x.split(";"))), v.values.tolist()))
    list_np     = np.mean(np.array(list_form), axis=0)
    return ';'.join(map(str, list_np))

read_df.groupBy(sparkf.col("id")).agg(avg_vect_df(sparkf.col("feature")).alias("feature")).show(truncate=100)