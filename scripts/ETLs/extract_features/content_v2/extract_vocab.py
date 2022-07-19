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

published_history_dir   = "gs://clpub/data_lake/arnet/tables/published_history/merge-0"
# published_history_draft_dir= "gs://clpub/data_lake/arnet/tables/published_history/draft-0"

dst_dir                     = "/home/howard/recsys/vocab"

spark.read.parquet(published_history_dir).createOrReplaceTempView("published_df")

paper_vect_df = spark.sql("""
    select phd.paper_id as id, phd.paper_title as sentence
    from published_df as phd 
        inner join (
            select first_value(phd2._id) as _id
            from published_df as phd2
            group by phd2.paper_id
        ) as group_paper on phd._id = group_paper._id
""")

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover

tokenizer   = Tokenizer(inputCol="sentence", outputCol="words")
words_data  = tokenizer.transform(paper_vect_df)

stopwords_remover   = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_words      = stopwords_remover.transform(words_data)

import string

def word_extract(_iter):
    words = [] \

    for x in _iter:
        for word in x["filtered"]:
            for w in word.split("-"):
                new_w = w.translate(str.maketrans('', '', string.punctuation))
                words.append(new_w) \

    words_content = "\n".join(words) \

    f = open(f"{dst_dir}/{str(uuid.uuid1())}.txt", "w+")
    f.write(words_content)
    f.close()

filtered_words.repartition(1000).rdd.foreachPartition(word_extract)
