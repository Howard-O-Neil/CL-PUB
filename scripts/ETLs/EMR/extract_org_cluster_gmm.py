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

from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.linalg import DenseVector
from pyspark.ml.evaluation import ClusteringEvaluator

org_rank_dir            = "s3://recsys-bucket/data_lake/arnet/tables/org_rank_algo/iter-14"
dst_dir                 = "s3://recsys-bucket/data_lake/arnet/tables/org_cluster/merge-0"

spark                   = (pyspark.sql.SparkSession.builder.getOrCreate())

org_node_schema = StructType([
    StructField("_1", FloatType(), False),
    StructField("_2", IntegerType(), False),
    StructField("_3", IntegerType(), False),
    StructField("_4", LongType(), False),
    StructField("_5", FloatType(), False),
])
org_rank_schema = StructType([
    StructField("id", LongType(), False),
    StructField("node", org_node_schema, False),
])

optimized_partition = 500
rank_df_writer = spark.read.schema(org_rank_schema).parquet(org_rank_dir)
rank_df_writer.repartition(optimized_partition).write.mode("overwrite").parquet("hdfs:///temp/recsys/org_rank")

org_rank_df     = spark.read.schema(org_rank_schema).parquet("/temp/recsys/org_rank").rdd
dataset_rdd     = org_rank_df.map(lambda x: (x["id"], DenseVector([x["node"]["_1"], x["node"]["_1"]]))).repartition(optimized_partition)
dataset         = spark.createDataFrame(dataset_rdd).withColumnRenamed("_1", "label").withColumnRenamed("_2", "features")

gmm_dataset = spark.read.format("libsvm").load("file:///home/hadoop/sample_kmeans_data.txt")
gmm = GaussianMixture(k=10, tol=0.0001, seed=10)
model = gmm.fit(dataset)

model.gaussiansDF.show()

# predictions = model.transform(dataset)

# evaluator = ClusteringEvaluator()

# silhouette = evaluator.evaluate(predictions)
# print("Silhouette with squared euclidean distance = " + str(silhouette))

# centers = model.clusterCenters()
