import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

import os
os.environ["JAVA_HOME"] = "/opt/corretto-8"
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"

JAVA_LIB = "/opt/corretto-8/lib"

# spark = SparkSession.builder \
#     .config("spark.app.name", "Recsys") \
#     .config("spark.master", "spark://recsys-sparkmaster1:7077") \
#     .getOrCreate()

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .getOrCreate()

df = spark.read.option("delimiter", ",") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/data/adult_data.csv")

print("===== Start =====")
df.printSchema()

print("===== Start =====")
df.printSchema()

print("===== Start =====")
df.crosstab('age', 'income').sort("age_income").show()

print("===== Start =====")
df.groupby('marital-status').agg({'capital-gain': 'mean'}).show()

