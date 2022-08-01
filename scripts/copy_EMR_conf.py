from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

spark   = SparkSession.builder.getOrCreate()
dst_dir = "/home/hadoop/emr_env.txt"

f = open(dst_dir, "w")
for config in spark.sparkContext.getConf().getAll():
    f.write(f"{config[0]};{config[1]}\n")

f.close()
