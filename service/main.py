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

def create_spark_session():
    spark_conf = SparkConf()

    emr_conf_f = open("/home/howard/dataproc_env_2.txt")
    conf_lines = emr_conf_f.readlines()

    for conf in conf_lines:
        conf_set = conf.strip().split(";")
        spark_conf.set(conf_set[0], conf_set[1])

    return SparkSession.builder \
                .appName("Clpub service") \
                .master("local[6]") \
                .config(conf=spark_conf) \
                .config("spark.executor.memory", "10g") \
                .getOrCreate()

spark = create_spark_session()
app = FastAPI()

spark.read.parquet("gs://clpub/data_lake/arnet/tables/coauthor/merge-0").createOrReplaceTempView("coauthor")
spark.read.parquet("gs://clpub/data_lake/arnet/tables/published_history/merge-0").createOrReplaceTempView("published_history")
spark.sql("""
    select first_value(author_id) as author_id, author_name
    from published_history
    group by author_name
""").createOrReplaceTempView("author")

import faiss
index = faiss.index_factory(3, "L2norm,IVF1,Flat", faiss.METRIC_INNER_PRODUCT)

spark.read.parquet("gs://clpub/data_lake/arnet/tables/author_vect/merge-0").createOrReplaceTempView("author_vect")
spark.read.parquet("gs://clpub/data_lake/arnet/tables/author_org_rank/merge-0").createOrReplaceTempView("author_org_rank")
spark.read.parquet("gs://clpub/data_lake/arnet/tables/author_rwr_bias/merge-0").createOrReplaceTempView("author_rwr_bias")
spark.sql("""
    select av.author_id, av.feature, arb.ranking, aor.org_rank
    from author_vect as av
        inner join author_org_rank as aor on av.author_id = aor.author_id
        inner join author_rwr_bias as arb on av.author_id = arb.author_id
    limit 1000
""")

index_input = 

@app.get("/")
async def root():
    res_rows = spark.sql("""
        select _id from coauthor
        limit 100
    """).collect()

    res = [r["_id"] for r in res_rows]
    
    return {"arr": res}

@app.get("/search_author")
async def search_author(name: str):
    res_rows = spark.sql(f"""
        select author_id, author_name
        from author
        where author_name like '%{name}%'
        limit 1000
    """).collect()

    res = [{
            "author_id": r["author_id"],
            "author_name": r["author_name"]
        } for r in res_rows]

    return {
        "result": res
    }
