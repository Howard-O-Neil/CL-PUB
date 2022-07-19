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
                .getOrCreate()

spark = create_spark_session()

def load_model_re4():
    def model_creator(config):
        x_inputs = tf.keras.Input(shape=(3,))

        initializer = tf.keras.initializers.HeNormal()
        regularizer = tf.keras.regularizers.L2(0.00005)

        linear1 = tf.keras.layers.Dense(units=32, \
            activation='relu', \
            kernel_initializer=initializer, \
            bias_initializer=initializer, \
            kernel_regularizer=regularizer, \
            bias_regularizer=regularizer)

        linear2 = tf.keras.layers.Dense(units=32, \
            activation='relu', \
            kernel_initializer=initializer, \
            bias_initializer=initializer, \
            kernel_regularizer=regularizer, \
            bias_regularizer=regularizer)

        SVM_layer = tf.keras.layers.Dense(units=16, \
            kernel_initializer=initializer, \
            bias_initializer=initializer, \
            kernel_regularizer=regularizer, \
            bias_regularizer=regularizer)

        def SVM_linear_loss(y_true, y_pred):
            loss_t = tf.math.maximum( \
                0., \
                tf.math.subtract(1., tf.math.multiply(tf.cast(y_true, tf.float32), y_pred))) \
            
            return tf.math.reduce_mean(loss_t)
        
        def SVM_binary_metric(y_true, y_pred):
            pos     = tf.ones(tf.shape(y_true))
            neg     = tf.math.multiply(pos, tf.constant(-1.))

            pred_label = tf.where(
                tf.math.less_equal(tf.expand_dims(tf.reduce_mean(y_pred, 1), axis=1), tf.constant(0.)), 
                neg, pos)

            return tf.math.divide(
                tf.math.reduce_sum(tf.cast(tf.math.equal(pred_label, tf.cast(y_true, tf.float32)), tf.int32)),
                tf.gather(tf.shape(y_true), 0)
            )

        ml_outputs = SVM_layer(linear2(linear1(x_inputs)))
        model = tf.keras.Model(inputs=x_inputs, outputs=ml_outputs)

        optim = tf.keras.optimizers.Adam(learning_rate=0.0001)
        model.compile(optimizer=optim, loss=SVM_linear_loss, metrics=[SVM_binary_metric])

        return model

    model = model_creator(None)
    model.load_weights("/home/howard/recsys/model/re4/model.h5")

    return model

ranker = load_model_re4()

user_features = spark.read.parquet("gs://clpub/data_lake/arnet/tables/user_feature/re4/merge-0").limit(100).collect()

user_id = [r["author_id"] for r in user_features]
user_name = [r["author_name"] for r in user_features]
user_content_vect = [r["feature"] for r in user_features]
user_ranking = [r["ranking"] for r in user_features]
user_org_rank = [r["org_rank"] for r in user_features]

user_content_vect_float = [list(map(lambda x: float(x), r.split(";"))) for r in user_content_vect]

np_user_content_full = np.array(user_content_vect_float).astype(np.float32) 
np_user_content = np.expand_dims(
    np.mean(np.array(user_content_vect_float).astype(np.float32), axis=1), axis=1
)
np_ranking = np.expand_dims(np.array(user_ranking).astype(np.float32), axis=1)
np_org_rank = np.expand_dims(np.array(user_org_rank).astype(np.float32), axis=1)

np_features = np.hstack((np.hstack((np_user_content, np_ranking)), np_org_rank))

import faiss
from scipy.spatial import distance as sci_distance

index = faiss.index_factory(3, "L2norm,IVF1,Flat", faiss.METRIC_INNER_PRODUCT)

index.train(np_features)
index.add(np_features)

distance, idx = index.search(np.array([np_features[5]]), 100)

# Please squeeze first dimension of list_idx like below before passing
# np.squeeze(idx, axis=0)
def cal_collab_ranking(user_idx, list_idx):
    vect_prox = []
    for vect in np_user_content_full[list_idx].tolist():
        user_vect = np_user_content_full[user_idx].tolist()
        cos_dis = np.float32(sci_distance.cosine(user_vect, vect)).astype(float).item()
        vect_prox.append(cos_dis)

    ranking_prox = []
    for rank in np_ranking[list_idx].tolist():
        user_rank = np_ranking[user_idx].item()
        proximity = abs(user_rank - rank) \
            / max(abs(user_rank), abs(rank))

        ranking.append(proximity)

    org_rank_prox = []
    for org_rank in np_org_rank[list_idx].tolist():
        user_org_rank = np_org_rank[user_idx].item()
        proximity = 1
        if user_org_rank > 0 or org_rank > 0:
            proximity = abs(user_org_rank - org_rank) \
                / max(abs(user_org_rank), abs(org_rank))

        org_rank_prox.append(proximity)

    np_vect_prox = np.expand_dims(np.array(vect_prox).astype(np.float32), axis=1)
    np_ranking_prox = np.expand_dims(np.array(ranking_prox).astype(np.float32), axis=1)
    np_org_rank_prox = np.expand_dims(np.array(org_rank_prox).astype(np.float32), axis=1)

    predict_feature = np.hstack((np.hstack((np_vect_prox, np_ranking_prox)), np_org_rank_prox))

    collab_rank = tf.reduce_mean(ranker(predict_feature), 1).numpy()

    idx_sort = np.flip(np.argsort(collab_rank.flatten()), 0)

    return np.hstack((
        np.expand_dims(list_idx, axis=1),
        np.expand_dims(collab_rank, axis=1)
    ))[idx_sort]

app = FastAPI()

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
