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

origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://127.0.0.1",
    "http://127.0.0.1:3000",
]

import numpy as np
import tensorflow as tf

indexing_scale = [0.0, 0.35, 0.95]

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

from pyhive import trino

trino_conn = trino.Connection(host="localhost", port='8098', catalog='hive', schema='default', protocol='http')
trino_cursor = trino_conn.cursor()

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

print("===== Collecting user features =====")

import pickle
pickle_dir = "/home/howard/recsys/faiss/re4"

import multiprocessing
from multiprocessing import freeze_support

freeze_support()

list_pickles = os.listdir(pickle_dir)

if "success.p" in list_pickles:
    print("=== Load from pickle ...")

    user_id                     = pickle.load(open(f"{pickle_dir}/user_id.p", "rb"))
    user_name                   = pickle.load(open(f"{pickle_dir}/user_name.p", "rb"))
    user_content_vect           = pickle.load(open(f"{pickle_dir}/user_content_vect.p", "rb"))
    user_ranking                = pickle.load(open(f"{pickle_dir}/user_ranking.p", "rb"))
    user_org_rank               = pickle.load(open(f"{pickle_dir}/user_org_rank.p", "rb"))
    user_content_vect_float     = pickle.load(open(f"{pickle_dir}/user_content_vect_float.p", "rb"))
else:
    print("=== Executing trino ...")

    trino_cursor.execute("""
        SELECT author_id, author_name, feature, ranking, org_rank
        FROM author_feature
    """)

    feature_fetch_res = trino_cursor.fetchall()

    # # This will cause very high ram usage, each numpy element has fixed equal allocated space
    # user_features = np.array(feature_fetch_res).astype(object)

    def parsing_feature_worker(idx, fetch, manager_dict):
        if idx <= 2:
            manager_dict[str(idx)] = [r[idx] for r in fetch] 
        else:
            manager_dict[str(idx)] = [float(r[idx]) for r in fetch]

    worker_manager = multiprocessing.Manager()
    manager_dict = worker_manager.dict()

    jobs = []

    for i in range(5):
        p = multiprocessing.Process(\
            target=parsing_feature_worker,\
            args=(i, feature_fetch_res, manager_dict,))
        
        jobs.append(p)

    for i in range(0, 3): jobs[i].start()
    for i in range(0, 3): jobs[i].join()

    for i in range(3, 5): jobs[i].start()
    for i in range(3, 5): jobs[i].join()

    user_id             = manager_dict['0']
    user_name           = manager_dict['1']
    user_content_vect   = manager_dict['2']
    user_ranking        = manager_dict['3']
    user_org_rank       = manager_dict['4']

    user_content_vect_float = [
        [float(x1) for x1 in x.split(";")] for x in user_content_vect
    ]

    save_msg = {"msg": "SUCCESS"}

    pickle.dump(save_msg, open(f"{pickle_dir}/success.p", "wb"))
    pickle.dump(user_id, open(f"{pickle_dir}/user_id.p", "wb"))
    pickle.dump(user_name, open(f"{pickle_dir}/user_name.p", "wb"))
    pickle.dump(user_content_vect, open(f"{pickle_dir}/user_content_vect.p", "wb"))
    pickle.dump(user_ranking, open(f"{pickle_dir}/user_ranking.p", "wb"))
    pickle.dump(user_org_rank, open(f"{pickle_dir}/user_org_rank.p", "wb"))
    pickle.dump(user_content_vect_float, open(f"{pickle_dir}/user_content_vect_float.p", "wb"))


print("===== Done, user features collected! ===== \n")

print("===== Calculating indexing =====")

np_user_content = np.array(user_content_vect_float).astype(np.float32)
np_user_content_med = np.expand_dims(np.median(np_user_content, axis=1), axis=1)
np_ranking = np.expand_dims(np.array(user_ranking).astype(np.float32), axis=1)
np_org_rank = np.expand_dims(np.array(user_org_rank).astype(np.float32), axis=1)

from sklearn.preprocessing import QuantileTransformer

content_scaler  = QuantileTransformer()
ranking_scaler  = QuantileTransformer()
org_rank_scaler = QuantileTransformer()

content_scaler.fit(np_user_content_med)
ranking_scaler.fit(np_ranking)
org_rank_scaler.fit(np_org_rank)

np_features = np.hstack((
        np.hstack((
            np.multiply(content_scaler.transform(np_user_content_med), indexing_scale[0]),
            np.multiply(ranking_scaler.transform(np_ranking), indexing_scale[1]))),
        np.multiply(org_rank_scaler.transform(np_org_rank), indexing_scale[2]))).astype(np.float32)

import faiss
from faiss import index_factory
from scipy.spatial import distance as sci_distance

feature_dimen   = 3
num_cluster     = 100

# Search on 10% Database
num_search      = int(num_cluster * 0.1)

if "search.index" not in os.listdir(pickle_dir):
    print("=== Training index ...")

    # L2 Norm + Inner Product = Cosine
    index = index_factory(feature_dimen, f"L2norm,IVF{num_cluster},Flat,RFlat", faiss.METRIC_INNER_PRODUCT)

    index.train(np_features)
    index.add(np_features)

    faiss.write_index(index, f"{pickle_dir}/search.index")
else:
    print("=== Loading index ...")
    index = faiss.read_index(f"{pickle_dir}/search.index")

print("===== Done, index calculated! ===== \n")

index.nprobe = num_search

# # Ugly mock testing
# distance, idx = index.search(np.array([np_features[5]]), 100)

# Please squeeze first dimension of list_idx like below before passing
# new_idx, collab_rank = cal_collab_ranking(5, np.squeeze(idx, axis=0))
# user_idx = single integer
def cal_collab_ranking(user_feature, list_idx):
    vect_prox = []
    for vect in np_user_content[list_idx].tolist():
        # user_vect = np_user_content[user_idx].tolist()
        user_vect = user_feature[0]
        cos_dis = np.float32(sci_distance.cosine(user_vect, vect)).astype(float).item()
        vect_prox.append(cos_dis)

    ranking_prox = []
    for rank in np_ranking[list_idx].tolist():
        # user_rank = np_ranking[user_idx].item()
        user_rank = user_feature[1]
        proximity = abs(user_rank - rank[0]) \
            / max(abs(user_rank), abs(rank[0]))

        ranking_prox.append(proximity)

    org_rank_prox = []
    for org_rank in np_org_rank[list_idx].tolist():
        # user_org_rank = np_org_rank[user_idx].item()
        user_org_rank = user_feature[2]
        proximity = 1.
        if user_org_rank > 0 or org_rank[0] > 0:
            proximity = abs(user_org_rank - org_rank[0]) \
                / max(abs(user_org_rank), abs(org_rank[0]))

        org_rank_prox.append(proximity)

    np_vect_prox = np.expand_dims(np.array(vect_prox).astype(np.float32), axis=1)
    np_ranking_prox = np.expand_dims(np.array(ranking_prox).astype(np.float32), axis=1)
    np_org_rank_prox = np.expand_dims(np.array(org_rank_prox).astype(np.float32), axis=1)

    predict_feature = np.hstack((np.hstack((np_vect_prox, np_ranking_prox)), np_org_rank_prox))

    collab_rank = tf.reduce_mean(ranker(predict_feature), 1).numpy()

    idx_sort = np.flip(np.argsort(collab_rank.flatten()), 0)

    return list_idx[idx_sort], collab_rank[idx_sort]
