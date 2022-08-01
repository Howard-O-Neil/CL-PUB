import os
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as sparkf

coauthor_dir        = "gs://clpub/data_lake/arnet/tables/coauthor/merge-0"
testing_dir         = "gs://clpub/data_lake/arnet/tables/testing/merge-0"

spark_conf = SparkConf()

emr_conf_f = open("/home/hadoop/emr_env.txt")
conf_lines = emr_conf_f.readlines()

for conf in conf_lines:
    conf_set = conf.strip().split(";")
    spark_conf.set(conf_set[0], conf_set[1])

# .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "3600s") \
# .config("spark.scheduler.minRegisteredResourcesRatio", "1.0") \
spark = SparkSession.builder \
            .appName("AI model") \
            .config(conf=spark_conf) \
            .config("spark.executor.memory", "5g") \
            .getOrCreate()


import tensorflow as tf
import numpy as np

def model_creator(config):
    x_inputs = tf.keras.Input(shape=(4,))

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

model.load_weights("/home/hadoop/model/re5/model.h5")

print("\n\n===== EVALUTING MODEL... =====\n\n")

def cal_test_df():
    coauthor_dir    = "gs://clpub/data_lake/arnet/tables/coauthor/merge-0"

    coauthor_schema = StructType([
        StructField('_id', StringType(), False),
        StructField('_status', IntegerType(), False),
        StructField('_order', IntegerType(), False),
        StructField('paper_id', StringType(), False),
        StructField('paper_title', StringType(), False),
        StructField('author1_id', StringType(), False),
        StructField('author1_name', StringType(), False),
        StructField('author1_org', StringType(), False),
        StructField('author2_id', StringType(), False),
        StructField('author2_name', StringType(), False),
        StructField('author2_org', StringType(), False),
        StructField('year', FloatType(), False),
    ])
    coauthor_df = spark.read.schema(coauthor_schema).parquet(coauthor_dir) \
                    .filter((sparkf.col("author1_id") != "") & (sparkf.col("author2_id") != "")) \

    coauthor_df.createOrReplaceTempView("coauth_df")

    coauthor_output_df = spark.sql("""
        select author1_id as author1, author2_id as author2, count(_id) as collab
        from coauth_df
        group by author1_id, author2_id
    """)
    coauthor_output_df.createOrReplaceTempView("coauthor_output_df")

    spark.read.parquet(testing_dir).createOrReplaceTempView("test_view")

    return spark.sql("""
        select tv.cos_dist as f1, tv.org_rank_proximity as f2, tv.rwr_bias_proximity as f3, tv.freq_proximity as f4, cast(tv.label as int) as label,
            cast((CASE WHEN cod.author1 IS NULL THEN 0 ELSE cod.collab END) as int) as collab
        from test_view as tv 
            left join coauthor_output_df as cod on cod.author1 = tv.author1 and cod.author2 = tv.author2
    """)

test_samples_bigdl = cal_test_df()
test_sample_np = np.array(test_samples_bigdl.collect())
test_sample_feature = test_sample_np[:, 0:4]
test_sample_label   = test_sample_np[:, 4:5]
test_sample_collab  = test_sample_np[:, 5:6]

def cal_binary_acc():
    acc = tf.keras.metrics.BinaryAccuracy(dtype=None, threshold=0.0)
    acc.update_state(test_sample_label, 
        np.expand_dims(tf.reduce_mean(model(test_sample_feature), 1).numpy(), axis=1))

    return acc.result().numpy()

def cal_precision():
    predictions = tf.reduce_mean(model(test_sample_feature), 1).numpy()
    norm_pred = (predictions - np.min(predictions)) / (np.max(predictions) - np.min(predictions))

    thresholds_range = np.linspace(0.1, 0.9, num=200)

    acc = tf.keras.metrics.Precision(thresholds=thresholds_range.tolist())
    acc.update_state(test_sample_label,
        np.expand_dims(norm_pred, axis=1))

    return np.mean(acc.result().numpy())

def cal_recall():
    predictions = tf.reduce_mean(model(test_sample_feature), 1).numpy()
    norm_pred = (predictions - np.min(predictions)) / (np.max(predictions) - np.min(predictions))

    thresholds_range = np.linspace(0.1, 0.9, num=200)

    acc = tf.keras.metrics.Recall(thresholds=thresholds_range.tolist())
    acc.update_state(test_sample_label,
        np.expand_dims(norm_pred, axis=1))

    return np.mean(acc.result().numpy())

def cal_AUC():
    predictions = tf.reduce_mean(model(test_sample_feature), 1).numpy()
    norm_pred = (predictions - np.min(predictions)) / (np.max(predictions) - np.min(predictions))

    acc = tf.keras.metrics.AUC(num_thresholds=200)
    acc.update_state(test_sample_label,
        np.expand_dims(norm_pred, axis=1))

    return acc.result().numpy()

def cal_collab_quality_k(k=1):
    predictions = tf.reduce_mean(model(test_sample_feature), 1).numpy()
    norm_pred = (predictions - np.min(predictions)) / (np.max(predictions) - np.min(predictions))

    norm_flat = norm_pred.flatten()
    idx_sort  = np.flip(np.argsort(norm_flat), 0)[0:k]

    sorted_pred = norm_pred[idx_sort]
    sorted_collab = test_sample_collab[idx_sort].flatten()

    return np.sum(np.multiply(sorted_pred, sorted_collab))

import pandas
data = [
    ["BINARY_ACCURACY", cal_binary_acc()],
    ["PRECESION", cal_precision()],
    ["RECALL", cal_recall()],
    ["AUC", cal_AUC()],
    ["CQ@1", cal_collab_quality_k(1)],
    ["CQ@5", cal_collab_quality_k(5)],
    ["CQ@10", cal_collab_quality_k(10)],
    ["CQ@50", cal_collab_quality_k(50)],
    ["CQ@100", cal_collab_quality_k(100)],
]

df = pandas.DataFrame(data)

print(df.to_string())

df.to_csv("/home/hadoop/model/re5/eval.csv")
