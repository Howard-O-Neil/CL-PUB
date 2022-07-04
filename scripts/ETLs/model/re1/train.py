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

content_train_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/content_sample_train/merge-0"
content_test_dir = "s3://recsys-bucket-1/data_lake/arnet/tables/content_sample_test/merge-0"

train_hdfs_dir = "hdfs:///temp/recsys/train/re1"
test_hdfs_dir = "hdfs:///temp/recsys/test/re1"

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

import torch
import tensorflow as tf
import numpy as np

spark.read.parquet(train_hdfs_dir).createOrReplaceTempView("train_view")

train_samples_bigdl = spark.sql("""
    select tv.cos_dist as feature,
        cast((CASE WHEN tv.label = 0 THEN -1 ELSE 1 END) as int) as label
    from train_view as tv
""")

train_samples_np = np.array(train_samples_bigdl.collect())

spark.stop()

feature_np = train_samples_np[:, 0:1]
label_np = train_samples_np[:, 1:2]

SVM_threshold = 0.2

def model_creator(config):
    x_inputs = tf.keras.Input(shape=(1,))

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
            tf.math.less_equal(tf.expand_dims(tf.reduce_mean(y_pred, 1), axis=1), tf.constant(SVM_threshold)), 
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

batch_size = 32768

history_file = "/home/hadoop/model/re1/train.csv"
history_logger=tf.keras.callbacks.CSVLogger(history_file, separator=",", append=False)

history = model.fit(feature_np, label_np, batch_size=batch_size, epochs=25, shuffle=True, callbacks=[history_logger])

model.save_weights("/home/hadoop/model/re1/model.h5")

print("\n\n ===== DONED ===== \n\n")

