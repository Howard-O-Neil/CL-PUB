import sys

sys.path.append("../..")

from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
from tensorflow import keras

import numpy as np

def count(stop):
  i = 0
  while i<stop:
    yield i
    i += 1

ds_counter = tf.data.Dataset.from_generator(count, args=[25], output_types=tf.int32, output_shapes = (), )

v = tf.Variable(1, shape=tf.TensorShape(None))
ds_counter = ds_counter.map(lambda x: tf.add(x, v))

for count_batch in ds_counter.batch(10).take(3):
  print(count_batch.numpy())

v.assign(2)

for count_batch in ds_counter.batch(10).take(3):
  print(count_batch.numpy())
