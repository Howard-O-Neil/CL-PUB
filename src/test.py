import tensorflow_core as tf
from matplotlib import pyplot as plt
import numpy as np
import sys

sess = tf.Session()
init = (tf.global_variables_initializer())
sess.run(init)

t1 = tf.constant([
    [1, 2, 3],
])

t2 = tf.constant([
    [-1, -2, -3],
    [-1, -2, -3],
    [-1, -2, -3],
    [-1, -2, -3],
])

print(sess.run(
    tf.subtract(t1, t2)
))