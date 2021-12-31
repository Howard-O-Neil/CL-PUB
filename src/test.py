import tensorflow_core as tf
import matplotlib.pyplot as plt
import numpy as np

sess = tf.Session()
init = tf.global_variables_initializer()
sess.run(init)

a = tf.constant([1., 2.])
b = tf.constant([3., 4.])

print(sess.run(tf.sqrt(tf.square(tf.subtract(a, b)))))