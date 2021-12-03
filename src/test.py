import tensorflow_core as tf
import numpy as np
import matplotlib.pyplot as plt

a = tf.constant([[[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]]])
b = tf.constant([[[1, 2, 3, 4]], [[0, 0, 0, 0]]])

sess = tf.Session()
init = (tf.global_variables_initializer(), tf.local_variables_initializer())
sess.run(init)

a = tf.add(a, 1)
print("====================")
print(sess.run(tf.squeeze(a)))

res = tf.subtract(a, b)
sess.run(a)
sess.run(b)

print("====================")
print(sess.run(res))

print("====================")

print(sess.run(tf.reduce_sum(res, 2)))

print("====================")

print(sess.run(tf.argmin(tf.reduce_sum(res, 2), 0)))

print("====================")

print(sess.run(tf.unsorted_segment_sum(
    tf.squeeze(a), tf.argmin(tf.reduce_sum(res, 2), 0), 3
)))

print("another problem")
print("====================")

c = tf.constant([[1,2,3,4], [5,6,7,8], [4,3,2,1]])
print(sess.run(tf.math.unsorted_segment_sum(c, tf.constant([0, 1, 2]), num_segments=3)))
# ==> [[ 5, 5, 5, 5],
#       [5, 6, 7, 8]]