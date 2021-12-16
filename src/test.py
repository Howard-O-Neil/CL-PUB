import tensorflow_core as tf
import numpy as np

sess = tf.Session()
init = (tf.global_variables_initializer(), tf.local_variables_initializer())
sess.run(init)

t3 = tf.constant([[1, 2], [3, 4]])
t4 = tf.reduce_sum(t3)

arr = [1, 2, 3, 4, 5]

print(arr[-1])
print(sess.run(tf.reshape(t3, tf.shape(t3))))