import numpy as np
import tensorflow_core as tf

import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
tf.enable_eager_execution()

a = tf.constant([
    [1, 2, -3],
    [3, 4, 5 ]
])

b = tf.constant([1, 2])

b = tf.expand_dims(b, 1)

print(tf.divide(a, b).numpy())
print(tf.maximum(0, a))
# print(np.float32(2))