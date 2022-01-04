import tensorflow_core as tf
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sb
import scipy as sp
import sklearn.preprocessing as skp
import sklearn.model_selection as skm

sess = tf.Session()
init = tf.global_variables_initializer()
sess.run(init)


a = tf.constant([[1, 2, 3, 4], [1, 2, 3, 4]])
b = tf.constant(5)

print(sess.run(tf.divide(a, b)))