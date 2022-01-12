"""
IMPORT LIBS
"""

import tensorflow_core as tf
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd
import seaborn as sb
import scipy as sp
import sklearn.preprocessing as skp
import sklearn.model_selection as skm

import sys

np.set_printoptions(threshold=sys.maxsize)

"""
LOAD DATA + SCALE
"""

air_bnb = pd.read_csv("./airbnb.csv")

dataset = air_bnb[["latitude", "longitude", "id"]].to_numpy()

scaler = skp.StandardScaler()
scaler.fit(dataset)

dataset = scaler.transform(dataset)

"""
PLOT DATA
"""

df = pd.DataFrame(dataset[:, 0:2], columns=["x", "y"])
sb.relplot(x="x", y="y", data=df, kind="scatter", height=6, aspect=1.2)

"""
SOME HYPERPARAMETERS
"""

max_training_epochs = 2000

neurons_diff_stop_cond = 0.0001

# 2500 (50 * 50) neurons
width = 50
height = 50
num_neurons = width * height

# init sigma value = max(width, height) / 2
init_sigma = 25.0

learning_rate = 0.1

# dimension, depend on number of features
dim = 2

"""
INIT VARIABLES
"""

x = tf.placeholder(tf.float32, [None, dim])
iter = tf.placeholder(tf.float32)

# (w x h) neurons, contains weight
neurons = tf.Variable(tf.random_normal([width * height, dim]))

"""
LATERLAS INHIBITION CONNECTIONS BETWEEN NEURONS
"""

locs = [[x, y] for y in range(height) for x in range(width)]
# neurons locations
neu_locs = tf.to_float(tf.convert_to_tensor(locs))

"""
BMU = BEST MATCH UNIT / BEST NEURON
GET BMU LOCATION FROM X (SINGLE DATAPOINT)
"""

expanded_x = tf.expand_dims(x, 1)

mse_to_neurons_by_weight = tf.reduce_mean(
    tf.square(tf.subtract(expanded_x, neurons)), 2
)

min_mse_idx = tf.argmin(mse_to_neurons_by_weight, 1)

loc_x = tf.floormod(min_mse_idx, width)
loc_y = tf.floordiv(min_mse_idx, height)

bmu_loc = tf.to_float(tf.squeeze(
    tf.stack([loc_x, loc_y], 2)
))

"""
CALCULATE NEIGHBOR CHANGES TOWARD DATA X
"""

decay_rate = tf.exp(tf.divide(tf.multiply(-1.0, iter), max_training_epochs))

alpha = tf.multiply(decay_rate, learning_rate)
sigma = tf.multiply(decay_rate, init_sigma)

expanded_bum_loc = tf.expand_dims(bmu_loc, 1)
mse_to_neurons_by_lateral = tf.reduce_mean(
    tf.square(tf.subtract(expanded_bum_loc, neu_locs)), 2
)

neighbor_factor = tf.exp(
    tf.divide(
        tf.multiply(-1.0, mse_to_neurons_by_lateral), tf.multiply(2.0, tf.square(sigma))
    )
)

learning_factor = tf.multiply(alpha, neighbor_factor)

i = tf.constant(0)
m0 = tf.constant([[-99.0] * dim])  # dummy

c = lambda i, m: tf.less(i, num_neurons)
b = lambda i, m: [
    tf.add(i, 1),
    tf.concat(
        [m, tf.expand_dims(tf.tile(tf.slice(learning_factor, [i], [1]), [dim]), 0)], 0
    ),
]
tile = tf.while_loop(
    c,
    b,
    loop_vars=[i, m0],
    shape_invariants=[i.get_shape(), tf.TensorShape([None, dim])],
)

learning_factor_grid = tf.slice(tile[1], [1, 0], [num_neurons, dim])  # remove dummy

# vector from neurons to x = x - neurons
distance_vectors = tf.subtract(tf.expand_dims(x, 0), neurons)

neurons_diff = tf.multiply(learning_factor_grid, distance_vectors)

update_neurons = tf.assign(neurons, tf.add(neurons, neurons_diff))

"""
TRAINING LOOP
"""

sess = tf.Session()
init = (tf.global_variables_initializer())
sess.run(init)

for i in range(max_training_epochs):
    variance = np.float32(0.)
    for data in dataset:
        _, diff = sess.run([update_neurons, neurons_diff], \
                    feed_dict={x: data[0:2], iter: i})
        
        variance += np.sum(
            np.mean(np.absolute(diff), axis=1))

    variance = variance / dataset.shape[0] 
    
    print(variance)
    if variance <= neurons_diff_stop_cond:
        break

np.as np.float32