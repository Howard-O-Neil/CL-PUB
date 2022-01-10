import tensorflow_core as tf
from matplotlib import pyplot as plt
import numpy as np
import sys



height = 4
width = 4

sigma = 2.
alpha = 0.3

dim = 3
num_nodes = width * height

bmu_loc = tf.stack([
    2., 3.
])

locs = [
    [x, y]
        for y in range(height)
            for x in range(width)
]
node_locs = tf.to_float(tf.convert_to_tensor(locs))

expanded_bmu_loc = tf.expand_dims(bmu_loc, 0)
sqr_dists_from_bmu = tf.reduce_sum(
    tf.square(tf.subtract(expanded_bmu_loc, node_locs)), 1
)

neigh_factor = tf.exp(-tf.div(sqr_dists_from_bmu, 2 * tf.square(sigma)))

rate = tf.multiply(alpha, neigh_factor)

print()

rate_factor = tf.stack([
    tf.tile(tf.slice(rate, [i], [1]), [dim])
        for i in range(num_nodes)])

print(sess.run(rate_factor))
print()

sess = tf.Session()
init = tf.global_variables_initializer()
sess.run(init)

i = tf.constant(0)
m0 = tf.constant([ [-99.] * dim]) # dummy

c = lambda i, m: tf.less(i, num_nodes)
b = lambda i, m: [tf.add(i, 1), tf.concat(
        [m, tf.expand_dims( tf.tile( tf.slice(rate, [i], [1]), [dim] ), 0 ) ], 0
    )]
tile = tf.while_loop(c, b, loop_vars=[i, m0],
    shape_invariants=[i.get_shape(), tf.TensorShape([None, dim])])

print(sess.run(tf.slice(tile[1], [1, 0], [num_nodes, dim]))) # remove dummy


x = tf.constant([1, 2, 3])
print(sess.run(tf.stack(x for i in range(num_nodes))))