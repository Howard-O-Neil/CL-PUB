from SOM import SOM
from matplotlib import pyplot as plt
import numpy as np
import tensorflow_core as tf

tf.logging.set_verbosity(tf.logging.ERROR)

# our color dataset
colors = np.array(
    [[0., 0., 1.],
    [0., 0., 0.95],
    [0., 0.05, 1.],
    [0., 1., 0.],
    [0., 0.95, 0.],
    [0., 1, 0.05],
    [1., 0., 0.],
    [1., 0.05, 0.],
    [1., 0., 0.05],
    [1., 1., 0.]])

# width     = 4
# height    = 4
# division  = 3, match dataset features
som = SOM(4, 4, 3)

# start SOM learning
som.train(colors)

# plot data
plt.imshow(som.centroid_grid)
plt.show()