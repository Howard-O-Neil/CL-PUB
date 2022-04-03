import keras
import tensorflow as tf

m = keras.metrics.TopKCategoricalAccuracy(k=2)
m.update_state([[0, 0, 1], [0, 1, 0]],
               [[0.1, 0.8, 0.9], [0.05, 0.95, 0]])

print(m.result().numpy())