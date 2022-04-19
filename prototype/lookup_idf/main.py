import tensorflow as tf

vocab = ["a", "b", "c", "d"]
idf_weights = [0.25, 0.75, 0.6, 0.4]
data = tf.constant([["a", "c", "d", "d"], ["d", "z", "b", "z"]])
layer = tf.keras.layers.StringLookup(output_mode="tf_idf")
layer.set_vocabulary(vocab, idf_weights=idf_weights)
print(layer(data))