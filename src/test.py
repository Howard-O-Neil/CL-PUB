import numpy as np
import tensorflow_core as tf

LIST_conv_w = []
LIST_conv_b = []

for i in range(0, num_conv_layers):
    wnd_h = filter_wnd_h[i]
    wnd_w = filter_wnd_w[i]
    
    chanel = 1
    if i > 0: chanel = num_filters[i - 1]

    num_filter = num_filters[i]

    CONV_w = tf.random_normal([wnd_h, wnd_w, chanel, num_filter])
    CONV_b = tf.random_normal([num_filter])
    fan_in = np.float32(wnd_h * wnd_h * chanel)
    he_norm = tf.sqrt(
        tf.divide(2., fan_in)
    )

    LIST_conv_w.append(tf.Variable(tf.multiply(CONV_w, he_norm), name=f"CONV_W{i + 1}"))
    LIST_conv_b.append(tf.Variable(tf.multiply(CONV_b, he_norm)), name=f"CONV_B{i + 1}")


# channel size = num_filters, result from previous layer
conv_idx = 1
CONV_w2 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_w2 = tf.multiply(CONV_w2, tf.sqrt(tf.divide(2., )))
CONV_b2 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w3 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b3 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w4 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b4 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w5 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b5 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = 1, directly from input
conv_idx += 1
CONV_w6 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b6 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w7 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b7 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w8 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b8 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w9 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b9 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w10 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b10 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = 1, directly from input
conv_idx += 1
CONV_w11 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b11 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w12 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b12 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))

# channel size = num_filters, result from previous layer
conv_idx += 1
CONV_w13 = tf.Variable(tf.random_normal([filter_wnd_h[conv_idx], filter_wnd_w[conv_idx], num_filters[conv_idx - 1], num_filters[conv_idx]]))
CONV_b13 = tf.Variable(tf.random_normal([num_filters[conv_idx]]))