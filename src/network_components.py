import tensorflow_core as tf

def relu_act(x):
    return tf.maximum(0., x)

def conv_layer(x, W, b, conv_stride_step, conv_padding, \
                is_maxpool, is_dropout, \
                maxpool_wnd_size=None, maxpool_stride_step=None, maxpool_padding=None, \
                drop_rate=None):
    conv = tf.nn.conv2d(x, W, strides=[conv_stride_step] * 4, padding=conv_padding)
    conv_with_b = tf.nn.bias_add(conv, b)
    conv_relu = relu_act(conv_with_b)
    
    # max pool layer
    conv_maxpool = None
    if is_maxpool:
      conv_maxpool = tf.nn.max_pool(conv_relu, \
          ksize=[maxpool_wnd_size[0], maxpool_wnd_size[1]], \
          strides=[1, maxpool_stride_step, maxpool_stride_step, 1], \
          padding=maxpool_padding)
    
    # dropout layer
    if is_dropout:
      if conv_maxpool != None:
          return tf.nn.dropout(conv_maxpool, rate=drop_rate)
      else: return tf.nn.dropout(conv_relu, rate=drop_rate)

    if conv_maxpool != None:
        return conv_maxpool
    else: return conv_relu

def dense_layer(x, W, b):
    dense = tf.add(tf.matmul(x, W), b)
    return relu_act(dense)

