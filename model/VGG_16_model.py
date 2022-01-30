import tensorflow_core as tf
import numpy as np
import model.network_components as nc
import model.model_hyper_params as mhp

# VGG-16 model made gor grayscale images
class VGG_16:
    def __init__(self) -> None:
        pass

    def init_conv_parameters(self):
        self.LIST_conv_w = []
        self.LIST_conv_b = []

        self.SUM_conv = tf.constant(0.)
        self.COUNT_conv = tf.constant(0.)

        for i in range(0, mhp.num_conv_layers):
            wnd_h = mhp.filter_wnd_h[i]
            wnd_w = mhp.filter_wnd_w[i]
            
            chanel = 1
            if i > 0: chanel = mhp.num_filters[i - 1]

            num_filter = mhp.num_filters[i]

            CONV_w = tf.random_normal([wnd_h, wnd_w, chanel, num_filter])
            CONV_b = tf.random_normal([num_filter])
            fan_in = np.float32(wnd_h * wnd_h * chanel)
            he_norm = tf.cast(tf.sqrt(
                tf.divide(2., fan_in)
            ), tf.float32)

            VAR_CONV_w = tf.Variable(tf.multiply(CONV_w, he_norm), name=f"CONV_W{i + 1}")
            VAR_CONV_b = tf.Variable(tf.multiply(CONV_b, he_norm), name=f"CONV_B{i + 1}")

            self.SUM_conv = tf.add(
                self.SUM_conv,
                tf.reduce_sum(tf.square(
                    tf.nn.bias_add(VAR_CONV_w, VAR_CONV_b))   
                )
            )
            self.COUNT_conv = tf.add(
                self.COUNT_conv, tf.cast(wnd_h * wnd_w * chanel * num_filter, dtype=tf.float32)
            )

            self.LIST_conv_w.append(VAR_CONV_w)
            self.LIST_conv_b.append(VAR_CONV_b)

    def init_dense_parameters(self):
        self.LIST_dense_w = []
        self.LIST_dense_b = []

        self.SUM_dense = tf.constant(0.)
        self.COUNT_dense = tf.constant(0.)

        for i in range(0, mhp.num_dense_layers):
            
            fan_in = 7 * 7 * mhp.num_filters[-1]
            if i > 0: fan_in = mhp.num_dense_neurons[i - 1]

            fan_out = mhp.num_dense_neurons[i]
            
            DENSE_w = tf.random_normal([fan_in, fan_out])
            DENSE_b = tf.random_normal([fan_out])

            he_norm = tf.cast(tf.sqrt(
                tf.divide(2., fan_in)
            ), tf.float32)

            VAR_DENSE_w = tf.Variable(tf.multiply(DENSE_w, he_norm), name=f"DENSE_W{i + 1}")
            VAR_DENSE_b = tf.Variable(tf.multiply(DENSE_b, he_norm), name=f"DENSE_B{i + 1}")

            self.SUM_dense = tf.add(
                self.SUM_dense,
                tf.reduce_sum(tf.square(
                    tf.nn.bias_add(VAR_DENSE_w, VAR_DENSE_b))
                )
            )
            self.COUNT_dense = tf.add(
                self.COUNT_dense, tf.cast(fan_in * fan_out, dtype=tf.float32)
            )

            self.LIST_dense_w.append(VAR_DENSE_w)
            self.LIST_dense_b.append(VAR_DENSE_b)

    def get_sum_params(self):
        return tf.add(self.SUM_dense, self.SUM_conv)
    
    def get_total_params(self):
        return tf.add(self.COUNT_dense, self.COUNT_conv)
    
    def get_model(self, data, is_train):

        layer_c = 0
        
        conv_out1 = nc.conv_layer(data, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)
        
        layer_c += 1
        conv_out2 = nc.conv_layer(conv_out1, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=True, \
                maxpool_wnd_size=[mhp.maxpool_wnd_h[0], mhp.maxpool_wnd_w[0]], \
                maxpool_padding=mhp.maxpool_padding_strategies[0], \
                maxpool_stride_step=mhp.maxpool_stride_steps[0], \
            is_dropout=False)
        
        if is_train == True:
            conv_out2 = tf.nn.dropout(
                conv_out2,
                rate = mhp.drop_rates[0]
            )
        

        # | FOR ABOVE CODE |
        # ===================== MAX POOL 1

        layer_c += 1
        conv_out3 = nc.conv_layer(conv_out2, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)
        
        layer_c += 1
        conv_out4 = nc.conv_layer(conv_out3, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=True, \
                maxpool_wnd_size=[mhp.maxpool_wnd_h[1], mhp.maxpool_wnd_w[1]], \
                maxpool_padding=mhp.maxpool_padding_strategies[1], \
                maxpool_stride_step=mhp.maxpool_stride_steps[1], \
            is_dropout=False)
        
        if is_train == True:
            conv_out4 = tf.nn.dropout(
                conv_out4,
                rate = mhp.drop_rates[1]
            )
        
        # | FOR ABOVE CODE |
        # ===================== MAX POOL 2

        layer_c += 1
        conv_out5 = nc.conv_layer(conv_out4, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)
        
        layer_c += 1
        conv_out6 = nc.conv_layer(conv_out5, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)

        layer_c += 1
        conv_out7 = nc.conv_layer(conv_out6, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=True, \
                maxpool_wnd_size=[mhp.maxpool_wnd_h[2], mhp.maxpool_wnd_w[2]], \
                maxpool_padding=mhp.maxpool_padding_strategies[2], \
                maxpool_stride_step=mhp.maxpool_stride_steps[2], \
            is_dropout=False)
        
        if is_train == True:
            conv_out7 = tf.nn.dropout(
                conv_out7,
                rate = mhp.drop_rates[2]
            )

        # | FOR ABOVE CODE |    
        # ===================== MAX POOL 3

        layer_c += 1
        conv_out8 = nc.conv_layer(conv_out7, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)
        
        layer_c += 1
        conv_out9 = nc.conv_layer(conv_out8, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)

        layer_c += 1
        conv_out10 = nc.conv_layer(conv_out9, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=True, \
                maxpool_wnd_size=[mhp.maxpool_wnd_h[3], mhp.maxpool_wnd_w[3]], \
                maxpool_padding=mhp.maxpool_padding_strategies[3], \
                maxpool_stride_step=mhp.maxpool_stride_steps[3], \
            is_dropout=False)
        
        if is_train == True:
            conv_out10 = tf.nn.dropout(
                conv_out10,
                rate = mhp.drop_rates[3]
            )

        # | FOR ABOVE CODE |
        # ===================== MAX POOL 4

        layer_c += 1
        conv_out11 = nc.conv_layer(conv_out10, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)
        
        layer_c += 1
        conv_out12 = nc.conv_layer(conv_out11, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=False, \
            is_dropout=False)

        layer_c += 1
        conv_out13 = nc.conv_layer(conv_out12, self.LIST_conv_w[layer_c], self.LIST_conv_b[layer_c], \
            conv_stride_step=mhp.conv_stride_steps[layer_c], \
            conv_padding=mhp.conv_padding_strategies[layer_c], \
            is_maxpool=True, \
                maxpool_wnd_size=[mhp.maxpool_wnd_h[4], mhp.maxpool_wnd_w[4]], \
                maxpool_padding=mhp.maxpool_padding_strategies[4], \
                maxpool_stride_step=mhp.maxpool_stride_steps[4], \
            is_dropout=False)
        
        if is_train == True:
            conv_out13 = tf.nn.dropout(
                conv_out13,
                rate = mhp.drop_rates[4]
            )
        
        # | FOR ABOVE CODE |
        # ===================== MAX POOL 5

        flat_out = tf.reshape(conv_out13, [-1, self.LIST_dense_w[0].get_shape()[0]])

        dense_out1 = nc.dense_layer(flat_out, self.LIST_dense_w[0], self.LIST_dense_b[0])
        dense_out2 = nc.dense_layer(dense_out1, self.LIST_dense_w[1], self.LIST_dense_b[1])
        
        if is_train == True:
            dense_out1 = tf.nn.dropout(
                nc.dense_layer(flat_out, self.LIST_dense_w[0], self.LIST_dense_b[0]),
                rate = mhp.drop_rates[5]
            )

            dense_out2 = tf.nn.dropout(
                nc.dense_layer(dense_out1, self.LIST_dense_w[1], self.LIST_dense_b[1]),
                rate = mhp.drop_rates[6]
            )

        return dense_out2