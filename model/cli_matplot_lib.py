import pickle
import json
import numpy as np
import os

import tensorflow_core as tf
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd
import seaborn as sb
import scipy as sp
import sklearn.preprocessing as skp
import skimage
import skimage.io
import skimage.transform
import numpy as np
import cv2
import time
import sys

np.set_printoptions(threshold=sys.maxsize)

from tensorflow_core import ConfigProto

config = ConfigProto()
config.gpu_options.allow_growth = True


import file_utils
import tf_preprocess_img as tf_preproc

def plot_images(images, label_ids, label_names, is_original, is_scale=True):
    
    # fig_s = plt.gcf()
    # fig_s.set_size_inches(30, 30)
    
    # 3x3 grid
    fig, axes = plt.subplots(6, 6, figsize=(10,10))
    fig.tight_layout()

    # fig.subplots_adjust(hspace=0.5)

    for i, ax in enumerate(axes.flat): # from 0 -> 8

        # Show image.
        if is_original:
            img = np.reshape(images[i], (3, 32, 32))
            proper_imgs = np.transpose(img, (1, 2, 0))

            ax.imshow(proper_imgs, \
                      interpolation="spline16")
        else:
            if is_scale == True:
                ax.imshow(np.reshape(file_utils.scale_img(images[i]), (224, 224, 3)), \
                          interpolation="spline16")
            else: ax.imshow(np.reshape(images[i], (224, 224, 3)), \
                          interpolation="spline16")
            
        _class_ = label_names[label_ids[i]]
        
        xlabel = "{0}".format(_class_)

        ax.set_xlabel(xlabel)
        
        # Remove ticks from the plot.
        ax.set_xticks([])
        ax.set_yticks([])
    
    plt.savefig(f"/home/howard/collaborative-recommender/data/plot/{str(int(time.time()))}.png")


names, data, labels = file_utils.read_data(\
    "/home/howard/collaborative-recommender/data/cifa10", is_train=True)

t_names, t_data, t_labels = file_utils.read_data(\
    "/home/howard/collaborative-recommender/data/cifa10", is_train=False)

plot_images(data, labels, names, is_original=True)

sess = tf.Session()

# 32 x 32 images
x = tf.placeholder(tf.float32, [None, 3072])

# one-hot vector
y = tf.placeholder(tf.float32, [None, 10])

is_dropout = True

preprocess_x = tf_preproc.tf_preprocess_images(x, is_dropout)

if is_dropout:
    x_scale = tf_preproc.tf_augment_images(preprocess_x)
    x_data_shape = tf.shape(x_scale)
    x_data_size = tf.squeeze(tf.slice(x_data_shape, [0], [1]))

    y_data_shape = tf.shape(y)
    y_data_size = tf.squeeze(tf.slice(y_data_shape, [0], [1]))

    y_augments = tf.tile(y, [tf.cast(tf.divide(x_data_size, y_data_size), tf.int32), 1]) 

    shuffle_index = tf.random.shuffle(
        tf.range(0, x_data_size, 1)
    )

    x_shuffle = tf.gather(x_scale, shuffle_index)
    y_shuffle = tf.gather(y_augments, shuffle_index)
else:
    x_shuffle = preprocess_x



def convert_to_one_hot(y_dataset, num_labels):
    y_one_hot = np.array([-99.] * num_labels)
    for val in y_dataset:
        one_hot = np.array(\
            [0.] * int(val) + [1.] + [0.] * int(num_labels - val - 1))
        
        y_one_hot = np.vstack((y_one_hot, one_hot))
    
    return y_one_hot[1:] # remove first dummy

y_train_one_hot = convert_to_one_hot(labels, len(names))
y_test_one_hot = convert_to_one_hot(t_labels, len(t_names))

print(sess.run(x_scale, feed_dict={x: data[0:5]}).shape)

# print(sess.run(x_scale, feed_dict={x: data[0:5]}).shape)
# print(sess.run(x_data_size, feed_dict={x: data[0:5]}))
# print(sess.run(shuffle_index, feed_dict={x: data[0:5], y: y_train_one_hot[0:5]}))