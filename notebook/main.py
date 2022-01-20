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
import sklearn.model_selection as skm

import sys

np.set_printoptions(threshold=sys.maxsize)

scaler = skp.StandardScaler()

def unpickle(file):
    fo = open(file, 'rb')
    dict = pickle.load(fo, encoding='latin1')
    fo.close()
    return dict

def read_data(directory):
    names = unpickle('{}/batches.meta'.format(directory))['label_names']

    data, labels = [], []
    for i in range(1, 6):
        filename = '{}/data_batch_{}'.format(directory, i)
        batch_data = unpickle(filename)
        if len(data) > 0:
            data = np.vstack((data, batch_data['data']))
            labels = np.hstack((labels, batch_data['labels']))
        else:
            data = batch_data['data']
            labels = batch_data['labels']

    data = clean(data)
    data = data.astype(np.float32)
    return names, data, labels

def clean(data, is_train):
    # 3072 = 3 * 32 * 32
    imgs = data.reshape(data.shape[0], 3, 32, 32)
    grayscale_imgs = np.mean(imgs, axis=1)

    cropped_imgs = grayscale_imgs[:, 4:28, 4:28]
    img_data = cropped_imgs.reshape(data.shape[0], -1)
    
    if is_train:
        scaler.fit(img_data)
    
    return scaler.transform(img_data)

names, data, labels = read_data(os.getcwd())
print(data.shape)