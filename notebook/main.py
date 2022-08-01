import tensorflow_core as tf
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd
import seaborn as sb
import scipy as sp
import sklearn.preprocessing as skp
import sklearn.model_selection as skm

np.set_printoptions(linewidth=1000)

def convert_to_one_hot(y_dataset, num_labels):
    y_one_hot = np.array([-99.] * num_labels)
    for val in y_dataset:
        one_hot = np.array(\
            [0.] * (val - 1) + [1.] + [0.] * (10 - val))
        y_one_hot = np.vstack((y_one_hot, one_hot))
    
    return y_one_hot[1:] # remove first dummy

red_wines = pd.read_csv("./winequality-red.csv")

x_dataset = red_wines.drop(\
    ["quality"], axis=1).to_numpy()
y_dataset =  red_wines["quality"].to_numpy()

x_train, x_test, y_train, y_test = \
    skm.train_test_split(\
         x_dataset, \
         y_dataset, \
         random_state = 0, test_size = 0.15)

y_train_one_hot = convert_to_one_hot(y_train, 10)
y_test_one_hot = convert_to_one_hot(y_test, 10)

print(y_train_one_hot)
scaler = skp.StandardScaler()
scaler.fit(x_train)

x_train = scaler.transform(x_train)
x_test = scaler.transform(x_test)