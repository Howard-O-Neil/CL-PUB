import tensorflow_core as tf
import matplotlib.pyplot as plt
import numpy as np
import math

def split_dataset(x_dataset, y_dataset, ratio):
    arr = np.arange(x_dataset.size)
    np.random.shuffle(arr)
    num_train = int(ratio * x_dataset.size)
    x_train = x_dataset[arr[0:num_train]]
    x_test = x_dataset[arr[num_train:x_dataset.size]]
    y_train = y_dataset[arr[0:num_train]]
    y_test = y_dataset[arr[num_train:x_dataset.size]]
    return x_train, x_test, y_train, y_test

x_dataset = np.linspace(-1, 1, 300)

y_train_coeffs = [0, 1, 2, 3, 4, 5, 6] # 6 degree
y_dataset = 5

for i in range(len(y_train_coeffs)):
    y_dataset += y_train_coeffs[i] * np.power(x_dataset, i)

y_dataset += np.random.randn(*x_dataset.shape) * 1.5 # add noise

(x_train, x_test, y_train, y_test) = split_dataset(x_dataset, y_dataset, 0.7)

x_test += np.random.randn(*x_test.shape) * 0.5 # add noise
y_test += np.random.randn(*y_test.shape) * 0.5 # add noise

def model(X, w):
    terms = []
    for i in range(len(y_train_coeffs)):
        term = tf.multiply(w[i], tf.pow(X, i))
        terms.append(term)
    return tf.add_n(terms)

X = tf.placeholder(tf.float32)
Y = tf.placeholder(tf.float32)

w = tf.Variable([0.] * len(y_train_coeffs), name="parameters")
y_model = model(X, w)

learning_rate = 0.01
training_epochs = 100
reg_lambda = 0. # start point

cost = tf.add(
    tf.reduce_sum(tf.square(Y - y_model)),
    tf.multiply(reg_lambda, tf.reduce_sum(tf.square(w))),
)

train_op = tf.train.GradientDescentOptimizer(learning_rate).minimize(cost)

acceptableError = 8.0

def comparePrediction(y_feed, y_predict):
    if np.sqrt(np.square(y_feed - y_predict)) <= acceptableError:
        return True
    return False

def calculate_accuracy(x_dataset, y_dataset, w_val, coeffs):
    acc = 0
    for ind, x in enumerate(x_dataset):
        y_predict = 5
        for i in range(len(coeffs)):
            y_predict += w_val[i] * np.power(x, i)

        if comparePrediction(y_dataset[ind], y_predict):
            acc += 1

    return acc / len(x_dataset) * 100

sess = tf.Session()
init = tf.global_variables_initializer()
sess.run(init)

min_distance_accuracy = 100

# parameter equivalent for best lambda
final_w = np.array([0.] * len(y_train_coeffs))

for reg_lambda in np.linspace(0, 10, 10000): 
    for epoch in range(training_epochs):
        sess.run(train_op, feed_dict={X: x_train, Y: y_train})

    distance_accuracy = math.fabs( \
        calculate_accuracy(x_train, y_train, sess.run(w), y_train_coeffs) - \
        calculate_accuracy(x_test, y_test, sess.run(w), y_train_coeffs))
    
    if distance_accuracy <= min_distance_accuracy:
        print("recover from overfit")
        print(f"""Accuracy on training dataset \ 
            {calculate_accuracy(x_train, y_train, sess.run(w), y_train_coeffs)}""")
        print(f"""Accuracy on testing dataset \ 
            {calculate_accuracy(x_test, y_test, sess.run(w), y_train_coeffs)}""")
        final_w = sess.run(w)
        min_distance_accuracy = distance_accuracy
    
    w.assign([0.] * len(y_train_coeffs)) # reset parameters

sess.close()