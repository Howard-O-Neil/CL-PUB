import tensorflow_core as tf
import matplotlib.pyplot as plt
import numpy as np

import utils

learning_rate = 0.001
training_epochs = 1000
reg_lambda = 0.0

x_dataset = np.linspace(-1, 1, 200)

num_coeffs = 9
y_dataset_params = [0.0] * num_coeffs
y_dataset_params[2] = 1

y_dataset = 0
for i in range(num_coeffs):
    y_dataset += y_dataset_params[i] * np.power(x_dataset, i)
y_dataset += np.random.randn(*x_dataset.shape) * 0.3
(x_train, x_test, y_train, y_test) = utils.split_dataset(x_dataset, y_dataset, 0.7)

# plot train data
plt.scatter(x_train, y_train)
plt.show()

# plot test data
plt.scatter(x_test, y_test)
plt.show()

X = tf.placeholder(tf.float32)
Y = tf.placeholder(tf.float32)


def model(X, w):
    terms = []
    for i in range(num_coeffs):
        term = tf.multiply(w[i], tf.pow(X, i))
        terms.append(term)
    return tf.add_n(terms)


w = tf.Variable([0.0] * num_coeffs, name="parameters")
y_model = model(X, w)

cost = tf.divide(
    tf.add(
        tf.reduce_sum(tf.square(Y - y_model)),
        tf.multiply(reg_lambda, tf.reduce_sum(tf.square(w))),
    ),
    2 * x_train.size,
)

train_op = tf.train.GradientDescentOptimizer(learning_rate).minimize(cost)

sess = tf.Session()
init = tf.global_variables_initializer()
sess.run(init)

list_w = list()
list_final_cost = list()

for reg_lambda in np.linspace(0, 1, 100):
    for epoch in range(training_epochs):
        sess.run(train_op, feed_dict={X: x_train, Y: y_train})

    print("## Before and after calculate cost")
    print(np.sum(sess.run(w)))
    list_final_cost.append(sess.run(cost, feed_dict={X: x_test, Y: y_test}))
    list_w.append(sess.run(w))
    print(np.sum(sess.run(w)))

sess.run(cost, feed_dict={X: x_test, Y: y_test})
print("## After go through all lambda")
print(np.sum(sess.run(w)))


sess.close()

print("===== Done training =====")
