import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf

# single derivative
def simple_linear():
    x = tf.Variable(3.0)

    with tf.GradientTape() as tape:
        y = x**2

    dy_dx = tape.gradient(y, x)

    print("Simple linear derivative ...")
    print(dy_dx.numpy())

# gradient derivative
# we can understand this as calculating derivative for multiple variable formula
# by calculating derivative for each variable, and treat others as constant
def weight_bias():
    w = tf.Variable(tf.random.normal((3, 2)), name="w")
    b = tf.Variable(tf.zeros(2, dtype=tf.float32), name="b")
    
    x = tf.convert_to_tensor([[1., 2., 3.]])

    with tf.GradientTape() as tape:
        y = tf.nn.bias_add(tf.matmul(x, w), b)
        loss = tf.reduce_mean(y ** 2)
    
    # Calculate gradient derivative for each w, b
    [dl_dw, dl_db] = tape.gradient(loss, [w, b])

    print("Gradient derivative ...")
    print(f"Weight gradient: {dl_dw.numpy()}")
    print(f"Bias   gradient: {dl_db.numpy()}")

# calculating derivative for multiple variable formula
def multi_variant_derivative():
    # A trainable variable
    x0 = tf.Variable(3.0, name='x0')
    # Not trainable
    x1 = tf.Variable(3.0, name='x1', trainable=False)
    # Not a Variable: A variable + tensor returns a tensor.
    x2 = tf.add(tf.Variable(2.0, name='x2'), 1.0)
    # Not a variable
    x3 = tf.constant(3.0, name='x3')
    # A trainable variable
    x4 = tf.Variable(4.0, name="x4")

    with tf.GradientTape() as tape:
        y = (x0**2) + (x1**2) + (x2**2) - (x4**2) + x3

    grad = tape.gradient(y, [x0, x1, x2, x3, x4])
    print("Multivariant derivative ...")
    print(f"x0 derivative: {grad[0].numpy()}")
    print(f"x1 derivative: {grad[1]}")
    print(f"x2 derivative: {grad[2]}")
    print(f"x3 derivative: {grad[3]}")
    print(f"x4 derivative: {grad[4]}")

simple_linear()
weight_bias()
multi_variant_derivative()