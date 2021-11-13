import tensorflow_core as tf
import numpy as np

raw_data = np.random.normal(10, 1, 100)

alpha = tf.constant(0.05)
curr_value = tf.placeholder(tf.float32)
prev_avg = tf.Variable(0.)

update_avg = alpha * curr_value + (1 - alpha) * prev_avg

# avg_hist = tf.compat.v1.summary.scalar("running average", update_avg)
# value_hist = tf.compat.v1.summary.scalar("incoming_values", curr_value)
# merged = tf.compat.v1.summary.merge_all()
# writer = tf.compat.v1.summary.FileWriter("./logs")

init = tf.global_variables_initializer()

with tf.Session() as sess:
    sess.run(init)
    sess.add_graph(sess.graph)
    for i in range(len(raw_data)):
        curr_avg = sess.run(update_avg, feed_dict={curr_value: raw_data[i]})
        # summary_str, curr_avg = sess.run([merged, update_avg],
        #     feed_dict={curr_value: raw_data[i]})
        sess.run(tf.compat.v1.assign(prev_avg, curr_avg))
        print(raw_data[i], curr_avg)
        # writer.add_summary(summary_str, i)
