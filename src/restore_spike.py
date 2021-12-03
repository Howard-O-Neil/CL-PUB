import tensorflow as tf

tf.compat.v1.disable_eager_execution()
sess = tf.compat.v1.InteractiveSession()

raw_data = [1.0, 2.0, 8.0, -1.0, 0.0, 5.5, 6.0, 13]
spikes = tf.Variable([False] * len(raw_data), name="spikes")
saver = tf.compat.v1.train.Saver()

print("\n ========== Before restore ==========\n")
# print(spikes.eval()) # throw error as spikes is uninitialized

saver.restore(sess, "./spike.ckpt")
print("\n ========== After restore ==========\n")
print(spikes.eval())

sess.close()
