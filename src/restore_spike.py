import tensorflow as tf
tf.compat.v1.disable_eager_execution()
sess = tf.compat.v1.InteractiveSession()

raw_data = [1., 2., 8., -1., 0., 5.5, 6., 13]
spikes = tf.Variable([False] * len(raw_data), name='spikes')
saver = tf.compat.v1.train.Saver()

print("\n ========== Before restore ==========\n")
# print(spikes.eval()) # throw error as spikes is uninitialized

saver.restore(sess, "./spike.ckpt")
print("\n ========== After restore ==========\n")
print(spikes.eval())

sess.close()