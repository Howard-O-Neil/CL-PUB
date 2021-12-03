import tensorflow_core as tf
import numpy as np

from BregmanToolkit.bregman import suite as st

import matplotlib.pyplot as plt

filenames = tf.train.match_filenames_once(
    "/home/howard/collaborative-recommender/data/*.wav"
)
count_num_files = tf.size(filenames)
filename_queue = tf.train.string_input_producer(filenames)
reader = tf.WholeFileReader()
filename, file_contents = reader.read(filename_queue)

sess = tf.Session()
init = (tf.global_variables_initializer(), tf.local_variables_initializer())
sess.run(init)

coord = tf.train.Coordinator()
threads = tf.train.start_queue_runners(coord=coord, sess=sess)


def get_chromagram(sess):
    F = st.Chromagram(
        sess.run(filename).decode("utf-8"), nfft=16384, wfft=8192, nhop=2205
    )
    return F.X


def extract_feature_vector(sess, chroma_data):
    chroma = tf.placeholder(tf.float32)
    max_freqs = tf.argmax(chroma, 0)  # first division

    num_features, num_samples = np.shape(chroma_data)
    freq_vals = sess.run(max_freqs, feed_dict={chroma: chroma_data})
    hist, _ = np.histogram(freq_vals, bins=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    return hist.astype(float) / num_samples


def get_dataset(sess):
    num_files = sess.run(count_num_files)
    coord = tf.train.Coordinator()
    threads = tf.train.start_queue_runners(sess=sess, coord=coord)

    xs = []
    for _ in range(num_files):
        chroma_data = get_chromagram(sess)
        x = np.matrix(extract_feature_vector(sess, chroma_data))

        if len(xs) == 0: xs = x
        else: xs = np.vstack((xs, x))

    coord.request_stop()
    coord.join()

    return xs

k = 2
max_iterations = 100

def initial_centroids(X, k):
    return X[0:k, :]

def assign_cluster(X, centroids):
    expanded_vectors = tf.expand_dims(X, 0)
    expanded_centroids = tf.expand_dims(centroids, 1)

    # calculate distance from each vector to each centroids 
    distances = tf.reduce_sum(tf.square(tf.subtract(expanded_vectors,
        expanded_centroids)), 2)

    return tf.argmin(distances, 0)

def recenter_centroids(X, Y, k):
    sums = tf.unsorted_segment_sum(X, Y, k)
    counts = tf.unsorted_segment_sum(tf.ones_like(X), Y, k)

    return sums / counts    

X = get_dataset(sess)
centroids = initial_centroids(X, k)

i, coveraged = 0, False
while not coveraged and i < max_iterations:
    i += 1
    Y = sess.run(assign_cluster(X, centroids))
    centroids = sess.run(recenter_centroids(X, Y, k))

print(centroids)