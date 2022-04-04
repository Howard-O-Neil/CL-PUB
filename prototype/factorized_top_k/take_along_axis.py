from numpy import take_along_axis
import tensorflow as tf


def take_along_axis(arr: tf.Tensor, indices: tf.Tensor) -> tf.Tensor:
    """Partial TF implementation of numpy.take_along_axis.
    """

    # convert from normal 2D index which row = batch and column = id
    # to list of 2D positions matrix
    """
    For example convert 
    [
        [1, 3, 6], 
        [2, 5, 8]
    ] to
    [
        [0, 1],
        [0, 3],
        [0, 6],
        [1, 2],
        [1, 5],
        [1, 8],
    ]
    """ 
    row_indices = tf.tile(
        tf.expand_dims(tf.range(tf.shape(indices)[0]), 1), [1, tf.shape(indices)[1]]
    )
    gather_indices = tf.concat(
        [tf.reshape(row_indices, (-1, 1)), tf.reshape(indices, (-1, 1))], axis=1
    )

    # gather_nd = access to all dimension of matrix
    # gather just allow access to 1 dimension
    # however gather_nd result in a flat matrix
    return tf.reshape(tf.gather_nd(arr, gather_indices), tf.shape(indices))

identifiers = tf.reshape(tf.range(10 * 5), shape=(5, 10))
indices = tf.constant(
    [
        [0, 3],
        [3, 7],
        [8, 5],
        [6, 7],
        [9, 3],
    ]
)
print(take_along_axis(identifiers, indices))
