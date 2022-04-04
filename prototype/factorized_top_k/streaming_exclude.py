from typing import Tuple
import tensorflow as tf

# The goal is to calculate the top K highest relevant negative samples from very large dataset
# but given scenarios K = 10, the top K might have overlapse ids with the positive sample
# given ids of positive samples on each batch row, remove overlapse ids if exists

# A query embedding will be passed into stream
queries = None

# top 10
k = 10

# Given candidates = movies.batch(128).map(
#       lambda x: (x["movie_id"], movie_model(x["movie_title"]))
# )
# this function will be used as a another mapper to candidate
# notice that output indices will be 2 dimension
def top_scores(candidate_index: tf.Tensor,
                candidate_batch: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:

    scores = tf.linalg.matmul(queries, candidate_batch, transpose_b=True)

    k_ = k

    scores, indices = tf.math.top_k(scores, k=k_, sorted=True)

    return scores, tf.gather(candidate_index, indices)

# top_scores will produce multiple batch of tuple(score, identity)
# this function will be used as reduce, to reduce result to only 1 tuple of top k 
def top_k(state: Tuple[tf.Tensor, tf.Tensor],
            x: Tuple[tf.Tensor, tf.Tensor]) -> Tuple[tf.Tensor, tf.Tensor]:

    state_scores, state_indices = state
    x_scores, x_indices = x

    joined_scores = tf.concat([state_scores, x_scores], axis=1)
    joined_indices = tf.concat([state_indices, x_indices], axis=1)

    k_ = k

    scores, indices = tf.math.top_k(joined_scores, k=k_, sorted=True)

    return scores, tf.gather(joined_indices, indices, batch_dims=1)

# You can use this function instead of tf.gather(identifiers, indices, batch_dims=1)
def _take_along_axis(arr: tf.Tensor, indices: tf.Tensor) -> tf.Tensor:
  """Partial TF implementation of numpy.take_along_axis.

  See
  https://numpy.org/doc/stable/reference/generated/numpy.take_along_axis.html
  for details.

  Args:
    arr: 2D matrix of source values.
    indices: 2D matrix of indices.

  Returns:
    2D matrix of values selected from the input.
  """

  row_indices = tf.tile(
      tf.expand_dims(tf.range(tf.shape(indices)[0]), 1),
      [1, tf.shape(indices)[1]])
  gather_indices = tf.concat(
      [tf.reshape(row_indices, (-1, 1)),
       tf.reshape(indices, (-1, 1))], axis=1)

  return tf.reshape(tf.gather_nd(arr, gather_indices), tf.shape(indices))


# 10 + 1 positive samples
# First step would be calculate top 11 scores, 
# then reduce the ids if exists and recalculate top 10 scores
adjusted_k = 11

batch_size = 5

# Seed data for testing, ensure data format
scores = tf.reshape(tf.cast(tf.range(50, (adjusted_k * 5) + 50), tf.float32), shape=(batch_size, adjusted_k))
identifiers = tf.reshape(tf.range(adjusted_k * 5), shape=(batch_size, adjusted_k))

def exclude():
    # Seed ids need to be excluded
    exclude = tf.constant([
        [2],
        [15],
        [27],
        [33],
        [47],
    ])

    idents = tf.expand_dims(identifiers, -1)
    exclude = tf.expand_dims(exclude, 1)

    isin = tf.math.reduce_any(tf.math.equal(idents, exclude), -1)

    # Set the scores of the excluded candidates to a very low value.
    adjusted_scores = (scores - tf.cast(isin, tf.float32) * 1.0e5)

    # Make sure k is smaller or equal scores size
    k_ = tf.math.minimum(k, tf.shape(scores)[1])

    _, indices = tf.math.top_k(adjusted_scores, k_)

    # produce the same result
    print(tf.gather(identifiers, indices, batch_dims=1))
    print(_take_along_axis(identifiers, indices))

exclude()