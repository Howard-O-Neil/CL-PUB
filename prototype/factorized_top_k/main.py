import sys

sys.path.append("../..")

from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
from tensorflow import keras

import numpy as np

from model.movielen_model import MovieLensModel

tf.random.set_seed(42)

# For each integer from StringLookup
#   create a params vector of size = embedding_dimension
def create_embedding_model(data, embedding_dimension=32):
    return keras.Sequential(
        [
            keras.layers.StringLookup(vocabulary=data, mask_token=None),
            keras.layers.Embedding(data.shape[0] + 1, embedding_dimension),
        ]
    )


def convert_to_numpy(dataset):
    return np.asarray(list(dataset.as_numpy_iterator()))


# Ratings
ratings = tfds.load("movielens/100k-ratings", split="train")

movies = tfds.load("movielens/100k-movies", split="train").map(
    lambda x: x["movie_title"]
)

# Select basic features
ratings = ratings.map(
    lambda x: {"movie_title": x["movie_title"], "user_id": x["user_id"]}
)
gt_users = ratings.map(lambda x: x["user_id"])
gt_movies = ratings.map(lambda x: x["movie_title"])

# query tower
unique_user_ids = np.unique(convert_to_numpy(gt_users))
user_model = create_embedding_model(unique_user_ids)

# candidate tower
unique_movie_titles = np.unique(convert_to_numpy(gt_movies))
movie_model = create_embedding_model(unique_movie_titles)

k = 100 # top 100 most relevant candidates

query_embeddings = user_model(convert_to_numpy(gt_users.take(10)))
true_candidate_embeddings = movie_model(convert_to_numpy(gt_movies.take(10)))

task = tfrs.tasks.Retrieval(
    metrics=tfrs.metrics.FactorizedTopK(
        candidates=movies.batch(128).map(lambda x: movie_model(x))
    )
)

def compare_metrics_after_train():
    test_embed_dataset = movies.take(1).map(lambda x: movie_model(x))

    print("===== Before training...")
    top_k_predictions, ids = task.factorized_metrics._candidates(query_embeddings, k=k)
    print(next(iter(test_embed_dataset)).numpy())
    print(top_k_predictions)

    # top 1, 5, 10, 50, 100 accuracy
    print(task.factorized_metrics.result())

    # Create a retrieval model.
    model = MovieLensModel(user_model, movie_model, task)
    model.compile(optimizer=keras.optimizers.Adagrad(0.5))

    # Train

    model.fit(ratings.batch(4096), epochs=1)

    print("===== After training...")
    top_k_predictions, ids = task.factorized_metrics._candidates(query_embeddings, k=k)
    print(next(iter(test_embed_dataset)).numpy())
    print(top_k_predictions)

    # top 1, 5, 10, 50, 100 accuracy
    print(task.factorized_metrics.result())


# "*" is not matmul, this is just each query multiply with its equivalent candidate
#       there can be duplicated user or candidate
# keepdims = keep the original dimensions
positive_scores = tf.reduce_sum(
    query_embeddings * true_candidate_embeddings, axis=1, keepdims=True
)

top_k_predictions, ids = task.factorized_metrics._candidates(query_embeddings, k=k)

# label positive as 1
# label top K as zeros

y_true = tf.concat(
    [tf.ones(tf.shape(positive_scores)), tf.zeros_like(top_k_predictions)], axis=1
)
y_pred = tf.concat([positive_scores, top_k_predictions], axis=1)

update_ops = []
for metric in task.factorized_metrics._top_k_metrics:
    update_ops.append(metric.update_state(y_true=y_true, y_pred=y_pred))

for idx, metric in enumerate(task.factorized_metrics._top_k_metrics):
    print(f"Top {idx + 1} accuracy: {metric.result()}")
