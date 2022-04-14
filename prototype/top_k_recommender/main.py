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
    lambda x: {"movie_id": x["movie_id"], "movie_title": x["movie_title"]}
)

# Select basic features
gt_users = ratings.map(lambda x: x["user_id"])
gt_movies = ratings.map(
    lambda x: {"movie_id": x["movie_id"], "movie_title": x["movie_title"]}
)

# query tower
unique_user_ids = np.unique(convert_to_numpy(gt_users))
user_model = create_embedding_model(unique_user_ids)

# candidate tower
unique_movie_titles = np.unique(
    convert_to_numpy(movies.map(lambda x: x["movie_title"]))
)
movie_model = create_embedding_model(unique_movie_titles)

k = 100  # top 100 most relevant candidates

task = tfrs.tasks.Retrieval(
    metrics=tfrs.metrics.FactorizedTopK(
        candidates=movies.batch(128).map(
            lambda x: (x["movie_id"], movie_model(x["movie_title"]))
        )
    )
)


def compare_metrics_after_train(num_users):
    query_embeddings = user_model(convert_to_numpy(gt_users.take(num_users)))

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


def compute_metrics(num_users, num_candidates):
    query_embeddings = user_model(convert_to_numpy(gt_users.take(num_users)))
    true_candidate_embeddings = movie_model(
        convert_to_numpy(gt_movies.map(lambda x: x["movie_title"]).take(num_candidates))
    )
    candidate_ids = np.expand_dims(
        convert_to_numpy(gt_movies.map(lambda x: x["movie_id"]).take(num_candidates)),
        axis=1,
    )

    # "*" is not matmul, this is just each query multiply with its equivalent candidate
    #       there can be duplicated user or candidate
    # keepdims = keep the original dimensions
    positive_scores = tf.reduce_sum(
        query_embeddings * true_candidate_embeddings, axis=1, keepdims=True
    )

    top_k_predictions, ids = task.factorized_metrics._candidates.query_with_exclusions(
        query_embeddings, k=k, exclusions=tf.convert_to_tensor(candidate_ids)
    )

    print("=== Top predictions...")
    print(top_k_predictions)
    print(ids)
    # label positive as 1
    # label top K as zeros

    y_true = tf.concat(
        [tf.ones(tf.shape(positive_scores)), tf.zeros_like(top_k_predictions)], axis=1
    )
    y_pred = tf.concat([positive_scores, top_k_predictions], axis=1)

    update_ops = []
    for metric in task.factorized_metrics._top_k_metrics:
        update_ops.append(metric.update_state(y_true=y_true, y_pred=y_pred))

    top_k = [1, 5, 10, 50, 100]
    for idx, metric in enumerate(task.factorized_metrics._top_k_metrics):
        print(f"Top {top_k[idx]} accuracy: {metric.result()}")


def train(num_epochs):
    model = MovieLensModel(user_model, movie_model, task, False)
    model.compile(optimizer=keras.optimizers.Adagrad(0.5))

    # Train

    model.fit(
        ratings.map(
            lambda x: {
                "user_id": x["user_id"],
                "movie_title": x["movie_title"],
                "movie_id": x["movie_id"],
            }
        ).batch(4096),
        epochs=num_epochs,
    )

train(1000)

compute_metrics(movies.cardinality().numpy(), movies.cardinality().numpy())

# Concat the dataset into big vector, then find the top-k
def brute_force():
    brute = tfrs.layers.factorized_top_k.BruteForce().index_from_dataset(
        tf.data.Dataset.zip(
            (
                movies.map(lambda x: x["movie_id"]).batch(100),
                movies.map(lambda x: movie_model(x["movie_title"])).batch(100),
            )
        )
    )

    # Get top 10 recommendations.
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))
    _, ids = brute(user_embedding, k=10)
    print(ids)

# Calculate top-k on each batch, then reduce each batch's top-k into 1 result
# The same things as computing metrics
def streaming():
    stream = tfrs.layers.factorized_top_k.Streaming().index_from_dataset(
        tf.data.Dataset.zip(
            (
                movies.map(lambda x: x["movie_id"]).batch(100),
                movies.map(lambda x: movie_model(x["movie_title"])).batch(100),
            )
        )
    )
    # Get top 10 recommendations.
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))
    _, ids = stream(user_embedding, k=10)
    print(ids)

# ScaNN concept
def _scann():
    scann = tfrs.layers.factorized_top_k.ScaNN( 
        num_leaves=1000,
        num_leaves_to_search=100,
        num_reordering_candidates=1000).index_from_dataset(
            tf.data.Dataset.zip(
                (
                    movies.map(lambda x: x["movie_id"]).batch(100),
                    movies.map(lambda x: movie_model(x["movie_title"])).batch(100),
                )
            )
        )
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))

    # initialize after indexing, first time call
    scann(user_embedding, k=10)

    return scann(user_embedding, k=10)

# produce same results
# both is not efficient at scale, but the 2nd approach is better for memory
brute_force()
streaming()
