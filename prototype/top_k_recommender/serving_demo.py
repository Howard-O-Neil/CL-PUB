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

def get_interaction_list(ratings, user_ids):
    res_dict = {}
    for id in user_ids:
        res_dict[id] = []

    for rating in list(ratings):
        uid         = rating["user_id"].numpy().decode("utf-8")
        mov_id      = rating["movie_id"].numpy().decode("utf-8")
        mov_title   = rating["movie_title"].numpy().decode("utf-8")

        if uid in user_ids:
            res_dict[uid].append({
                "id": mov_id,
                "title": mov_title
            })
        
    return res_dict

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


train(1)

dummy_ids = (
    movies.map(lambda x: x["movie_id"])
    .batch(4096)
    .concatenate(
        movies.map(lambda x: x["movie_id"])
        .repeat(1000)
        .batch(4096)
        .map(lambda x: tf.zeros_like(x)),
    )
)

dummy_embed = (
    movies.map(lambda x: x["movie_title"])
    .batch(4096)
    .map(movie_model)
    .concatenate(
        movies.map(lambda x: x["movie_title"])
        .repeat(1000)
        .batch(4096)
        .map(movie_model)
        .map(lambda x: x * tf.random.uniform(tf.shape(x))),
    )
)

import time

# Slow
def brute_force():
    brute = tfrs.layers.factorized_top_k.BruteForce().index_from_dataset(
        tf.data.Dataset.zip((dummy_ids, dummy_embed))
    )
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))
    
    start = time.time()
    print(brute(user_embedding, k=10))
    end = time.time()
    print(f"[Elapse time] {end - start}")

# Same results as bruteforce, but more memory-effective as operation run on each batch then reduced into 1
# Very slow
def streaming():
    stream = tfrs.layers.factorized_top_k.Streaming().index_from_dataset(
        tf.data.Dataset.zip((dummy_ids, dummy_embed))
    )
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))

    start = time.time()
    print(stream(user_embedding, k=10))
    end = time.time()
    print(f"[Elapse time] {end - start}")

# Very fast, but a little bit accuracy trade off
def scann_compute_1():
    # num_reordering should be higher than k value, as proposed from official scann 
    scann1 = tfrs.layers.factorized_top_k.ScaNN(num_reordering_candidates=1000).index_from_dataset(
        tf.data.Dataset.zip((dummy_ids, dummy_embed))
    )
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))

    # initialize after indexing, first time call
    scann1(user_embedding, k=10)

    start = time.time()
    print(scann1(user_embedding, k=10))
    end = time.time()
    print(f"[Elapse time] {end - start}")

# A little bit slower than scann1
# by default scann1 has 
#   100 leaves              the algorithm index the database of vector into 100 partitions
#   10  leaves to search    the algorithm evaluate and choose top 10 most promising leaves to begin searching
#                           which also mean that the algorithm just search on 10/100 = 10% data of a whole dataset
# deploy scann2, more leaves and more leaves to search. But still remain the same searching space
def scann_compute_2():
    scann2 = tfrs.layers.factorized_top_k.ScaNN( 
        num_leaves=1000,
        num_leaves_to_search=100,
        num_reordering_candidates=1000).index_from_dataset(
            tf.data.Dataset.zip((dummy_ids, dummy_embed))
        )
    user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))

    # initialize after indexing, first time call
    scann2(user_embedding, k=10)

    start = time.time()
    print(scann2(user_embedding, k=10))
    end = time.time()
    print(f"[Elapse time] {end - start}")


# print("Bruteforce executing ...")
# brute_force()

# print("Streaming executing ...")
# streaming()

print("Scann1 executing ...")
scann_compute_1()

# print("Scann2 executing ...")
# scann_compute_2()