import sys

sys.path.append("../..")

from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
import keras

import numpy as np

from model.movielen_model import MovieLensModel

tf.random.set_seed(42)

def log_out(dataset, count=10):
    for x in dataset.take(count).as_numpy_iterator():
        print(x)

# For each integer from StringLookup
#   create a params vector of size = embedding_dimension
def create_embedding_model(data, embedding_dimension=32):
    return keras.Sequential([
        keras.layers.StringLookup(
            vocabulary=data, mask_token=None),
        keras.layers.Embedding(data.shape[0] + 1, embedding_dimension)
    ])

# Ratings 
ratings = tfds.load("movielens/100k-ratings", split="train")

movies = tfds.load("movielens/100k-movies", split="train")


# Select basic features
ratings = ratings.map(
    lambda x: {"movie_title": x["movie_title"], "user_id": x["user_id"]}
)
movies = movies.map(lambda x: x["movie_title"])

shuffled = ratings.shuffle(100_000, seed=42, reshuffle_each_iteration=False)

train = shuffled.take(80_000)
test = shuffled.skip(80_000).take(20_000)

movie_titles = movies.batch(1_000)
user_ids = ratings.batch(1_000_000).map(lambda x: x["user_id"])

unique_movie_titles = np.unique(np.concatenate(list(movie_titles)))
unique_user_ids = np.unique(np.concatenate(list(user_ids)))

print(unique_movie_titles[0])

str_lookup = keras.layers.StringLookup(vocabulary=unique_movie_titles, mask_token=None)
embed = keras.layers.Embedding(10, 32)

print(str_lookup(unique_movie_titles[10:11]).numpy())
print(embed(str_lookup(unique_movie_titles[0:2])).shape)

# query tower
user_model = create_embedding_model(unique_user_ids)

# candidate tower
movie_model = create_embedding_model(unique_movie_titles)

print(f"User params  : {user_model(unique_user_ids).shape}")
print(f"Movie params : {movie_model(unique_movie_titles).shape}")

task = tfrs.tasks.Retrieval(
    metrics=tfrs.metrics.FactorizedTopK(
        candidates=movies.batch(128).map(movie_model)
    )
)

# Create a retrieval model.
model = MovieLensModel(user_model, movie_model, task)
model.compile(optimizer=keras.optimizers.Adagrad(0.5))

# Train.
model.fit(ratings.batch(4096), epochs=3)

# Set up retrieval using trained representations.
index = tfrs.layers.ann.BruteForce(model.user_model)
index.index_from_dataset(
    movies.batch(100).map(lambda title: (title, model.movie_model(title)))
)

# Get recommendations.
_, titles = index(np.array(["42"]))
print(f"Recommendations for user 42: {titles[0, :3]}")
