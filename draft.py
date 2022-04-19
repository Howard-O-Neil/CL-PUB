from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
from tensorflow import keras

import numpy as np

from model.scholar_recsys.time_series_view import TimeSeriesView 
tf.random.set_seed(42)

ratings = tfds.load("movielens/100k-ratings", split="train")

model = TimeSeriesView()

model.fit(
    ratings.map(
        lambda x: {
            "user_id": x["user_id"],
            "movie_title": x["movie_title"],
            "movie_id": x["movie_id"],
        }
    ).take(10).batch(5),
    epochs=1,
)