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
        uid = rating["user_id"].numpy().decode("utf-8")
        mov_id = rating["movie_id"].numpy().decode("utf-8")
        _mov_title = rating["movie_title"].numpy().decode("utf-8")

        if uid in user_ids:
            res_dict[uid].append(mov_id)

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

import logging

log_dir = "/recsys/prototype/factorized_top_k/compare_scann.log"
logger = logging.getLogger("TRAINING LOG")
logger.setLevel(logging.DEBUG)
log_filehandler = logging.FileHandler(filename=log_dir, mode="a")
log_filehandler.setFormatter(
    logging.Formatter("%(name)s - %(levelname)s - %(asctime)s - %(message)s")
)
logger.addHandler(log_filehandler)
logger.debug("hello world")


test_dict = get_interaction_list(ratings, ["42", "43"])

logger.debug(f"""User 42 interaction list size {len(test_dict["42"])}""")
logger.debug(f"""User 43 interaction list size {len(test_dict["43"])}""")

class AccuracyPrintingCallback(keras.callbacks.Callback):
    def on_train_batch_end(self, batch, logs=None):
        None

    def on_test_batch_end(self, batch, logs=None):
        None

    def on_epoch_end(self, epoch, logs=None):
        scann = tfrs.layers.factorized_top_k.ScaNN(
            num_leaves=1000, num_leaves_to_search=100, num_reordering_candidates=1000
        ).index_from_dataset(
            tf.data.Dataset.zip(
                (
                    movies.map(lambda x: x["movie_id"]).batch(100),
                    movies.map(lambda x: movie_model(x["movie_title"])).batch(100),
                )
            )
        )

        user_embedding = user_model(tf.convert_to_tensor(["42", "43"]))

        # initialize after indexing, first time call
        scann_res = scann(user_embedding, k=max(len(test_dict["42"]), len(test_dict["43"])))

        test_users = ["42", "43"]
        predict = scann_res[1].numpy().tolist()

        for i, mov_ids in enumerate(predict):
            flag = 0
            for mov_id in mov_ids:
                if mov_id.decode("utf-8") in test_dict[test_users[i]]:
                    flag += 1

            logger.debug(
                f"[EPOCH {epoch}] User {test_users[i]} accuracy = {flag / len(test_dict[test_users[i]])}"
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
        callbacks=[AccuracyPrintingCallback()]
    )

train(1000)
