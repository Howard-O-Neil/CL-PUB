import sys

sys.path.append("../..")

from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
from tensorflow import keras
from annoy import AnnoyIndex
import random

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

log_dir = "/recsys/prototype/top_k_recommender/compare_annoy.log"
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


def build_annoy_index(candidates_embedding):
    embed_dimension = 32
    t = AnnoyIndex(embed_dimension, "dot")  # index base on dot product

    identifiers_and_candidates = list(candidates_embedding)
    candidates_embedding = np.concatenate(
        [embeddings for _, embeddings in identifiers_and_candidates], axis=0
    )
    identifiers = np.concatenate(
        [identifiers for identifiers, _ in identifiers_and_candidates], axis=0
    )
    for i in range(len(identifiers)):
        t.add_item(i, candidates_embedding[i].tolist())

    n_tree = 10
    t.build(n_tree, n_jobs=-1)  # 10 trees

    return (t, identifiers)


def get_annoy_nns(index, identifiers, list_embedding, k=1000):
    n_tree = 10

    list_ids = []
    list_dis = []
    list_identities = []
    for embed_vec in list_embedding:
        nn = index.get_nns_by_vector(
            embed_vec, n=k, search_k=n_tree * 500, include_distances=True
        )
        list_ids.append(nn[0])
        list_dis.append(nn[1])

    for i in range(len(list_embedding)):
        list_identities.append(identifiers[list_ids[i]].tolist())

    return (list_identities, list_dis)


class AccuracyPrintingCallback(keras.callbacks.Callback):
    def on_train_batch_end(self, batch, logs=None):
        None

    def on_test_batch_end(self, batch, logs=None):
        None

    def on_epoch_end(self, epoch, logs=None):
        (annoy, identifiers) = build_annoy_index(
            tf.data.Dataset.zip(
                (
                    movies.map(lambda x: x["movie_id"]).batch(100),
                    movies.map(lambda x: movie_model(x["movie_title"])).batch(100),
                )
            )
        )
        
        (predict_ids, _) = get_annoy_nns(
            annoy,
            identifiers,
            user_model(tf.convert_to_tensor(["42", "43"])).numpy().tolist(),
        )

        test_users = ["42", "43"]
        predict = predict_ids

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
        callbacks=[AccuracyPrintingCallback()],
    )


train(25)
