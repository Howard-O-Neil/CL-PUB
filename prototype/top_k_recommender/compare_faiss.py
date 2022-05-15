import sys

sys.path.append("../..")

from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
from tensorflow import keras
from annoy import AnnoyIndex
import faiss
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
    for i, id in enumerate(user_ids):
        res_dict[id] = np.concatenate(
            [
                [mov_id.numpy()]
                for mov_id in list(
                    ratings.filter(
                        lambda x: tf.math.equal(x["user_id"], user_ids[i])
                    ).map(lambda x: x["movie_id"])
                )
            ],
            axis=0,
        )
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

log_dir = "/recsys/prototype/top_k_recommender/compare_faiss.log"
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

max_search_cap = 0.5


def build_faiss_index(candidates_embedding):
    embed_dimension = 32

    identifiers_and_candidates = list(candidates_embedding)
    candidates_embedding = np.concatenate(
        [embeddings for _, embeddings in identifiers_and_candidates], axis=0
    )
    identifiers = np.concatenate(
        [identifiers for identifiers, _ in identifiers_and_candidates], axis=0
    )

    # number of centroids, each centroids contain at leasts 39 vectors
    nlist = int(len(candidates_embedding) / 39)

    # quantizer = faiss.IndexFlatIP(embed_dimension)
    # index = faiss.IndexIVFFlat(quantizer, embed_dimension, nlist)

    index = faiss.IndexFlatIP(embed_dimension)
    
    # index.train(candidates_embedding)
    # index.nprobe = int(
    #     nlist * max_search_cap
    # )  # max searching = (max_search_cap * 100)%

    # i have no luck with embedding custom ids into index
    # use ordinal as the id, first vector gets 0 then second get 1 ...
    index.add(candidates_embedding)

    return (index, identifiers)


def get_faiss_nns(index, identifiers, list_embedding, k=500):

    if k > len(identifiers) * max_search_cap:
        k = int(k * max_search_cap)

    nn = index.search(list_embedding, k)

    return (identifiers[nn[1]], nn[0])


class AccuracyPrintingCallback(keras.callbacks.Callback):
    def on_train_batch_end(self, batch, logs=None):
        None

    def on_test_batch_end(self, batch, logs=None):
        None

    def on_epoch_end(self, epoch, logs=None):
        (faiss, identifiers) = build_faiss_index(
            tf.data.Dataset.zip(
                (
                    movies.map(lambda x: x["movie_id"]).batch(100),
                    movies.map(lambda x: movie_model(x["movie_title"])).batch(100),
                )
            )
        )

        (predict_ids, _) = get_faiss_nns(
            faiss,
            identifiers,
            user_model(tf.convert_to_tensor(["42", "43"])).numpy(),
        )

        test_users = ["42", "43"]

        for i, user in enumerate(test_users):
            # intersect between ground-truth & prediction
            percentage = len(list(set(test_dict[user]) & set(predict_ids[i]))) / len(
                test_dict[user]
            )
            logger.debug(f"[EPOCH {epoch}] User {user} accuracy = {percentage}")


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
