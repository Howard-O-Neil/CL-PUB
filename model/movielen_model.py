import typing
import tensorflow_recommenders as tfrs
import tensorflow as tf
import tensorflow.keras as keras

from typing import Dict, Text


class MovieLensModel(tfrs.Model):
    def __init__(
        self,
        user_model: keras.Model,
        movie_model: keras.Model,
        task: tfrs.tasks.Retrieval,
        compute_metrics: bool,
    ):
        super().__init__()

        # Set up user and movie representations.
        self.user_model = user_model
        self.movie_model = movie_model

        # Set up a retrieval task.
        self.task = task

        # Turn off metrics computation while training
        self.compute_metrics = compute_metrics

    def compute_loss(
        self, features: Dict[Text, tf.Tensor], training=False
    ) -> tf.Tensor:
        # Define how the loss is computed.

        user_embeddings = self.user_model(features["user_id"])
        movie_embeddings = self.movie_model(features["movie_title"])
        
        return self.task(
            query_embeddings=user_embeddings,
            candidate_embeddings=movie_embeddings,
            compute_metrics=self.compute_metrics,
        )
