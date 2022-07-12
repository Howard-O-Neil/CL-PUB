import tensorflow as tf

class TimeSeriesView(tf.keras.Model):
    def __init__(self) -> None:
        pass
    
    def compute_loss(self, inputs, training: bool = False) -> tf.Tensor:
        print(inputs)

    def train_step(self, inputs):
        """Custom train step using the `compute_loss` method."""
        self.compute_loss(inputs)
        # with tf.GradientTape() as tape:
        #     loss = self.compute_loss(inputs, training=True)

        #     # Handle regularization losses as well.
        #     regularization_loss = sum(self.losses)

        #     total_loss = loss + regularization_loss

        # gradients = tape.gradient(total_loss, self.trainable_variables)
        # self.optimizer.apply_gradients(zip(gradients, self.trainable_variables))

        # metrics = {metric.name: metric.result() for metric in self.metrics}
        # metrics["loss"] = loss
        # metrics["regularization_loss"] = regularization_loss
        # metrics["total_loss"] = total_loss

        # return metrics
        return

    def test_step(self, inputs):
        """Custom test step using the `compute_loss` method."""

        # loss = self.compute_loss(inputs, training=False)

        # # Handle regularization losses as well.
        # regularization_loss = sum(self.losses)

        # total_loss = loss + regularization_loss

        # metrics = {metric.name: metric.result() for metric in self.metrics}
        # metrics["loss"] = loss
        # metrics["regularization_loss"] = regularization_loss
        # metrics["total_loss"] = total_loss

        # return metrics
        return

    def call(self, user_features, item_embedding):
        return