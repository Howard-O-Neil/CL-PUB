from this import d
import pandas as pd
import tensorflow_core as tf
import model.tf_preprocess_img as tf_preproc
import model.model_hyper_params as mhp
from model.VGG_16_model import VGG_16
import numpy as np
from datetime import datetime


class cifa10_model:
    def __init__(self, id, checkpoint_dir):
        self.checkpoint_dir = checkpoint_dir
        self.MODEL_ID = id

        self.init_placeholder()
        self.init_params()
        self.init_softmax_parameters()
        
        self.init_model()

        self.sess = None
        self.sess_saver = None

        self.EPOCH_id = tf.Variable(0.0, name="EPOCH_id")

    def init_params(self):
        self.vgg_model = VGG_16()

        self.vgg_model.init_conv_parameters()
        self.vgg_model.init_dense_parameters()

    def init_model(self):
        [self.train_model, self.cost_train] = self.get_model(True)
        [self.predict_model, self.cost_predict] = self.get_model(False)

        self.make_predict = tf.argmax(self.predict_model, axis=1)

        self.train_op = tf.train.AdamOptimizer(learning_rate=mhp.learning_rate).minimize(
            self.cost_train
        )

    def init_placeholder(self):
        # 32 x 32 images
        self.x = tf.placeholder(tf.float32, [None, 3072])

        # one-hot vector
        self.y = tf.placeholder(tf.float32, [None, 10])

    def init_softmax_parameters(self):
        self.SUM_softmax = tf.constant(0.0)
        self.COUNT_softmax = tf.constant(0.0)

        fan_in = mhp.num_dense_neurons[1]
        he_norm = tf.cast(tf.sqrt(tf.divide(2.0, np.float32(fan_in))), tf.float32)

        self.SOFTMAX_w = tf.Variable(
            tf.multiply(tf.random_normal([fan_in, 10]), he_norm), name="SOFTMAX_w"
        )

        self.SOFTMAX_b = tf.Variable(
            tf.multiply(tf.random_normal([10]), he_norm), name="SOFTMAX_b"
        )

        self.SUM_softmax = tf.add(
            self.SUM_softmax,
            tf.reduce_sum(tf.square(tf.nn.bias_add(self.SOFTMAX_w, self.SOFTMAX_b))),
        )
        self.COUNT_softmax = tf.add(
            self.COUNT_softmax, tf.cast(fan_in * 10, dtype=tf.float32)
        )

    def get_session(self):
        return self.sess

    def get_sum_parameters(self):
        return tf.add(
            self.vgg_model.get_sum_params(),
            tf.reduce_sum(tf.square(tf.nn.bias_add(self.SOFTMAX_w, self.SOFTMAX_b))),
        )

    def get_preprocess_tensor(self, is_train):
        preprocess_x = tf_preproc.tf_preprocess_images(self.x, is_train)

        x_scale = None
        batch_x = self.x
        batch_y = self.y

        if is_train:
            x_scale = tf_preproc.tf_augment_images(preprocess_x)
            x_data_shape = tf.shape(x_scale)
            x_data_size = tf.squeeze(tf.slice(x_data_shape, [0], [1]))

            y_data_shape = tf.shape(self.y)
            y_data_size = tf.squeeze(tf.slice(y_data_shape, [0], [1]))

            y_augments = tf.tile(
                self.y, [tf.cast(tf.divide(x_data_size, y_data_size), tf.int32), 1]
            )

            shuffle_index = tf.random.shuffle(tf.range(0, x_data_size, 1))

            batch_x = tf.gather(x_scale, shuffle_index)
            batch_y = tf.gather(y_augments, shuffle_index)
        else:
            batch_x = preprocess_x

        return [batch_x, batch_y]

    def get_model(self, is_train):

        [batch_x, batch_y] = self.get_preprocess_tensor(is_train)

        vgg = self.vgg_model.get_model(batch_x, is_train)

        model = tf.nn.bias_add(tf.matmul(vgg, self.SOFTMAX_w), self.SOFTMAX_b)

        if is_train:
            return [
                model,
                tf.add(
                    tf.reduce_mean(
                        tf.nn.softmax_cross_entropy_with_logits_v2(
                            labels=batch_y, logits=model
                        )
                    ),
                    tf.multiply(mhp.lambda_val, self.get_sum_parameters()),
                ),
            ]
        else:
            return [
                model,
                tf.reduce_mean(
                    tf.nn.softmax_cross_entropy_with_logits_v2(
                        labels=batch_y, logits=model
                    )
                ),
            ]

    def compare_dif_result(self, x1, x2, is_count):
        total_same_val = 0
        for i, x in enumerate(x1):
            if x1[i].astype(np.int32) == x2[i].astype(np.int32):
                total_same_val += 1

        if is_count:
            return total_same_val
        return total_same_val / x1.shape[0]

    def cost_by_batch(self, x_dataset, y_dataset, bs):
        ttb = y_dataset.shape[0] // bs + 1

        if y_dataset.shape[0] % bs == 0:
            ttb -= 1

        avg_res = 0.0
        for i in range(0, ttb):
            offset = (i * bs) % y_dataset.shape[0]
            bx = x_dataset[offset : offset + bs, :]
            by = y_dataset[offset : offset + bs, :]

            avg_res += self.sess.run(
                self.cost_predict, feed_dict={self.x: bx, self.y: by}
            )

        return avg_res / ttb

    def precision_by_batch(self, x_dataset, y_dataset, bs):
        ttb = y_dataset.shape[0] // bs + 1

        if y_dataset.shape[0] % bs == 0:
            ttb -= 1

        avg_res = 0
        for i in range(0, ttb):
            offset = (i * bs) % y_dataset.shape[0]
            bx = x_dataset[offset : offset + bs, :]
            by = y_dataset[offset : offset + bs]

            avg_res += self.compare_dif_result(
                self.sess.run(self.make_predict, feed_dict={self.x: bx}), by, True
            )

        return avg_res / y_dataset.shape[0]

    def init_session(self):
        self.sess = tf.Session()
        self.sess.run(tf.global_variables_initializer())

        self.sess_saver = tf.train.Saver()

    def restore_session(self, dir):
        self.sess_saver.restore(self.sess, dir)

    def save_model(self):
        self.sess_saver.save(
            self.sess,
            f"{self.checkpoint_dir}/model{self.MODEL_ID}/obj-detect-{self.MODEL_ID}",
        )

    def training_loop(
        self, data, labels, y_train_one_hot, t_data_eva, t_labels_eva, y_test_one_hot_eva
    ):
        total_batch = (data.shape[0] // mhp.batch_size) + 1

        if data.shape[0] % mhp.batch_size == 0:
            total_batch -= 1

        print("TOTAL BATCH: ", total_batch)
        print(mhp.training_epochs)

        print(f"===== [START TRAINING] {str(datetime.now())} =====")
        print()

        stable_cost_variance_rate = 0.000001

        self.avg_train_cost = []
        self.avg_evaluate_cost = []
        self.avg_evaluate_precision = []
        self.avg_train_precision = []

        for j in range(0, mhp.training_epochs):
            avg_train_b = []
            train_precision = 0

            for i in range(0, total_batch):
                offset = (i * mhp.batch_size) % labels.shape[0]

                batch_data = data[offset : offset + mhp.batch_size, :]
                batch_onehot_vals = y_train_one_hot[offset : offset + mhp.batch_size, :]

                _, cost_val, predict_res = self.sess.run(
                    [self.train_op, self.cost_train, self.make_predict],
                    feed_dict={self.x: batch_data, self.y: batch_onehot_vals},
                )

                print(cost_val)

                avg_train_b.append(cost_val)
                train_precision += self.compare_dif_result(
                    predict_res, labels[offset : offset + mhp.batch_size], True
                )

            print(f"=== [START EVALUATE] {str(datetime.now())} ===")
            avg_train = np.mean(np.array(avg_train_b))
            avg_evaluate = self.cost_by_batch(t_data_eva, y_test_one_hot_eva, 128)
            avg_train_preci = train_precision / data.shape[0]
            avg_evaluate_preci = self.precision_by_batch(t_data_eva, t_labels_eva, 128)

            self.avg_train_cost.append(avg_train)
            self.avg_evaluate_cost.append(avg_evaluate)
            self.avg_train_precision.append(avg_train_preci)
            self.avg_evaluate_precision.append(avg_evaluate_preci)

            print("Epoch {}. AVG train cost {}".format(j, avg_train))
            print("Epoch {}. Evaluation cost {}".format(j, avg_evaluate))
            print("Epoch {}. Train precision {}".format(j, avg_train_preci))
            print("Epoch {}. Evaluation precision {}".format(j, avg_evaluate_preci))

            print(f"=== [END EVALUATE] {str(datetime.now())} ===")

            self.sess.run(tf.assign(self.EPOCH_id, tf.add(self.EPOCH_id, 1.0)))

            # shuffle all the dataset
            arr_idx_t = np.arange(data.shape[0])
            np.random.shuffle(arr_idx_t)

            data = data[arr_idx_t]
            labels = labels[arr_idx_t]

            y_train_one_hot = y_train_one_hot[arr_idx_t]

        print(f"===== [END TRAINING] {str(datetime.now())} =====")
        # sess.close()

    def save_metrics(self):
        pd_df = pd.DataFrame(
            {
                "avg_train_cost": np.array(self.avg_train_cost),
                "avg_evaluate_cost": np.array(self.avg_evaluate_cost),
                "avg_train_precision": np.array(self.avg_train_precision),
                "avg_evaluate_precision": np.array(self.avg_evaluate_precision),
            }
        )
        pd_df.to_csv(
            f"{self.checkpoint_dir}/model{self.MODEL_ID}/metrics-{self.sess.run(self.EPOCH_id)}.csv"
        )
