# THIS CAN BE CODE ON NOTEBOOK

import pickle
import numpy
import numpy as np

import model.file_utils as file_utils

# test local only
import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

MODEL_ID = 3

# t_train_size = 100
# t_test_size = 32

names, data, labels = file_utils.read_data(\
    "data/cifa10", is_train=True)

# data = data[5000:5000+t_train_size]
# labels = labels[5000:5000+t_train_size]

t_names, t_data, t_labels = file_utils.read_data(\
    "data/cifa10", is_train=False)

# t_data = t_data[1000:1000+t_test_size]
# t_labels = t_labels[1000:1000+t_test_size]

evaluation_size = 1000
evaluation_pos = 2000
t_data_eva = t_data[evaluation_pos:evaluation_pos+evaluation_size]
t_labels_eva = t_labels[evaluation_pos:evaluation_pos+evaluation_size]


def convert_to_one_hot(y_dataset, num_labels):
    y_one_hot = np.array([-99.] * num_labels)
    for val in y_dataset:
        one_hot = np.array(\
            [0.] * int(val) + [1.] + [0.] * int(num_labels - val - 1))
        
        y_one_hot = np.vstack((y_one_hot, one_hot))
    
    return y_one_hot[1:] # remove first dummy

y_train_one_hot = convert_to_one_hot(labels, len(names))
y_test_one_hot = convert_to_one_hot(t_labels, len(t_names))
y_test_one_hot_eva = convert_to_one_hot(t_labels_eva, len(t_names))


from model.cifa10_model import cifa10_model

ml_model = cifa10_model(MODEL_ID, "meta")
ml_model.init_session()
# ml_model.restore_session("")

try:
    ml_model.training_loop(data, labels, y_train_one_hot, t_data_eva, t_labels_eva, y_test_one_hot_eva)
except KeyboardInterrupt:
    ml_model.save_model()
    ml_model.save_metrics()

    exit(1)
else:
    print("UNKNOWN ERROR!!!")