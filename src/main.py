import numpy as np
import tensorflow as tf
from HMM import *

"""

states = ('Rainy', 'Sunny')
observations = ('walk', 'shop', 'clean')
start_probability = {'Rainy': 0.6, 'Sunny': 0.4}

transition_probability = {
    'Rainy' : {'Rainy': 0.7, 'Sunny': 0.3},
    'Sunny' : {'Rainy': 0.4, 'Sunny': 0.6},
}

emission_probability = {
    'Rainy' : {'walk': 0.1, 'shop': 0.4, 'clean': 0.5},
    'Sunny' : {'walk': 0.6, 'shop': 0.3, 'clean': 0.1},
}

"""


if __name__ == '__main__':

    # row 0 = Rainy
    # row 1 = Sunny

    initial_prob = np.array([[0.6], [0.4]])
    trans_prob = np.array([[0.7, 0.3], [0.4, 0.6]])
    obs_prob = np.array([[0.1, 0.4, 0.5], [0.6, 0.3, 0.1]])

    observations = [0, 1, 1, 2, 1]

    with tf.Session() as sess:
        hmm = HMM(initial_prob=initial_prob, trans_prob=trans_prob, obs_prob=obs_prob, sess=sess)
        prob = forward_algorithm(sess, hmm, observations)
        seq = viterbi_decode(sess, hmm, observations)

        print('Probability of observing {} is {}'.format(observations, prob))
        print('Most likely hidden states are {}'.format(seq))
