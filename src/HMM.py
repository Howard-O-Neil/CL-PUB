import numpy as np
import tensorflow as tf

class HMM(object):
    def __init__(self, initial_prob, trans_prob, obs_prob, sess):
        self.N = np.size(initial_prob) # number of state

        self.initial_prob = initial_prob
        self.trans_prob = trans_prob
        self.emission = tf.constant(obs_prob)
        self.sess = sess

        assert self.initial_prob.shape == (self.N, 1) # each state 1 probability
        assert self.trans_prob.shape == (self.N, self.N) # transition matrix size = N x N

        # emission matrix size = N x number of observation type
        assert obs_prob.shape[0] == self.N

        self.obs_idx = tf.placeholder(tf.int32) # observation index?
        self.fwd = tf.placeholder(tf.float64) # forward algorithm cache
        self.viterbi = tf.placeholder(tf.float64)

    def get_emission(self, obs_idx): 

        # emission matrix is 2 dimension, so does slice location, slice_shape

        # feed location
        slice_location = [0, obs_idx]

        num_rows = tf.shape(self.emission)[0]

        # observe through states (vertical slice)
        slice_shape = [num_rows, 1]

        # return slice
        return tf.slice(self.emission, slice_location, slice_shape)

    def forward_init_op(self):
        obs_prob = self.get_emission(self.obs_idx)

        # obs_prob & initial_prob has the same shape
        fwd = tf.multiply(self.initial_prob, obs_prob)  

        return fwd

    def forward_op(self):
        # transpost to conert from (state x 1) to (1 x state)
        transpose_emission = tf.transpose(self.get_emission(self.obs_idx))
        
        # compute transitions
        transitions = tf.matmul(self.fwd, transpose_emission)
        
        # multiply by transition
        weighted_transitions = tf.multiply(transitions, self.trans_prob)
        
        # reduce sum then reshape to (2, 1)
        fwd = tf.reduce_sum(weighted_transitions, 0) 

        return tf.reshape(fwd, tf.shape(self.fwd))

    # the same a as forward op, but find max prob instead of sum
    def decode_op(self):
        transitions = tf.matmul(self.viterbi,
            tf.transpose(self.get_emission(self.obs_idx)))

        weighted_transitions = transitions * self.trans_prob
        viterbi = tf.reduce_max(weighted_transitions, 0)
        return tf.reshape(viterbi, tf.shape(self.viterbi))

    # find max index to predict the weather
    def backpt_op(self):
        back_transitions = tf.matmul(self.viterbi, np.ones((1, self.N)))
        weighted_back_transitions = back_transitions * self.trans_prob

        return tf.argmax(weighted_back_transitions, 0)


def viterbi_decode(sess, hmm, observations):
    viterbi = sess.run(hmm.forward_init_op(), feed_dict={hmm.obs_idx:
        observations[0]})
    
    backpts = np.ones((hmm.N, len(observations)), 'int32') * -1

    for t in range(1, len(observations)):
        viterbi, backpt = sess.run([hmm.decode_op(), hmm.backpt_op()],
            feed_dict={hmm.obs_idx: observations[t],
            hmm.viterbi: viterbi})

        backpts[:, t] = backpt

    tokens = [viterbi[:, -1].argmax()]
    for i in range(len(observations) - 1, 0, -1): # len(obs) - 1 -> 1
        tokens.append(backpts[tokens[-1], i])
    
    return tokens[::-1] # reverse


def forward_algorithm(sess, hmm, observations):
    # forward
    fwd = sess.run(hmm.forward_init_op(), feed_dict={hmm.obs_idx: observations[0]})

    # from 1 to len(observations) - 1
    for t in range(1, len(observations)):
        # ressign, then re-feed forward cache
        fwd = sess.run(hmm.forward_op(), feed_dict={hmm.obs_idx: observations[t], hmm.fwd: fwd})
    
    # the result probability
    prob = sess.run(hmm.forward_op(), feed_dict={hmm.obs_idx: observations[0], hmm.fwd: fwd})
    return prob