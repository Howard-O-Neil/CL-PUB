import sys

import copy
import uuid
import os

vocab_dir = "/home/howard/recsys/vocab"

list_vocabs = os.listdir(vocab_dir)

if "merge-0.txt" in list_vocabs:
    print("===== Collecting existed merge =====")

    f = open(f"{vocab_dir}/merge-0.txt", "r")
    unique_words = f.read().split("\n")
    f.close()
else:
    print("===== Merging multiples =====")

    list_words = []
    
    for file in list_vocabs:
        f = open(f"{vocab_dir}/{file}", "r")
        
        for w in f.read().split("\n"):
            list_words.append(w)
        
        f.close()

        unique_words = list(set(list_words))
        words_content = "\n".join(unique_words)

        f2 = open(f"{vocab_dir}/merge-0.txt", "w+")
        f2.write(words_content)

        f2.close()

import tensorflow as tf
from faker import Faker

fake = Faker()

def draft_text_vectorize():
    vocab_dataset = tf.data.Dataset.from_tensor_slices(unique_words)

    vectorize_layer = tf.keras.layers.TextVectorization(
        pad_to_max_tokens=True,
        standardize='lower_and_strip_punctuation',
        split='whitespace',
        max_tokens=len(unique_words) + 1,
        output_mode='count')

    avg_layer = tf.keras.layers.AveragePooling1D(
        pool_size=8258,
        strides=8255)

    vectorize_layer.adapt(vocab_dataset.batch(2048))

    print("===== Init Done =====")

    _input = [['i have acne motivated perceptually accelerator qhert']]

    for i in range(500):
        _input.append([fake.sentence()])

    text_bow_vect = vectorize_layer(_input)
    avg_vect = avg_layer(tf.reshape(text_bow_vect, [len(_input), tf.gather(tf.shape(text_bow_vect), 1), 1])) 
    output_vect = tf.reshape(avg_vect, [len(_input), tf.gather(tf.shape(avg_vect), 1)])
    
    print(output_vect)
    print(output_vect.numpy())
    print(len(unique_words))

draft_text_vectorize()
