import sys

sys.path.append("../..")

from pprint import pprint
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import tensorflow as tf
import numpy as np
import pandas as pd
from tensorflow import keras

# each JSON is small, there's no need in iterative processing
import json

with open("/recsys/data/arxiv/arxiv-metadata-oai-snapshot.json", 'r') as f:
    print(f"Total paper: {sum(1 for _ in f)}")

with open("/recsys/data/arxiv/arxiv-metadata-oai-snapshot.json", 'r') as f:
    for i, line in enumerate(f):
        data = json.dumps(json.loads(line), indent=4)

        if i <= 3:
            # Writing to sample.json
            with open("/recsys/prototype/read_arxiv/sample.json", "a") as outfile:
                outfile.write(data)
        else: exit()
