import faiss
import numpy as np

dataSetI    = [
    [.1, .2, .3],
    [.1, .5, .3],
]
dataSetII   = [.4, .5, .6]
# dataSetII   = [.1, .2, .3]

x   = np.array(dataSetI).astype(np.float32)
q   = np.array([dataSetII]).astype(np.float32)

index = faiss.index_factory(3, "L2norm,IVF1,Flat", faiss.METRIC_INNER_PRODUCT)

index.train(x)
index.add(x)

distance, index = index.search(q, 2)

print('Distance by FAISS:{}'.format(distance))
print('Index    by FAISS:{}'.format(index))
#To Tally the results check the cosine similarity of the following example

from scipy import spatial

# Cosine_distance = 1 - cosine_similarity
result = [
    1 - spatial.distance.cosine(dataSetI[0], dataSetII),
    1 - spatial.distance.cosine(dataSetI[1], dataSetII)
] 
print('Distance by SCIPY:{}'.format(result))