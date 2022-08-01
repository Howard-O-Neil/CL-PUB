# Index factory is all about indexing (partitioning), transforming, encoding
# These are nothing related to vector distance

# All index support L2 and InnerProduct distance
# Other distance metrics is supported only by IndexFlat

# =====

# An index can act as
# + coarse quantizer
# + sub index

# You can use index factory string, or pre-post-processing

# Vector transform + encoding 
# Exhaustive search
index = index_factory(128, "PCA80,Flat")
# or write this under pre-post-processing

sub_index = faiss.IndexFlatL2 (128)
# PCA 128->80
pca_matrix = faiss.PCAMatrix (128, 80, 0, True)
#- the wrapping index
index = faiss.IndexPreTransform (pca_matrix, sub_index)


# Vector transform + IMI Indexing + Encoding
# Index search
index = index_factory(128, "OPQ16_64,IMI2x8,PQ8+16")

# Vector transform + IVF Indexing + Encoding + Suffixes
# Index search
index = index_factory(128, "OPQ16_64,IVF262144(IVF512,PQ32x4fs,RFlat),PQ16x4fsr,Refine(OPQ56_112,PQ56)")


# <<<<< Quick prototype <<<<<

import numpy as np
import faiss
from faiss import index_factory

feature_dimen = 66
num_vect = 3000000

x_train = np.random.randn(num_vect, feature_dimen).astype(np.float32)
query = np.random.randn(1, feature_dimen).astype(np.float32)

num_cluster = 10000
num_search = int(num_cluster * 0.2) # 20% Database

index = index_factory(feature_dimen, f"OPQ16_64,IVF{num_cluster}_HNSW32,PQ16x4fs")

print("===== Training... =====")

index.train(x_train)
index.add(x_train)

np.savetxt("/home/howard/recsys/faiss/re4/sample_query.out", query, delimiter=",", fmt="%s")
faiss.write_index(index, "/home/howard/recsys/faiss/re4/sample_search.index")

index.nprobe = num_search

res = index.search(query, 100)

# ===== Suppose we just turn of the shell, all the input data is gone

import numpy as np
import faiss
from faiss import index_factory

index = faiss.read_index("/home/howard/recsys/faiss/re4/sample_search.index")
query = np.expand_dims(
    np.loadtxt("/home/howard/recsys/faiss/re4/sample_query.out", delimiter=",", dtype=np.float32),
    axis=0
)
res = index.search(query, 100)

# =====

# >>>>>
