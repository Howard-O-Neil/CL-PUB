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
