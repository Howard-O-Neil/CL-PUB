Using ANN (Approximate Nearest Neighbor Searching) in serving in production

Basically we perform indexing the dataset of vector to boost vector similarity searching

Google research group propose ScaNN concept in ANN. But within ANN there are more concepts to check out

## Results in seconds


| Algorithm | Time |
| :---         |     :---      |
| Bruteforce  | 0.029018163681030273     | 
| Streaming     | 15.509445428848267      |
| ScaNN 100 leaves | 0.0023899078369140625 |
|ScaNN 1000 leaves | 0.0025663375854492188 |

## Other than ScaNN
I also check other ANN library other than ScaNN, specifically Faiss, Annoy

For testing each ANN library, i choose 2 user "42" and "43", collecting all ground-truth movies interactions. Compare percentage of intersection between ANN top-k recommendations vs ground-truths at each training epoch.

| Algorithm | Status |
| :---         |     :---      |
| ScaNN  | slow index building, low accuracy     | 
| Annoy (inner product)     | fast index building, too high accuracy      |
| Faiss (inner product) | fast index building, high accuracy |
| Faiss (inner product + VORONOI cells) | fast index building, high accuracy |

Faiss VORONI cells is faster with trade off a little bit slower accuracy
