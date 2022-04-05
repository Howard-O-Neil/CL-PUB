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


Further results could be investigated when we dive more into ANN with more concepts. 

(I don't really know how ScaNN work underlying)