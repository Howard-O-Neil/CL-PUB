import nltk
from nltk.corpus import words

nltk.download("words")

vocab_prefix = "/recsys/data/arxiv/vocab-"

filter_vocab = []

for i in range(1, 2411):  # from file vocab-0 -> vocab-2410
    filter_each = []
    with open(vocab_prefix + str(i) + ".txt", "r") as f:
        filter_each = list(filter(lambda x: len(x) > 3, f.read().split("\n")))
        f.flush()

    filter_vocab.extend(filter_each)

    print(f"Filtered vocab-{i}.txt")

filter_vocab = list(
    filter(
        lambda x: x in words.words() and print(f"Matched word: {x}"),
        list(set(filter_vocab)),
    )
)

print(f"Filtered size: {len(filter_vocab)}")

with open(vocab_prefix + "filtered.txt", "w") as f:
    f.write("")

with open(vocab_prefix + "filtered.txt", "a") as f:
    for row in filter_vocab:
        f.write(str(row).lower() + "\n")
    f.flush()
