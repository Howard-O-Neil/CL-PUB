import json
import nltk
import re
import threading
import time

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

model_vocab = []

# max stable threads for processors + rams of my machine
# each thread process 21000 doc
num_threads = 2500
batch_docs = 850

def parse_vocab_thread(thread_id):
    limit_id = thread_id * batch_docs
    start_id = (thread_id - 1) * batch_docs

    vocab = []
    list_paragraph = []
    with open("/recsys/data/arxiv/arxiv-metadata-oai-snapshot.json", "r") as arxiv_read:
        for i, line in enumerate(arxiv_read):
            if i < start_id:
                continue
            if i == start_id:
                with open(f"/recsys/data/arxiv/vocab-{thread_id}.txt", "w") as f:
                    f.write("")  # reset
                    f.flush()
            if i >= limit_id:
                break

            # data = json.dumps(json.loads(line), indent=4)
            data = json.loads(line)

            # print(data)
            list_paragraph.append(
                data["title"].replace("\n", " ")
                + " "
                + data["abstract"].replace("\n", " ")
                + " "
            )
            if i % 100 == 0 and i > 0:
                print(f"Extracting json {i} ...")

                doc = nltk.word_tokenize(re.sub(r"[^A-Za-z0-9]+", " ", " ".join(list_paragraph)))
                pos_tagged = nltk.pos_tag(doc)
                noun_verbs = filter(lambda x: x[1] == 'NN' and len(x[0]) > 3, pos_tagged)

                for item in noun_verbs:
                    print(f"MATCHED {item[1]} - {item[0]}")
                    vocab.append(item[0])

                # Write vocab to file
                with open(f"/recsys/data/arxiv/vocab-{thread_id}.txt", "a") as output:
                    for row in vocab:
                        output.write(str(row).lower() + "\n")
                    output.flush()

                list_paragraph = []
                vocab = []

        arxiv_read.flush()

batch_thread = 20

for i in range(int(num_threads / batch_thread)):
    num_batch = (i + 1) * batch_thread
    threads = []

    start_time = time.time()

    for idx in range(i * batch_thread, num_batch):
        x = threading.Thread(target=parse_vocab_thread, args=(idx + 1,))
        threads.append(x)
        x.start()

    for idx in range(batch_thread):
        threads[idx].join()

    end_time = time.time()
    print(f"===== BATCH THREAD {i} Execution time: {end_time - start_time}")
