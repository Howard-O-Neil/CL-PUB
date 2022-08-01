import json
import spacy
import re
import threading
import time
from spacy.matcher import Matcher

model_vocab = []

# max stable threads for processors + rams of my machine
# each thread process 21000 doc
num_threads = 2500
batch_docs = 850


def parse_vocab_thread(thread_id):
    nlp = spacy.load("en_core_web_sm")
    matcher = Matcher(nlp.vocab)

    pattern1 = [{"POS": "NOUN"}]
    pattern2 = [{"POS": "VERB"}]

    # add match string id for each pattern
    matcher.add("MATCHED_NOUN", [pattern1])
    matcher.add("MATCHED_VERB", [pattern2])

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

                doc = nlp(re.sub(r"[^\w\s]", "", " ".join(list_paragraph)))
                matches = matcher(doc)
                for match_id_num, start, end in matches:
                    match_id_str = nlp.vocab.strings[match_id_num]
                    span = doc[start:end]  # The matched span

                    print(match_id_num, match_id_str, start, end, span.text)
                    vocab.append(span.text)

                # Write vocab to file
                with open(f"/recsys/data/arxiv/vocab-{thread_id}.txt", "a") as output:
                    for row in vocab:
                        output.write(str(row) + "\n")
                    output.flush()

                list_paragraph = []
                vocab = []

        arxiv_read.flush()

start_time = time.time()

batch_thread = 10
threads = []
for idx in range(batch_thread):
    x = threading.Thread(target=parse_vocab_thread, args=(idx + 1,))
    threads.append(x)
    x.start()

for idx in range(batch_thread):
    threads[idx].join()

end_time = time.time()
print(f"{batch_thread} THREADS Execution time: {end_time - start_time}")