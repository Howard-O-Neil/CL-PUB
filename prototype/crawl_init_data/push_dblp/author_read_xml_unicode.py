import re

source = "/recsys/data/dblp/xad"

with open(source, "r") as test_read:
    for i, line in enumerate(test_read):
        x = re.findall("<author>(.*?)</author>", line)
        print(x)
        if i == 100:
            break
