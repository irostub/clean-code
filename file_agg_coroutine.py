import time
import asyncio
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from clock import clock

path = Path("/home/shyeon/workspace/python/SparkDefinitiveGuide/data/activity-data")


def load_jsonfile(file):
    with open(file, "r", encoding="UTF-8") as fp:
        yield from fp


#         for line in fp:
#             yield line


def load_jsonfiles(files):
    for file in files:
        yield from load_jsonfile(file)


@clock()
def main():
    dd = defaultdict(list)
    files = path.glob("*.json")

    lines = load_jsonfiles(files)
    for line in lines:
        for key, value in json.loads(line).items():
            dd[key].append(value)

    result = pd.DataFrame(dd)
    print(len(result))
    print(result.head())


if __name__ == "__main__":
    print("started main")
    main()
    print("finished main")
