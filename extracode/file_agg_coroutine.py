import time
import asyncio
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from clock import clock

path = Path("./data")


def load_jsonfile(file):
    with open(file, "r", encoding="UTF-8") as fp:
        yield from fp


def load_jsonfiles(files):
    for file in files:
        yield from load_jsonfile(file)


def convert_json_to_dict(jsons):
    """ Appends json context to default dictionery """

    dd = defaultdict(list)

    for json_context in jsons:
        for key, value in json.loads(json_context).items():
            dd[key].append(value)

    return dd


@clock()
def main():

    files = path.glob("*.json")
    df = convert_json_to_dict(load_jsonfiles(files))

    pdf = pd.DataFrame(df)
    print(pdf.head())


if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"Main function finished in {time.time() - t0:.2f} sec")
