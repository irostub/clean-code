import asyncio
import json
import multiprocessing
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
from aiofile import AIOFile
from tqdm import tqdm

import log
from clock import clock

path = Path("./data")


def load_jsonfile(file):
    with open(file, "r", encoding="UTF-8") as fp:
        return fp.read()


def convert_onefile(file: Path):
    context = load_jsonfile(file)

    dd = defaultdict(list)
    for line in context.splitlines():
        for key, value in json.loads(line).items():
            dd[key].append(value)

    return dd


def convert_allfiles():
    file_list = path.glob("*.json")

    with multiprocessing.Pool(4) as pool:
        default_dicts = pool.map(convert_onefile, (file for file in file_list))

    pdf = pd.concat([pd.DataFrame(dd) for dd in default_dicts])
    print(pdf.head())
    print(f"Data length : {len(pdf)}")


def main():
    try:
        t0 = time.time()
        convert_allfiles()
        print(f"Main function finished in {time.time() - t0:.2f} sec")

    finally:
        pass


if __name__ == "__main__":
    main()
