import asyncio
import json
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
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


async def convert_allfiles(loop):
    file_list = path.glob("*.json")
    executor = ProcessPoolExecutor(max_workers=4)

    to_do = (loop.run_in_executor(executor, convert_onefile, file) for file in file_list)
    default_dicts = await asyncio.gather(*to_do)

    pdf = pd.concat([pd.DataFrame(dd) for dd in default_dicts])
    print(pdf.head())
    print(f"Data length : {len(pdf)}")


def main():
    loop = asyncio.get_event_loop()

    try:
        t0 = time.time()
        loop.run_until_complete(convert_allfiles(loop))
        print(f"Main function finished in {time.time() - t0:.2f} sec")

    finally:
        # Shutting down and closing file descriptors after interrupt
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
