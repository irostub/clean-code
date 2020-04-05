import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import uvloop
from aiofile import AIOFile, LineReader
from tqdm import tqdm

import log
from clock import clock

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
path = Path("./data")


async def load_jsonfile(file):
    async with AIOFile(file, "r", encoding="UTF-8") as afp:
        return await afp.read()


async def convert_onefile(file: Path):
    context = await load_jsonfile(file)

    dd = defaultdict(list)
    for line in context.splitlines():
        for key, value in json.loads(line).items():
            dd[key].append(value)

    return dd


async def convert_allfiles():
    file_list = path.glob("*.json")

    to_do = (convert_onefile(file) for file in file_list)
    default_dicts = await asyncio.gather(*to_do)

    pdf = pd.concat([pd.DataFrame(dd) for dd in default_dicts])
    print(pdf.head())
    print(len(pdf))


def main():
    loop = asyncio.get_event_loop()
    try:
        t0 = time.time()
        loop.run_until_complete(convert_allfiles())
        print(f"Main function finished in {time.time() - t0:.2f} sec")

    finally:
        # Shutting down and closing file descriptors after interrupt
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
