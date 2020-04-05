import time
import asyncio, uvloop
import json
import pandas as pd
from collections import defaultdict
from pathlib import Path
from tqdm import tqdm
from aiofile import AIOFile, LineReader

import log
from clock import clock


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
path = Path("./data")


async def load_jsonfile(file):
    async with AIOFile(file, "r", encoding="UTF-8") as afp:
        return await afp.read()


async def convert_onefile(file: Path, dd: defaultdict):
    context = await load_jsonfile(file)

    for line in context.splitlines():
        for key, value in json.loads(line).items():
            dd[key].append(value)


async def convert_allfiles():
    file_list = path.glob("*.json")
    dd = defaultdict(list)

    to_do = [convert_onefile(file, dd) for file in file_list]
    await asyncio.gather(*to_do)

    pdf = pd.DataFrame(dd)
    print(len(pdf))
    print(pdf.head())


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
