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


async def load_jsonfiles(files):
    for file in files:
        yield load_jsonfile(file)


async def convert_json2dict(agen_files):
    """ Appends json context to default dictionery """
    dd = defaultdict(list)

    async for afile in agen_files:
        multi_line_jsons = await (afile)

        for line in multi_line_jsons.splitlines():
            for key, value in json.loads(line).items():
                dd[key].append(value)

    return dd


@clock()
async def main():
    files = path.glob("*.json")

    df = await convert_json2dict(load_jsonfiles(files))

    pdf = pd.DataFrame(df)
    print(pdf.head())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        t0 = time.time()
        loop.run_until_complete(main())
        print(f"Main function finished in {time.time() - t0:.2f} sec")

    finally:
        # Shutting down and closing file descriptors after interrupt
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
