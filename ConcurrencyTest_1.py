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
path = Path("/home/shyeon/workspace/python/SparkDefinitiveGuide/data/activity-data")


async def load_json(file):
    async with AIOFile(file, "r", encoding="UTF-8") as afp:
        return await afp.read()


async def load_jsons(files):
    for file in files:
        return await load_json(file)


@clock()
async def main():
    dd = defaultdict(list)
    files = path.glob("*.json")

    multi_json_lines = await load_jsons(files)
    for line in multi_json_lines.splitlines():
        for key, value in json.loads(line).items():
            dd[key].append(value)

    result = pd.DataFrame(dd)
    print(len(result))
    print(result.head())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        # Shutting down and closing file descriptors after interrupt
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
