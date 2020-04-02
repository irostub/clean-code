import time
import asyncio, uvloop
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm
from aiofile import AIOFile, LineReader
from clock import clock

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
path = Path("/home/shyeon/workspace/python/SparkDefinitiveGuide/data/activity-data")


async def load_json(file):
    async with AIOFile(file, "r", encoding="UTF-8") as afp:
        async for line in LineReader(afp):
            yield json.loads(line)


@clock()
async def main():
    dd = defaultdict(list)
    files = path.glob("*.json")

    for file in files:
        async for line in load_json(file):
            for key, value in line.items():
                dd[key].append(value)

    result = pd.DataFrame(dd)
    print(len(result))
    print(result.head())


if __name__ == "__main__":
    print("started main")

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(
            loop.shutdown_asyncgens()
        )  # see: https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.shutdown_asyncgens
        loop.close()

    print("finished main")
