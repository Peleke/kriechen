import aiofiles
import asyncio
import os
import pathlib

DATA_DIR = "data"

async def read_file(filename: str, mode: str="r") -> str:
  async with aiofiles.open(filename, mode=mode) as f:
    return await f.read()

async def main():
  files = [f"{DATA_DIR}/{filename}" for filename in os.listdir(pathlib.Path(DATA_DIR))]

  read_tasks = [asyncio.create_task(read_file(file)) for file in files]

  results = await asyncio.gather(*read_tasks)

  print(results)

asyncio.run(main())