#! /usr/bin/env python3

"""Execute notebooks in parallel.

Reads the list of notebooks to execute from the command line.
If no notebooks are specified, all notebooks
in the examples directory are executed.

Notebooks are executed in parallel, with one worker
per processor in the host machine.
"""
import asyncio
from pathlib import Path
import sys
from concurrent.futures import ProcessPoolExecutor

import nbformat
from nbclient import NotebookClient

ROOT_DIR = Path(__file__).resolve().parent.parent


async def run_notebook(notebook_file):
    rel_file_name = notebook_file.relative_to(ROOT_DIR).as_posix()
    print(f'{rel_file_name}: Loading notebook')
    nb = nbformat.read(notebook_file, as_version=4)
    client = NotebookClient(nb, timeout=600, kernel_name='python3', record_timing=False)
    print(f'{rel_file_name}: Executing')
    await client.async_execute()
    print(f'{rel_file_name}: Writing')
    nbformat.write(nb, notebook_file)
    print(f'{rel_file_name}: Finished')
    del nb, client


async def run():
    loop = asyncio.get_running_loop()

    if len(sys.argv) > 1:
        notebooks = (Path(p).resolve() for p in sys.argv[1:])
    else:
        notebooks = ROOT_DIR.joinpath('examples').rglob('*.ipynb')

    with ProcessPoolExecutor() as pool:
        await asyncio.gather(*(
            loop.run_in_executor(pool, run_notebook, notebook)
            for notebook in notebooks
        ))


if __name__ == '__main__':
    asyncio.run(run())
