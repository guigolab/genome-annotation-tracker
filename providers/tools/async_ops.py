import asyncio
import aiohttp
from datetime import datetime
import hashlib
from typing import List

async def fetch_data_many(
    tuples: list[tuple[str, str]], 
    fetch_func: callable, 
    concurrency: int = 20
) -> list[tuple[str, str]]:
    """
    Generic function to fetch data for multiple URLs with concurrency control.
    
    Args:
        tuples: List of (url, unique identifier) tuples
        fetch_func: Async function that takes (session, url) and returns data
        concurrency: Maximum number of concurrent requests
    
    Returns:
        List of (unique identifier, data) tuples for successful requests
    """
    results = []
    sem = asyncio.Semaphore(concurrency)
    
    async with aiohttp.ClientSession() as session:
        async def bound_fetch(session, url, unique_identifier):
            async with sem:
                data = await fetch_func(session, url)
                if data:
                    results.append((unique_identifier, data))
        
        await asyncio.gather(*(bound_fetch(session, url, unique_identifier) for url, unique_identifier in tuples))
    
    return results



async def get_last_modified_date(session: aiohttp.ClientSession, ftp_path: str) -> str:
    """
    This function fetches the last modified date of the annotation file from ncbi ftp server.
    """
    try:
        async with session.head(ftp_path, timeout=20) as resp:
            dt = datetime.strptime(resp.headers.get("Last-Modified"), "%a, %d %b %Y %H:%M:%S %Z")
            return dt.date().isoformat()
    except Exception:
        return None

async def check_last_modified_date_many(tuples: list[tuple[str, str]], concurrency: int = 20) -> list[tuple[str, str]]:
    """
    Check if a list of urls are present and return the last modified date.
    Returns a list of tuples (unique identifier, last_modified) of the urls that are present.
    """
    return await fetch_data_many(tuples, get_last_modified_date, concurrency)



# Chunk sizes (tune as needed)
DL_CHUNK = 1 << 20       # 1 MiB
READ_CHUNK = 1 << 20     # 1 MiB
DEFAULT_CONCURRENCY = 32
DEFAULT_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)

async def run_decompressor(cmd: List[str]) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

async def stream_hash(session: aiohttp.ClientSession, url: str, decomp_cmd: List[str]) -> str | None: 
    """
    Stream URL -> decompressor stdin; read decompressor stdout -> md5
    Returns: md5_hex or None if error
    """
    for attempt in range(1, DEFAULT_RETRIES + 1):
        h = hashlib.md5()
        total = 0
        proc = None
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()

                proc = await run_decompressor(decomp_cmd)

                # two coroutines: writer (to proc.stdin) and reader (from proc.stdout)
                async def writer():
                    try:
                        async for chunk in resp.content.iter_chunked(DL_CHUNK):
                            if not chunk:
                                continue
                            proc.stdin.write(chunk)
                            await proc.stdin.drain()
                    finally:
                        if proc.stdin and not proc.stdin.is_closing():
                            proc.stdin.close()

                async def reader():
                    nonlocal total, h
                    while True:
                        chunk = await proc.stdout.read(READ_CHUNK)
                        if not chunk:
                            break
                        h.update(chunk)
                        total += len(chunk)

                await asyncio.gather(writer(), reader())

                # drain stderr (don’t block forever)
                try:
                    _ = await asyncio.wait_for(proc.stderr.read(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

                ret = await proc.wait()
                if ret != 0:
                    # try to capture a small stderr sample
                    return 

                md5_hex = h.hexdigest()
                return md5_hex

        except Exception as e:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass

        await asyncio.sleep(min(2 * attempt, 10))

    return None

async def worker(name: int, sem: asyncio.Semaphore, session: aiohttp.ClientSession,
                 urls: List[str], decomp_cmd: List[str], out_tuples: list[tuple[str, str]]):
    for url in urls:
        async with sem:
            res = await stream_hash(session, url, decomp_cmd)
            if res: 
                out_tuples.append((url, res))

def chunk(lst, n):
    k = (len(lst) + n - 1) // n
    for i in range(0, len(lst), k):
        yield lst[i:i+k]

async def stream_md5_checksum_many(input_tuples: list[tuple[str, str]], concurrency: int = 20):
    """
    This function fetches the md5 checksum of the uncompressed content of the urls in the tuples.
    """
    decomp_cmd = ["bgzip", "-dc"]
    # common choices:
    #   "bgzip -dc" (recommended for bgzf/gz)
    #   "gzip -dc"
    #   "bzip2 -dc" (ONLY if your files are .bz2)

    sem = asyncio.Semaphore(concurrency)
    out_tuples = []
    urls = [url for url, _ in input_tuples]
    async with aiohttp.ClientSession(timeout=TIMEOUT, raise_for_status=False) as session:
        # Distribute URLs roughly evenly to workers
        url_chunks = list(chunk(urls, DEFAULT_CONCURRENCY))
        tasks = [asyncio.create_task(worker(i, sem, session, url_chunks[i], decomp_cmd, out_tuples))
                 for i in range(len(url_chunks))]
        await asyncio.gather(*tasks)

    return out_tuples

