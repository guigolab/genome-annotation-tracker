"""
Async HTTP probes with retries and explicit ProbeResult outcomes.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Literal

import aiohttp

_LAST_MODIFIED_FMT = "%a, %d %b %Y %H:%M:%S %Z"
DEFAULT_ATTEMPTS = 5
DEFAULT_CONCURRENCY = 12
DL_CHUNK = 1 << 20
READ_CHUNK = 1 << 20
DEFAULT_RETRIES = 3

ProbeStatus = Literal["ok", "not_found", "transient_error"]


@dataclass
class ProbeResult:
    key: str
    status: ProbeStatus
    value: str | None = None
    detail: str | None = None


def _date_from_last_modified_header(headers) -> str | None:
    raw = headers.get("Last-Modified") or headers.get("last-modified")
    if not raw:
        return None
    try:
        return datetime.strptime(raw, _LAST_MODIFIED_FMT).date().isoformat()
    except (ValueError, TypeError):
        return None


def make_session(concurrency: int = DEFAULT_CONCURRENCY) -> aiohttp.ClientSession:
    timeout = aiohttp.ClientTimeout(sock_connect=30, sock_read=120, total=180)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
    )
    return aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"User-Agent": "genome-annotation-tracker/1.0"},
    )


def _is_not_found(status: int) -> bool:
    return status in (404, 410)


def _is_transient_status(status: int) -> bool:
    return status >= 500 or status == 429


async def request_with_retry(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    attempts: int = DEFAULT_ATTEMPTS,
    base_delay: float = 2.0,
    range_first_byte: bool = False,
) -> tuple[int, dict] | None:
    """
    Perform an HTTP request with retries on transient failures.
    Returns (status_code, headers_dict) or None if all attempts exhausted.
    404/410 are returned immediately without retry.
    """
    headers = {"Range": "bytes=0-0"} if range_first_byte else None
    last_status: int | None = None

    for attempt in range(attempts):
        try:
            async with session.request(
                method,
                url,
                allow_redirects=True,
                headers=headers,
            ) as resp:
                status = resp.status
                hdrs = dict(resp.headers)
                if _is_not_found(status):
                    return status, hdrs
                if status < 400:
                    return status, hdrs
                last_status = status
                if not _is_transient_status(status):
                    return status, hdrs
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_status = None
            if attempt == attempts - 1:
                return None

        if attempt < attempts - 1:
            delay = min(base_delay * (2**attempt) + random.uniform(0, 0.5), 30)
            await asyncio.sleep(delay)

    if last_status is not None:
        return last_status, {}
    return None


async def probe_many(
    tuples: list[tuple[str, str]],
    probe_fn: Callable[[aiohttp.ClientSession, str, str], Awaitable[ProbeResult]],
    concurrency: int = DEFAULT_CONCURRENCY,
) -> list[ProbeResult]:
    """Run probe_fn for every (url, key); always returns one ProbeResult per input."""
    sem = asyncio.Semaphore(concurrency)
    results: list[ProbeResult | None] = [None] * len(tuples)

    async with make_session(concurrency) as session:

        async def bound(idx: int, url: str, key: str) -> None:
            async with sem:
                results[idx] = await probe_fn(session, url, key)

        await asyncio.gather(*(bound(i, url, key) for i, (url, key) in enumerate(tuples)))

    return [r if r is not None else ProbeResult(key=tuples[i][1], status="transient_error", detail="no_result") for i, r in enumerate(results)]


async def probe_last_modified(
    session: aiohttp.ClientSession, url: str, key: str
) -> ProbeResult:
    """HEAD then ranged GET; classify 404/410 vs transient vs ok."""
    head = await request_with_retry(session, "HEAD", url)
    if head is not None:
        status, hdrs = head
        if _is_not_found(status):
            return ProbeResult(key=key, status="not_found", detail=f"status_{status}")
        if status < 400:
            lm = _date_from_last_modified_header(hdrs)
            if lm:
                return ProbeResult(key=key, status="ok", value=lm, detail=f"head_{status}")

    getr = await request_with_retry(session, "GET", url, range_first_byte=True)
    if getr is None:
        return ProbeResult(key=key, status="transient_error", detail="request_exhausted")
    status, hdrs = getr
    if _is_not_found(status):
        return ProbeResult(key=key, status="not_found", detail=f"status_{status}")
    if status < 400:
        lm = _date_from_last_modified_header(hdrs)
        if lm:
            return ProbeResult(key=key, status="ok", value=lm, detail=f"get_{status}")
    if _is_transient_status(status):
        return ProbeResult(key=key, status="transient_error", detail=f"status_{status}")
    return ProbeResult(key=key, status="transient_error", detail=f"status_{status}")


async def check_last_modified_date_many(
    tuples: list[tuple[str, str]], concurrency: int = DEFAULT_CONCURRENCY
) -> list[ProbeResult]:
    return await probe_many(tuples, probe_last_modified, concurrency)


async def fetch_url_text(
    session: aiohttp.ClientSession, url: str, key: str
) -> ProbeResult:
    """Fetch URL body as text (e.g. uncompressed_checksums.txt)."""
    getr = await request_with_retry(session, "GET", url)
    if getr is None:
        return ProbeResult(key=key, status="transient_error", detail="request_exhausted")
    status, _ = getr
    if _is_not_found(status):
        return ProbeResult(key=key, status="not_found", detail=f"status_{status}")
    if status >= 400:
        detail = f"status_{status}"
        if _is_transient_status(status):
            return ProbeResult(key=key, status="transient_error", detail=detail)
        return ProbeResult(key=key, status="transient_error", detail=detail)
    try:
        async with session.get(url, allow_redirects=True) as resp:
            text = await resp.text()
            return ProbeResult(key=key, status="ok", value=text, detail=f"status_{resp.status}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        return ProbeResult(key=key, status="transient_error", detail=type(e).__name__)


async def run_decompressor(cmd: list[str]) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def stream_hash_md5(
    session: aiohttp.ClientSession, url: str, decomp_cmd: list[str]
) -> ProbeResult:
    """Stream URL through decompressor and return uncompressed MD5."""
    for attempt in range(1, DEFAULT_RETRIES + 1):
        proc = None
        try:
            getr = await request_with_retry(session, "GET", url, attempts=3)
            if getr is None:
                if attempt == DEFAULT_RETRIES:
                    return ProbeResult(key=url, status="transient_error", detail="request_exhausted")
                await asyncio.sleep(min(2 * attempt, 10))
                continue
            status, _ = getr
            if _is_not_found(status):
                return ProbeResult(key=url, status="not_found", detail=f"status_{status}")
            if status >= 400:
                if attempt == DEFAULT_RETRIES:
                    st: ProbeStatus = "transient_error" if _is_transient_status(status) else "transient_error"
                    return ProbeResult(key=url, status=st, detail=f"status_{status}")
                await asyncio.sleep(min(2 * attempt, 10))
                continue

            h = hashlib.md5()
            async with session.get(url, allow_redirects=True) as resp:
                resp.raise_for_status()
                proc = await run_decompressor(decomp_cmd)

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
                    while True:
                        chunk = await proc.stdout.read(READ_CHUNK)
                        if not chunk:
                            break
                        h.update(chunk)

                await asyncio.gather(writer(), reader())
                try:
                    await asyncio.wait_for(proc.stderr.read(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                ret = await proc.wait()
                if ret != 0:
                    if attempt == DEFAULT_RETRIES:
                        return ProbeResult(key=url, status="transient_error", detail=f"decompress_exit_{ret}")
                    await asyncio.sleep(min(2 * attempt, 10))
                    continue
                return ProbeResult(key=url, status="ok", value=h.hexdigest(), detail="stream_hash")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            if attempt == DEFAULT_RETRIES:
                return ProbeResult(key=url, status="transient_error", detail=type(e).__name__)
            await asyncio.sleep(min(2 * attempt, 10))
        except Exception as e:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            if attempt == DEFAULT_RETRIES:
                return ProbeResult(key=url, status="transient_error", detail=type(e).__name__)
            await asyncio.sleep(min(2 * attempt, 10))

    return ProbeResult(key=url, status="transient_error", detail="stream_hash_exhausted")


async def probe_stream_md5(
    session: aiohttp.ClientSession, url: str, key: str
) -> ProbeResult:
    """Ensembl-style: stream bgzip-decompressed content MD5."""
    result = await stream_hash_md5(session, url, ["bgzip", "-dc"])
    result.key = key
    return result


async def stream_md5_checksum_many(
    input_tuples: list[tuple[str, str]], concurrency: int = DEFAULT_CONCURRENCY
) -> list[ProbeResult]:
    return await probe_many(input_tuples, probe_stream_md5, concurrency)


# Backward-compatible alias used by ncbi.py before refactor
async def get_last_modified_date(session: aiohttp.ClientSession, ftp_path: str) -> str | None:
    r = await probe_last_modified(session, ftp_path, ftp_path)
    return r.value if r.status == "ok" else None
