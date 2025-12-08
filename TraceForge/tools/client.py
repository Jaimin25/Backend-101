import argparse
import asyncio
from bdb import effective
import csv
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from aiohttp import ClientSession, ClientError
from aiohttp.web import head

TRACE_HEADER = "X-Trace-Id"

async def perform_request(
    session: ClientSession,
    url: str,
    use_client_trace: bool,
    max_retries: int,
    retry_backoff_ms: int,
):
    """
    Performs a single logical request with retries.

    Returns a result dict with:
        trace_id
        status_code
        start_ts
        latency_ms
        retries
        error
    """
    retries = 0
    last_error: Optional[str] = None
    
    while True:
        start_ts = datetime.now(UTC).isoformat() + "Z"
        start = time.monotonic()

        headers = {}

        client_trace_id: Optional[str] = None
        if use_client_trace:
            client_trace_id = uuid.uuid4().hex
            headers[TRACE_HEADER] = client_trace_id

        status_code: Optional[int] = None
        trace_id_from_proxy: Optional[str] = None

        try:
            async with session.get(url, headers=headers) as resp:
                latency_ms = (time.monotonic() - start) * 1000.0
                status_code = resp.status
                
                trace_id_from_proxy = resp.headers.get(TRACE_HEADER)
                effective_trace_id = client_trace_id or trace_id_from_proxy

                await resp.read()

                error_msg = ""

        except ClientError as e:
            latency_ms = (time.monotonic() - start) * 1000.0
            error_msg = str(e)
            last_error = error_msg
        except Exception as e:
            latency_ms = (time.monotonix() - start) * 1000.0
            error_msg = f"unexpected error: {e}"
            last_error = error_msg

        else:
            if status_code is not None and status_code < 500:

                return {
                    "trace_id": effective_trace_id,
                    "status_code": status_code,
                    "start_ts": start_ts,
                    "latency_ms": latency_ms,
                    "retries": retries,
                    "error": error_msg,
                }

            last_error = f"http_{status_code}"
        
        if retries >= max_retries:
            return {
                "trace_id": effective_trace_id,
                "status_code": status_code,
                "start_ts": start_ts,
                "latency_ms": latency_ms,
                "retries": retries,
                "error": last_error or "max_retries_exceeded",
            }

        retries += 1
        backoff_seconds = (retry_backoff_ms / 1000.0) * (2 ** (retries - 1))
        await asyncio.sleep(backoff_seconds)

async def run_load(
    url: str,
    n: int,
    concurrency: int,
    use_client_trace: bool,
    max_retries: int,
    retry_backoff_ms: int
):
    """
    Run N logical requests with at most `concurrency` in flight.
    Returns a list of result dicts.
    """

    semaphore = asyncio.Semaphore(concurrency)
    results = []

    async with ClientSession() as session:

        async def worker(request_index: int):
            async with semaphore:
                res = await perform_request(
                    session=session,
                    url=url,
                    use_client_trace=use_client_trace,
                    max_retries=max_retries,
                    retry_backoff_ms=retry_backoff_ms,
                )
                res["request_index"] = request_index
                results.append(res)

        tasks = [asyncio.create_task(worker(i)) for i in range(n)]
        await asyncio.gather(*tasks)

    return results

def write_results_json(results, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

def write_results_csv(results, path):
    if not results:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = [
        "request_index",
        "trace_id",
        "status_code",
        "start_ts",
        "latency_ms",
        "retries",
        "error"
    ]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({
                k: row.get(k, "") for k in fieldnames
            })

def parse_args():
    parser = argparse.ArgumentParser(description="Async load generator for proxy")

    parser.add_argument(
        "--url",
        required=True,
        help="Target URL on the proxy, for example http://127.0.0.1:8080/api/resource"
    )

    parser.add_argument(
        "--n",
        type=int,
        default=100,
        help="Number of requests to send (default: 100)"
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Maximum concurrent requests (default: 10)"
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=0,
        help="Maximum retries per request of error or 5xx (default: 0)"
    )

    parser.add_argument(
        "--retry-backoff-ms",
        type=int,
        default=100,
        help="Initial retyr backoff in ms (default: 100, exponential)"
    )

    parser.add_argument(
        "--client-trace",
        action="store_true",
        help="If set, client generates X-Trace-Id for each request",
    )

    parser.add_argument(
        "--output",
        default="results.json",
        help="Output file path (default: results.json)"
    )

    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format (json or csv, default: json)"
    )

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    results = asyncio.run(
        run_load(
            url=args.url,
            n=args.n,
            concurrency=args.concurrency,
            use_client_trace=args.client_trace,
            max_retries=args.retries,
            retry_backoff_ms=args.retry_backoff_ms,
        )
    )

    if args.format == "json":
        write_results_json(results, args.output)
    else:
        write_results_csv(results, args.output)

    print(f"Wrote {len(results)} results to {args.output}")