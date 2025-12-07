import asyncio
import os
import random
import time
from urllib.parse import urljoin
import uuid
from aiohttp import ClientSession, web
from aiohttp.tracing import TraceRequestHeadersSentParams
from multidict import CIMultiDict
import structlog
from dotenv import load_dotenv
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST
)

load_dotenv()

log = structlog.get_logger()

UPSTREAM_BASE_URL = os.getenv("UPSTREAM_BASE_URL")
LISTEN_PORT = 8080

TRACE_HEADER = "X-Trace-Id"
REQUEST_HEADER = "X-Request-Id"

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade"
}

PROXY_DELAY_MS = int(os.getenv("PROXY_DELAY_MS"))
PROXY_DROP_RATE = float(os.getenv("PROXY_DROP_RATE"))
PROXY_MOD_HEADER_RAW = os.getenv("PROXY_MOD_HEADER")
RNG_SEED_RAW = os.getenv("RNG_SEED")

PROXY_MOD_HEADER_NAME = None
PROXY_MOD_HEADER_VALUE = None

if PROXY_MOD_HEADER_RAW:
    if ":" in PROXY_MOD_HEADER_RAW:
        name, value = PROXY_MOD_HEADER_RAW.split(":", 1)
        PROXY_MOD_HEADER_NAME = name.strip()
        PROXY_MOD_HEADER_VALUE = value.strip()

if RNG_SEED_RAW is not None:
    RNG_SEED = int(RNG_SEED_RAW)
    FAULT_RNG = random.Random(RNG_SEED)
else:
    RNG_SEED = None
    FAULT_RNG = random.Random()

REQUESTS_TOTAL = Counter(
    "requests_total",
    "Total HTTP requests processed by the proxy",
    ["method", "status_class"],
)

REQUEST_FORWARDED_TOTAL = Counter(
    "request_forwarded_total",
    "Total HTTP requests forwarded to upstream",
)

REQUESTS_DROPPED_TOTAL = Counter(
    "requests_dropped_total",
    "Total HTTP requests dropped by fault injection"
)

REQUESTS_DELAYED_TOTAL = Counter(
    "requests_delayed_total",
    "Total HTTP requests artificially delayed by fault injection"
)

REQUEST_LATENCY_SECONDS = Histogram(
    "request_latency_seconds",
    "Proxy request latency in seconds",
    ["method", "status_class"],
)

async def handle_metrics(request: web.Request):
    data = generate_latest()
    return web.Response(body=data, headers={"Content-Type": CONTENT_TYPE_LATEST})

def strip_hop_by_hop_headers(headers: CIMultiDict):
    new_headers = CIMultiDict(headers)

    for h in list(new_headers.keys()):
        if h.lower() in HOP_BY_HOP_HEADERS:
            del new_headers[h]

    connection = headers.get("connection")
    if connection:
        tokens = [h.strip() for h in connection.split(",") if h.strip()]

        for token in tokens:
            for key in list(new_headers.keys()):
                if key.lower() == token.lower():
                    del new_headers[key]

    return new_headers

@web.middleware
async def trace_metrics_middleware(request:web.BaseRequest, handler):
    start = time.monotonic()

    trace_id = request.headers.get(TRACE_HEADER)

    if not trace_id:
        trace_id = uuid.uuid4().hex

    request_id = request.headers.get(REQUEST_HEADER)

    if not request_id:
        request_id = uuid.uuid4().hex

    request["trace_id"] = trace_id
    request["request_id"] = request_id

    log.info("Incoming request %s %s trace_id=%s request_id=%s", request.method, request.rel_url, trace_id, request_id)

    response = await handler(request)

    duration = time.monotonic() - start
    method = request.method.upper()
    status = response.status
    status_class = f"{status // 100}"

    REQUESTS_TOTAL.labels(method=method, status_class=status_class).inc()
    REQUEST_LATENCY_SECONDS.labels(method=method, status_class=status_class).observe(duration)

    response.headers.setdefault(TRACE_HEADER, trace_id)
    response.headers.setdefault(REQUEST_HEADER, request_id)

    log.info("Completed request %s %s trace_id=%s request_id=%s status=%s decision %s", method, request.rel_url, trace_id, request_id, status, "forwarded" if status != 503 else "dropped")

    return response

@web.middleware
async def fault_injection_middleware(request: web.BaseRequest, handler):
    trace_id = request["trace_id"]
    request_id = request["request_id"]

    if PROXY_DELAY_MS > 0:
        REQUESTS_DELAYED_TOTAL.inc()

        log.info("Injecting artifical delay of %dms for request %s %s trace_id=%s request_id=%s", PROXY_DELAY_MS, request.method, request.rel_url, trace_id, request_id)

        await asyncio.sleep(PROXY_DELAY_MS / 1000.0)

    if PROXY_DROP_RATE > 0.0:
        r = FAULT_RNG.random()
        if r < PROXY_DROP_RATE:
            REQUESTS_DROPPED_TOTAL.inc()
            log.info("Dropping request %s %s trace_id=%s request_id=%s", request.method, request.rel_url, trace_id, request_id)

            body = {
                "error" : "simulated_drop",
                "message" : "request dropped by proxy fault injection",
                "trace_id" : trace_id,
                "request_id" : request_id,
            }
            resp = web.json_response(body, status=503)
            resp.headers[TRACE_HEADER] = trace_id
            resp.headers[REQUEST_HEADER] = request_id
            return resp

    response = await handler(request)
    return response

async def handle_proxy(request: web.BaseRequest):
    upstream_url = urljoin(UPSTREAM_BASE_URL, str(request.rel_url))

    trace_id = request["trace_id"]
    request_id = request["request_id"]

    body = await request.read()

    headers = strip_hop_by_hop_headers(request.headers)

    headers.pop("Host", None)

    headers[TRACE_HEADER] = trace_id
    headers[REQUEST_HEADER] = request_id

    log.info("Forwarding %s %s to %s trace_id=%s request_id=%s", request.method, request.rel_url, upstream_url, trace_id, request_id)

    REQUEST_FORWARDED_TOTAL.inc()

    async with ClientSession() as session:
        async with session.request(
            method=request.method,
            url=upstream_url,
            headers=headers,
            data=body,
            allow_redirects=False,
        ) as upstream_resp:

            resp_body = await upstream_resp.read()
            resp_headers = strip_hop_by_hop_headers(upstream_resp.headers)

            resp_headers.setdefault(TRACE_HEADER, trace_id)
            resp_headers.setdefault(REQUEST_HEADER, request_id)

            log.info("Upstream response %s for %s trace_id=%s request_id=%s", upstream_resp.status, request.rel_url, trace_id, request_id)

            if PROXY_MOD_HEADER_NAME:
                resp_headers[PROXY_MOD_HEADER_NAME] = PROXY_MOD_HEADER_VALUE

            return web.Response(
                status=upstream_resp.status,
                headers=resp_headers,
                body=resp_body
            )

def create_app():
    app = web.Application(middlewares=[trace_metrics_middleware, fault_injection_middleware])
    app.router.add_get("/metrics", handle_metrics)
    app.router.add_route("*", "/{tail:.*}", handle_proxy)
    return app

if __name__ == "__main__":
    web.run_app(create_app(), host="127.0.0.1", port=LISTEN_PORT)