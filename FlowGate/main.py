import asyncio
import config
from backend_pool import Backend, BackendPool
from health import HealthManager
from proxy_server import ProxyServer

async def main():
    pool = BackendPool(
        [Backend(h, p) for h, p in config.BACKENDS]
    )

    health = HealthManager(
        failure_threshold=config.FAILURE_THRESHOLD,
        cooldown=config.COOLDOWN_SECONDS
    )

    proxy = ProxyServer(pool, health, config.TIMEOUTS)

    server = await asyncio.start_server(
        proxy.handle_client,
        config.LISTEN_HOST,
        config.LISTEN_PORT,
    )

    print(f"FlowGazte running on {config.LISTEN_PORT}")

    async with server:
        await server.serve_forever()

asyncio.run(main())