import asyncio
from logger import log

class ProxyServer:
    def __init__(self, backend_pool, health_manager, timeouts):
        self.pool = backend_pool
        self.health = health_manager
        self.timeouts = timeouts

    async def handle_client(self, reader, writer):
        try:
            raw = await asyncio.wait_for(
                reader.read(65536),
                timeout=self.timeouts["client_read"]
            )
        except asyncio.TimeoutError:
            writer.close()
            return
        
        backend = self.pool.get_next_backend()

        if backend is None:
            writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        try:
            b_reader, b_writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=self.timeouts["backend_connect"]
            )

            b_writer.write(raw)
            await b_writer.drain()

            response = await asyncio.wait_for(
                b_reader.read(65536),
                timeout=self.timeouts["backend_resposne"],
            )

            writer.write(response)
            await writer.drain()

            self.health.record_success(backend)

        except Exception:
            self.health.record_failure(backend)
            writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\n")
            await writer.drain()

        finally:
            writer.close()

        