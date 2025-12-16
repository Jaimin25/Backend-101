import asyncio
from pydoc import cli
from logger import log

class ProxyServer:
    def __init__(self, backend_pool, health_manager, timeouts):
        self.pool = backend_pool
        self.health = health_manager
        self.timeouts = timeouts

    async def handle_client(self, reader, writer):
        client_addr = writer.get_extra_info("peername")
        log("----------------------START OF REQUEST------------------------")
        log("client_connected", client=client_addr)

        try:
            raw = await asyncio.wait_for(
                reader.read(65536),
                timeout=self.timeouts["client_read"]
            )
            log("request_received", size=len(raw))
        except asyncio.TimeoutError:
            log("client_timeout", client=client_addr)
            writer.close()
            return
        
        backend = self.pool.get_next_backend()

        if backend is None:
            log("no_backend_available")
            writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        log("backend_selected", backend=f"{backend.host}:{backend.port}")

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

            log("request_ok", backend=f"{backend.host}:{backend.port}")

        except Exception as e:
            self.health.record_failure(backend)
            log("request_fail", backend=f"{backend.host}:{backend.port}", error=str(e))

            writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\n")
            await writer.drain()

        finally:
            writer.close()
            log("connection_closed", client=client_addr)
            log("----------------------END OF REQUEST------------------------\n")

        