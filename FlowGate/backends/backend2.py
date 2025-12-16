import asyncio

PORT = 8002
NAME = "backend2"

async def handle(reader, writer):
    body = NAME.encode()
    response = (
        b"HTTP/1.1 200 OK\r\n"
        + f"Content-Length: {len(body)}\r\n".encode()
        + b"\r\n"
        + body
    )

    writer.write(response)
    await writer.drain()
    writer.close()

async def main():
    server = await asyncio.start_server(handle, "localhost", PORT)
    print(f"{NAME} listening on {PORT}")
    async with server:
        await server.serve_forever()

asyncio.run(main())