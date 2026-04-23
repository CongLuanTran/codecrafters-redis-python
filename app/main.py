from asyncio import StreamReader, StreamWriter, run, start_server

from app.resp import resp_decode
from app.server import CommandError, RedisServer


async def handle_client(
    reader: StreamReader,
    writer: StreamWriter,
    server: RedisServer,
):
    while True:
        request = await resp_decode(reader)
        if not request:
            break

        print(f"Request is:{request}")
        try:
            response = server.dispatch(request)
            writer.write(bytes(response))
            await writer.drain()
        except CommandError as e:
            print(e)

    writer.close()
    await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    HOST, PORT = "localhost", 6379

    redis_server = RedisServer()

    async def client_connected(reader, writer):
        await handle_client(reader, writer, redis_server)

    tcp_server = await start_server(
        client_connected,
        HOST,
        PORT,
    )

    async with tcp_server:
        await tcp_server.serve_forever()


if __name__ == "__main__":
    run(main())
