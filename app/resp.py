from asyncio import IncompleteReadError, StreamReader


async def readline(reader: StreamReader):
    """Wrapper to automatically strip the \\r\\n at the end"""
    data = await reader.readuntil(b"\r\n")
    return data.strip(b"\r\n")


class SimpleString:
    def __init__(self, string: str):
        self.string = string

    def __bytes__(self):
        return f"+{self.string}\r\n".encode()


class BulkString:
    def __init__(self, string: str | None):
        self.string = string

    @staticmethod
    async def decode(reader: StreamReader) -> str:
        length = int(await readline(reader))
        data = (await reader.readexactly(length)).decode()
        await reader.readexactly(2)
        return data

    def __bytes__(self):
        if self.string is None:
            return "$-1\r\n".encode()

        return f"${str(len(self.string))}\r\n{self.string}\r\n".encode()


class Array:
    @staticmethod
    async def decode(reader: StreamReader):
        count = int(await readline(reader))
        return [await resp_decode(reader) for _ in range(count)]


async def resp_decode(reader: StreamReader):
    try:
        _type = await reader.readexactly(1)

        match _type:
            case b"*":
                return await Array.decode(reader)
            case b"$":
                return await BulkString.decode(reader)
    except IncompleteReadError:
        return None
