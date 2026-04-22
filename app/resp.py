from datetime import datetime, timedelta
from io import BufferedIOBase


class Buffer:
    def __init__(self, reader: BufferedIOBase):
        self.reader = reader
        self.buf = b""

    def _fill(self):
        chunk = self.reader.read1(4096)
        if not chunk:
            raise EOFError("connection closed")
        self.buf += chunk

    def read_exact(self, n):
        while len(self.buf) < n:
            self._fill()
        data = self.buf[:n]
        self.buf = self.buf[n:]
        return data

    def read_line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line


class SimpleString:
    @staticmethod
    def encode(s: str):
        return ("+" + s + "\r\n").encode()


class BulkString:
    @staticmethod
    def parse(b: Buffer):
        length = int(b.read_line())
        data = b.read_exact(length).decode()
        b.read_exact(2)
        return data

    @staticmethod
    def encode(s: str):
        return ("$" + str(len(s)) + "\r\n" + s + "\r\n").encode()

    @staticmethod
    def null():
        return ("$-1\r\n").encode()


class Array:
    @staticmethod
    def parse(b: Buffer):
        count = int(b.read_line())
        return [parse(b) for _ in range(count)]


def parse(b: Buffer):
    _type = b.read_exact(1)

    match _type:
        case b"*":
            return Array.parse(b)
        case b"$":
            return BulkString.parse(b)
        case _:
            pass


class server:
    var = {}

    @classmethod
    def process(cls, a: list[str]):
        match a[0].lower():
            case "ping":
                return SimpleString.encode("PONG")
            case "echo":
                return BulkString.encode(a[1])
            case "set":
                name, val = a[1:3]
                expire = None
                if len(a) > 3:
                    op, opval = a[3:5]
                    now = datetime.today()
                    expire = now + timedelta(milliseconds=int(opval))
                cls.var[name] = {
                    "val": val,
                    "expire": expire,
                }
                return SimpleString.encode("OK")
            case "get":
                name = a[1]
                x = cls.var[name]
                now = datetime.today()
                if x and x["expire"] is None or x["expire"] > now:
                    return BulkString.encode(x["val"])
                return BulkString.null()
