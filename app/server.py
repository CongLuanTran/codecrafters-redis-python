import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta

from app.resp import Array, BulkString, Integer, SimpleString


class RedisServer:
    def __init__(self):
        self.store = defaultdict(str)
        self.list = defaultdict(list)
        self.expires = {}
        self.lock = asyncio.Lock()
        self.waiters = defaultdict(deque[asyncio.Future])
        self.commands = {
            "PING": self.ping,
            "ECHO": self.echo,
            "GET": self.get,
            "SET": self.set,
            "RPUSH": self.rpush,
            "LPUSH": self.lpush,
            "LLEN": self.llen,
            "LRANGE": self.lrange,
            "LPOP": self.lpop,
            "BLPOP": self.blpop,
        }

    async def dispatch(self, cmd):
        name = cmd[0].upper()

        if name not in self.commands:
            raise CommandError(f"unknown command '{cmd[0]}'")

        return await self.commands[name](cmd)

    async def ping(self, cmd):
        if len(cmd) == 1:
            return SimpleString("PONG")

        if len(cmd) == 2:
            return SimpleString(cmd[1])

        raise CommandError.wrong_argument_count("ping")

    async def echo(self, cmd):
        if len(cmd) == 2:
            return BulkString(cmd[1])

        raise CommandError.wrong_argument_count("echo")

    async def get(self, cmd):
        if len(cmd) == 2:
            if cmd[1] in self.expires and datetime.now() > self.expires[cmd[1]]:
                self.store.pop(cmd[1], None)
                self.expires.pop(cmd[1], None)
            return BulkString(self.store.get(cmd[1]))

        raise CommandError.wrong_argument_count("get")

    async def set(self, cmd):
        if len(cmd) >= 3:
            self.store[cmd[1]] = cmd[2]
            if len(cmd) > 3:
                try:
                    match cmd[3].upper():
                        case "PX":
                            lifetime = timedelta(milliseconds=int(cmd[4]))
                            self.expires[cmd[1]] = datetime.now() + lifetime
                except IndexError:
                    raise CommandError("syntax error")
            return SimpleString("OK")

        raise CommandError.wrong_argument_count("set")

    async def rpush(self, cmd):
        if len(cmd) >= 3:
            async with self.lock:
                self.list[cmd[1]].extend(cmd[2:])
                if self.waiters[cmd[1]]:
                    fut = self.waiters[cmd[1]].popleft()
                    if not fut.done():
                        fut.set_result(True)
            return Integer(len(self.list[cmd[1]]))

        raise CommandError.wrong_argument_count("rpush")

    async def lpush(self, cmd):
        if len(cmd) >= 3:
            for e in cmd[2:]:
                self.list[cmd[1]].insert(0, e)
            return Integer(len(self.list[cmd[1]]))

        raise CommandError.wrong_argument_count("lpush")

    async def llen(self, cmd):
        if len(cmd) == 2:
            return Integer(len(self.list[cmd[1]]))
        raise CommandError.wrong_argument_count("llen")

    async def lrange(self, cmd):
        if len(cmd) == 4:
            if cmd[1] not in self.list:
                return Array([])

            length = len(self.list[cmd[1]])
            start = int(cmd[2])
            if start < 0:
                start = length + start
                start = start if start > 0 else 0
            stop = int(cmd[3])
            if stop < 0:
                stop = length + stop
                stop = stop if stop > 0 else 0

            return Array(map(BulkString, self.list[cmd[1]][start : stop + 1]))

        raise CommandError.wrong_argument_count("lrange")

    async def lpop(self, cmd):
        if 3 >= len(cmd) >= 2:
            arr = self.list[cmd[1]]
            if len(cmd) == 3:
                count = int(cmd[2])
                if count < 0:
                    raise CommandError("value is out of range, must be positive")
                a = arr[:count]
                self.list[cmd[1]] = arr[count:]
                return Array(map(BulkString, a))
            a = None if len(arr) == 0 else arr.pop(0)
            return BulkString(a)

        raise CommandError.wrong_argument_count("lpop")

    async def blpop(self, cmd):
        if len(cmd) == 3:
            loop = asyncio.get_running_loop()
            fut = loop.create_future()

            # LPOP if the list is not empty, else add new waiter
            async with self.lock:
                if self.list[cmd[1]]:
                    val = self.list[cmd[1]].pop(0)
                    return Array([BulkString(cmd[1]), BulkString(val)])

                self.waiters[cmd[1]].append(fut)

            # Wait in [dur] seconds, or indefintiely if [dur] is 0
            try:
                dur = float(cmd[2])
                dur = None if dur == 0 else dur
                await asyncio.wait_for(fut, dur)
            except asyncio.TimeoutError:
                async with self.lock:
                    self.waiters[cmd[1]].remove(fut)
                return Array(None)

            async with self.lock:
                if self.list[cmd[1]]:
                    val = self.list[cmd[1]].pop(0)
                    return Array([BulkString(cmd[1]), BulkString(val)])

                return Array(None)

        raise CommandError.wrong_argument_count("blpop")


class CommandError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

    @classmethod
    def wrong_argument_count(cls, cmd: str):
        return cls(f"wrong number of arguments for '{cmd}' command")

    def __str__(self):
        return f"(error) ERR {self.message}"
