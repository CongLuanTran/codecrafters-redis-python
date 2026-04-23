from datetime import datetime, timedelta

from app.resp import BulkString, Interger, SimpleString


class RedisServer:
    def __init__(self):
        self.store = {}
        self.list = {}
        self.expires = {}
        self.commands = {
            "PING": self.ping,
            "ECHO": self.echo,
            "GET": self.get,
            "SET": self.set,
            "RPUSH": self.append_list,
        }

    def dispatch(self, cmd):
        name = cmd[0].upper()

        if name not in self.commands:
            raise CommandError(f"unknown command '{cmd[0]}'")

        return self.commands[name](cmd)

    def ping(self, cmd):
        if len(cmd) == 1:
            return SimpleString("PONG")

        if len(cmd) == 2:
            return SimpleString(cmd[1])

        raise CommandError.wrong_argument_count("ping")

    def echo(self, cmd):
        if len(cmd) == 2:
            return BulkString(cmd[1])

        raise CommandError.wrong_argument_count("echo")

    def get(self, cmd):
        if len(cmd) == 2:
            if cmd[1] in self.expires and datetime.now() > self.expires[cmd[1]]:
                self.store.pop(cmd[1], None)
                self.expires.pop(cmd[1], None)
            return BulkString(self.store.get(cmd[1]))

        raise CommandError.wrong_argument_count("get")

    def set(self, cmd):
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

    def append_list(self, cmd):
        if len(cmd) == 3:
            if cmd[1] not in self.list:
                self.list[cmd[1]] = []
            self.list[cmd[1]].append(cmd[2])
            return Interger(len(self.list[cmd[1]]))


class CommandError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

    @classmethod
    def wrong_argument_count(cls, cmd: str):
        return cls(f"wrong number of arguments for '{cmd}' command")

    def __str__(self):
        return f"(error) ERR {self.message}"
