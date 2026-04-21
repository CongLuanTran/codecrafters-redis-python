import socketserver


class MyTCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            data = self.rfile.readline(10000)
            if not data:
                break

            data = data.rstrip()
            if data == b"PING":
                self.wfile.write(b"+PONG\r\n")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    HOST, PORT = "localhost", 6379

    # Bind must happen after allow_reuse_adress for the setting to work
    with socketserver.ThreadingTCPServer(
        (HOST, PORT), MyTCPHandler, bind_and_activate=False
    ) as server:
        server.allow_reuse_address = True
        server.server_bind()
        server.server_activate()
        print(f"serving on {HOST} as port {PORT}")
        server.serve_forever()


if __name__ == "__main__":
    main()
