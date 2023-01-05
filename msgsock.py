"""Msgsock (Message Sockets)

A dead simple protocol library for exchanging messages over TCP.
"""

__version__ = "1.0.0"

import socket


class ConnectionClosed(ConnectionError):
    pass


class RawMessageSocket:
    def __init__(self, conn: socket.socket, header_size: int = 4) -> None:
        self.sock = conn
        self.header_size = header_size
        self.buffer = bytearray()
        self.byteorder = "big"

    def receive_message(self) -> bytearray:
        # Receive header and decode to an integer, denoting the length
        # of payload.
        try:
            header = self._receive_bytes(self.header_size)
        except ConnectionClosed:
            return b""

        payload_length = int.from_bytes(header, self.byteorder)
        return self._receive_bytes(payload_length)

    def send_message(self, payload: bytes) -> None:
        # Construct fixed size header.
        header = len(payload).to_bytes(self.header_size, self.byteorder)
        # Send header first, then payload.
        self.sock.sendall(header)
        self.sock.sendall(payload)

    def _receive_bytes(self, n: int) -> bytearray:
        data = bytearray()
        # Take at most n bytes from the buffer (unless it
        # contains less than n bytes, in which take all of it).
        buffer_cutoff = min(n, len(self.buffer))
        data += self.buffer[:buffer_cutoff]
        del self.buffer[:buffer_cutoff]
        # If the buffer didn’t have enough bytes, receive the
        # rest from the socket.
        while len(data) < n:
            chunk = self.sock.recv(1024)
            if not chunk:
                if len(data) + len(chunk) < n:
                    raise ConnectionClosed(
                        "Connection closed before all data received!")
                break
            data += chunk

        # If data contains more bytes than necessary, copy them
        # to the end of the buffer.
        if len(data) > n:
            self.buffer += data[n:]
            return data[:n]

        return data


class MessageSocket(RawMessageSocket):
    def receive_message(self) -> str:
        return super().receive_message().decode("utf-8")

    def send_message(self, message: str) -> None:
        super().send_message(message.encode("utf-8"))
