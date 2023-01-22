"""Msgsock (Message Sockets)

A dead simple protocol library for exchanging messages over TCP.
"""

__version__ = "2.1.0"
__all__ = ["ConnectionClosed", "RawMessageSocket", "MessageSocket"]

import socket
from typing import Final, Literal

RECV_SIZE = 1024


class ConnectionClosed(Exception):
    def __init__(self, msg: str, data: bytes, expected: int) -> None:
        super().__init__(msg)
        self.data = data
        self.expected = expected

    def __str__(self) -> str:
        msg, received, expected = self.args[0], len(self.data), self.expected
        return f"{msg} (got {received}, expected {expected})"


class RawMessageSocket:
    def __init__(self, conn: socket.socket, *, header_size: int = 4) -> None:
        self.sock = conn
        self.header_size = header_size
        self.buffer = bytearray()
        self.byteorder: Final[Literal["big", "little"]] = "big"

    def receive_message(self) -> bytearray:
        # Receive header and decode to an integer, denoting the length
        # of payload.
        try:
            header = self._receive_bytes(self.header_size)
        except ConnectionClosed as error:
            # If the connection was closed while the header was still
            # being received, reraising the error is more appropriate,
            # otherwise return an empty response.
            if error.data:
                raise
            return bytearray()

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
        # Take at most n bytes from the buffer (unless it contains less
        # than n bytes, in which take all of it).
        buffer_cutoff = min(n, len(self.buffer))
        data += self.buffer[:buffer_cutoff]
        del self.buffer[:buffer_cutoff]
        # If the buffer didn’t have enough bytes, receive the rest from
        # the socket.
        remaining = n - buffer_cutoff
        while remaining > 0:
            chunk = self.sock.recv(RECV_SIZE)
            if not chunk:
                raise ConnectionClosed(
                    "Connection closed before all bytes received", data, n)
            data += chunk
            remaining -= len(chunk)

        # If data contains more bytes than necessary, copy them to the
        # end of the buffer.
        if remaining < 0:
            self.buffer += data[n:]
            return data[:n]

        return data


class MessageSocket:
    def __init__(self, conn: socket.socket, *, header_size: int = 4) -> None:
        self.conn = RawMessageSocket(conn, header_size=header_size)

    def receive_message(self) -> str:
        return self.conn.receive_message().decode("utf-8")

    def send_message(self, message: str) -> None:
        self.conn.send_message(message.encode("utf-8"))
