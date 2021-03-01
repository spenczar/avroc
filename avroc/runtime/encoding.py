from typing import IO
import struct


# Long
def decode_long(src: IO[bytes]) -> int:
    byte = src.read(1)[0]
    n = byte & 0x7F
    shift = 7
    while (byte & 0x80) != 0:
        byte = src.read(1)[0]
        n |= (byte & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


def skip_long(src: IO[bytes]) -> None:
    while (src.read(1)[0] & 0x80) != 0:
        pass


def encode_long(msg: int, dst: IO[bytes]) -> None:
    if msg >= 0:
        msg = msg << 1
    else:
        msg = (msg << 1) ^ (~0)
    encoded = b""
    while True:
        chunk = msg & 0x7F
        msg >>= 7
        if msg:
            encoded += bytes((chunk | 0x80,))
        else:
            encoded += bytes((chunk,))
            break
    dst.write(encoded)


# Int
def decode_int(src: IO[bytes]) -> int:
    return decode_long(src)


def skip_int(src: IO[bytes]) -> None:
    skip_long(src)


def encode_int(msg: int, dst: IO[bytes]) -> None:
    return encode_long(msg, dst)


# Float
def decode_float(src: IO[bytes]) -> float:
    raw = src.read(4)
    return struct.unpack("<f", raw)[0]


def skip_float(src: IO[bytes]) -> None:
    src.read(4)


def encode_float(msg: float, dst: IO[bytes]) -> None:
    enc = struct.pack("<f", msg)
    dst.write(enc)


# Double
def decode_double(src: IO[bytes]) -> float:
    raw = src.read(8)
    return struct.unpack("<d", raw)[0]


def skip_double(src: IO[bytes]) -> None:
    src.read(8)


def encode_double(msg: float, dst: IO[bytes]) -> None:
    enc = struct.pack("<d", msg)
    dst.write(enc)


# String
def decode_string(src: IO[bytes]) -> str:
    n = decode_long(src)
    return src.read(n).decode("utf8")


def skip_string(src: IO[bytes]) -> None:
    src.read(decode_long(src))


def encode_string(msg: str, dst: IO[bytes]) -> None:
    encoded = msg.encode("utf8")
    encode_long(len(encoded), dst)
    dst.write(encoded)


# Null
def decode_null(src: IO[bytes]) -> None:
    pass


def skip_null(src: IO[bytes]) -> None:
    pass


def encode_null(msg: None, dst: IO[bytes]) -> None:
    pass


# Boolean
def decode_boolean(src: IO[bytes]) -> bool:
    return src.read(1)[0] == 1


def skip_boolean(src: IO[bytes]) -> None:
    src.read(1)


_literal_one = bytes((1,))
_literal_zero = bytes((0,))


def encode_boolean(msg: bool, dst: IO[bytes]) -> None:
    dst.write(_literal_one if msg else _literal_zero)


# Bytes
def decode_bytes(src: IO[bytes]) -> bytes:
    n = decode_long(src)
    return src.read(n)


def skip_bytes(src: IO[bytes]) -> None:
    n = decode_long(src)
    src.read(n)


def encode_bytes(msg: bytes, dst: IO[bytes]) -> None:
    encode_long(len(msg), dst)
    dst.write(msg)
