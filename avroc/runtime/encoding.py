from typing import IO
import struct
import decimal
import uuid
import datetime


# Primitives

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


def encode_long(msg: int) -> bytes:
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
    return encoded


# Int
def decode_int(src: IO[bytes]) -> int:
    return decode_long(src)


def skip_int(src: IO[bytes]) -> None:
    skip_long(src)


def encode_int(msg: int) -> bytes:
    return encode_long(msg)


# Float
def decode_float(src: IO[bytes]) -> float:
    raw = src.read(4)
    return struct.unpack("<f", raw)[0]


def skip_float(src: IO[bytes]) -> None:
    src.read(4)


def encode_float(msg: float) -> bytes:
    return struct.pack("<f", msg)


# Double
def decode_double(src: IO[bytes]) -> float:
    raw = src.read(8)
    return struct.unpack("<d", raw)[0]


def skip_double(src: IO[bytes]) -> None:
    src.read(8)


def encode_double(msg: float) -> bytes:
    return struct.pack("<d", msg)


# String
def decode_string(src: IO[bytes]) -> str:
    n = decode_long(src)
    return src.read(n).decode()


def skip_string(src: IO[bytes]) -> None:
    src.read(decode_long(src))


def encode_string(msg: str) -> bytes:
    encoded = msg.encode()
    buf = encode_long(len(encoded))
    buf += encoded
    return buf


# Null
def decode_null(src: IO[bytes]) -> None:
    pass


def skip_null(src: IO[bytes]) -> None:
    pass


def encode_null(msg: None) -> bytes:
    return b""


# Boolean
def decode_boolean(src: IO[bytes]) -> bool:
    return src.read(1)[0] == 1


def skip_boolean(src: IO[bytes]) -> None:
    src.read(1)


_literal_one = bytes((1,))
_literal_zero = bytes((0,))


def encode_boolean(msg: bool) -> bytes:
    return _literal_one if msg else _literal_zero


# Bytes
def decode_bytes(src: IO[bytes]) -> bytes:
    n = decode_long(src)
    return src.read(n)


def skip_bytes(src: IO[bytes]) -> None:
    n = decode_long(src)
    src.read(n)


def encode_bytes(msg: bytes) -> bytes:
    buf = encode_long(len(msg))
    buf += msg
    return buf


# Logical Types

# Decimal
decimal_context = decimal.Context()


def decode_decimal_bytes(
    src: IO[bytes], precision: int, scale: int = 0
) -> decimal.Decimal:
    raw_bytes = decode_bytes(src)
    return _deserialize_decimal(raw_bytes, precision, scale)


def encode_decimal_bytes(msg: decimal.Decimal, precision: int, scale: int = 0) -> bytes:
    _validate_decimal(msg, precision, scale)
    unscaled = _unscale_decimal(msg)
    length_bytes, remainder = divmod(unscaled.bit_length(), 8)
    if remainder != 0:
        length_bytes += 1
    as_bytes = unscaled.to_bytes(length_bytes, byteorder="big", signed=True)
    return encode_bytes(as_bytes)


def decimal_from_string(raw: str, precision: int, scale: int = 0) -> decimal.Decimal:
    return _deserialize_decimal(raw.encode(), precision, scale)


def decode_decimal_fixed(
    src: IO[bytes], size: int, precision: int, scale: int = 0
) -> decimal.Decimal:
    raw_bytes = src.read(size)
    return _deserialize_decimal(raw_bytes, precision, scale)


def encode_decimal_fixed(
    msg: decimal.Decimal, size: int, precision: int, scale: int = 0
) -> bytes:
    _validate_decimal(msg, precision, scale)
    unscaled = _unscale_decimal(msg)
    as_bytes = unscaled.to_bytes(size, byteorder="big", signed=True)
    return as_bytes


def _deserialize_decimal(raw: bytes, precision: int, scale: int) -> decimal.Decimal:
    unscaled = int.from_bytes(raw, byteorder="big", signed=True)
    decimal_context.prec = precision
    decimal_value = decimal_context.create_decimal(unscaled)
    if scale == 0:
        return decimal_value
    else:
        return decimal_value.scaleb(-scale, decimal_context)


def _validate_decimal(d: decimal.Decimal, precision: int, scale: int) -> None:
    """
    Validates that the decimal can be properly represented according to the
    precision and scale values.
    """
    _, digits, exp = d.as_tuple()
    # Precision represents the number of digits that can be stored.
    if len(digits) > precision:
        raise ValueError(
            "decimal value has more digits than is legal according "
            + "to the schema's precision"
        )

    # Scale represents the number of digits held after the decimal point.
    if exp < 0:
        if -exp > scale:
            raise ValueError(
                "decimal value requires greater decimal scale than is "
                + "legal according to the schema"
            )


def _unscale_decimal(d: decimal.Decimal) -> int:
    sign, _, exp = d.as_tuple()
    if exp == 0:
        unscaled = int(d)
    elif exp < 0:
        unscaled = int(d.scaleb(-exp))
    else:
        unscaled = int(d)
    if sign:
        unscaled = -unscaled
    return unscaled


# UUID
def decode_uuid(src: IO[bytes]) -> uuid.UUID:
    raw = decode_string(src)
    return uuid.UUID(raw)


def encode_uuid(msg: uuid.UUID) -> bytes:
    return encode_string(str(msg))


def uuid_from_bytes(raw: bytes) -> uuid.UUID:
    return uuid.UUID(raw.decode())


# Date
__unix_epoch_day_zero = datetime.date(1970, 1, 1).toordinal()


def decode_date(src: IO[bytes]) -> datetime.date:
    raw = decode_int(src)
    return datetime.date.fromordinal(__unix_epoch_day_zero + raw)


def encode_date(msg: datetime.date) -> bytes:
    return encode_int(msg.toordinal() - __unix_epoch_day_zero)


def decode_time_millis(src: IO[bytes]) -> datetime.time:
    total_millis = decode_int(src)
    hours, remainder = divmod(total_millis, 60 * 60 * 1000)
    minutes, remainder = divmod(remainder, 60 * 1000)
    seconds, remainder = divmod(remainder, 1000)
    microseconds = (remainder % 1000) * 1000
    return datetime.time(
        hour=hours, minute=minutes, second=seconds, microsecond=microseconds
    )


def encode_time_millis(msg: datetime.time) -> bytes:
    val = (
        msg.hour * 60 * 60 * 1000
        + msg.minute * 60 * 1000
        + msg.second * 1000
        + msg.microsecond // 1000
    )
    return encode_int(val)


def decode_time_micros(src: IO[bytes]) -> datetime.time:
    total_micros = decode_long(src)
    return time_micros_from_int(total_micros)


def encode_time_micros(msg: datetime.time) -> bytes:
    val = (
        msg.hour * 60 * 60 * 1000000
        + msg.minute * 60 * 1000000
        + msg.second * 1000000
        + msg.microsecond
    )
    return encode_int(val)


def time_micros_from_int(total_micros: int) -> datetime.time:
    hours, remainder = divmod(total_micros, 60 * 60 * 1000000)
    minutes, remainder = divmod(remainder, 60 * 1000000)
    seconds, remainder = divmod(remainder, 1000000)
    microseconds = remainder % 1000000
    return datetime.time(
        hour=hours, minute=minutes, second=seconds, microsecond=microseconds
    )


__unix_epoch_start_with_tz = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
__unix_epoch_start_without_tz = datetime.datetime(1970, 1, 1)


def decode_timestamp_millis(src: IO[bytes]) -> datetime.datetime:
    total_millis = decode_long(src)
    return timestamp_millis_from_int(total_millis)


def encode_timestamp_millis(msg: datetime.datetime) -> bytes:
    if msg.tzinfo is None:
        epoch_delta = msg - __unix_epoch_start_without_tz
    else:
        epoch_delta = msg - __unix_epoch_start_with_tz
    val = (
        epoch_delta.days * 24 * 60 * 60 * 1000
        + epoch_delta.seconds * 1000
        + epoch_delta.microseconds // 1000
    )
    return encode_long(val)


def timestamp_millis_from_int(total_millis: int) -> datetime.datetime:
    return __unix_epoch_start_with_tz + datetime.timedelta(
        microseconds=total_millis * 1000
    )


def decode_timestamp_micros(src: IO[bytes]) -> datetime.datetime:
    total_micros = decode_long(src)
    return timestamp_micros_from_int(total_micros)


def encode_timestamp_micros(msg: datetime.datetime) -> bytes:
    if msg.tzinfo is None:
        epoch_delta = msg - __unix_epoch_start_without_tz
    else:
        epoch_delta = msg - __unix_epoch_start_with_tz
    val = (
        epoch_delta.days * 24 * 60 * 60 * 1000000
        + epoch_delta.seconds * 1000000
        + epoch_delta.microseconds
    )
    return encode_long(val)


def timestamp_micros_from_int(total_micros: int) -> datetime.datetime:
    return __unix_epoch_start_with_tz + datetime.timedelta(microseconds=total_micros)
