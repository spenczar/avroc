"""
This module contains routines for encoding, decoding, and skipping Avro's
logical types.
"""
from avroc.runtime import encoding
from decimal import Decimal
from uuid import UUID


def cast_decimal(src: IO[bytes]) -> Decimal:
    raise NotImplementedError()


def skip_decimal(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_decimal(msg: Decimal, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_uuid(src: IO[bytes]) -> UUID:
    return UUID(encoding.decode_string(src))


def skip_uuid(src: IO[bytes]) -> None:
    encoding.skip_string(src)


def encode_uuid(msg: UUID, dst: IO[bytes]) -> None:
    encoding.encode_string(str(msg), dst)


def decode_date(src: IO[bytes]) -> Date:
    raise NotImplementedError()


def skip_date(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_date(msg: Date, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_time_millis(src: IO[bytes]) -> Time_Millis:
    raise NotImplementedError()


def skip_time_millis(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_time_millis(msg: Time_Millis, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_time_micros(src: IO[bytes]) -> Time_Micros:
    raise NotImplementedError()


def skip_time_micros(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_time_micros(msg: Time_Micros, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_timestamp_millis(src: IO[bytes]) -> Timestamp_Millis:
    raise NotImplementedError()


def skip_timestamp_millis(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_timestamp_millis(msg: Timestamp_Millis, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_timestamp_micros(src: IO[bytes]) -> Timestamp_Micros:
    raise NotImplementedError()


def skip_timestamp_micros(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_timestamp_micros(msg: Timestamp_Micros, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_local_timestamp_millis(src: IO[bytes]) -> Local_Timestamp_Millis:
    raise NotImplementedError()


def skip_local_timestamp_millis(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_local_timestamp_millis(msg: Local_Timestamp_Millis, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_local_timestamp_micros(src: IO[bytes]) -> Local_Timestamp_Micros:
    raise NotImplementedError()


def skip_local_timestamp_micros(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_local_timestamp_micros(msg: Local_Timestamp_Micros, dst: IO[bytes]) -> None:
    raise NotImplementedError()


def decode_duration(src: IO[bytes]) -> Duration:
    raise NotImplementedError()


def skip_duration(src: IO[bytes]) -> None:
    raise NotImplementedError()


def encode_duration(msg: Duration, dst: IO[bytes]) -> None:
    raise NotImplementedError()
