"""
This module contains functions for determining the type of an object so that
we can write correct union indexes.
"""
from typing import Any, Set
import decimal
import uuid
import datetime


# Primitives
def is_null(value: Any) -> bool:
    return value is None


def is_boolean(value: Any) -> bool:
    return isinstance(value, bool)


def is_string(value: Any) -> bool:
    return isinstance(value, str)


def is_bytes(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray))


def is_int(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int)
        and -(1 << 31) - 1 <= value <= (1 << 31) - 1
    )


def is_long(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int)
        and -(1 << 63) - 1 <= value <= (1 << 63) - 1
    )


def is_float(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, float)


def is_double(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, float)


def is_fixed(value: Any, size: int) -> bool:
    return isinstance(value, bytes) and len(value) == size


def is_enum(value: Any, symbols: Set[str]) -> bool:
    return isinstance(value, str) and value in symbols


def is_array(value: Any) -> bool:
    # The spec says "Unions may not contain more than one schema with the same
    # type, except for the named types record, fixed, and enum." This means we
    # can do naive stuff to detect arrays.
    return isinstance(value, list)


def is_decimal(value: Any) -> bool:
    return isinstance(value, decimal.Decimal)


def is_uuid(value: Any) -> bool:
    return isinstance(value, uuid.UUID)


def is_date(value: Any) -> bool:
    return isinstance(value, datetime.date)


def is_time(value: Any) -> bool:
    return isinstance(value, datetime.time)


def is_timestamp(value: Any) -> bool:
    return isinstance(value, datetime.datetime)


def is_map(value: Any) -> bool:
    return isinstance(value, dict)


def is_record(value: Any, field_names: Set[str]) -> bool:
    return isinstance(value, dict) and field_names.issuperset(set(value.keys()))
