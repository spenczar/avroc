import io
import pytest
from avroc.runtime import encoding


def roundtrip(value, encode, decode, skip):
    encoded = encode(value)

    buf = io.BytesIO()
    buf.write(encoded)
    end = buf.tell()
    buf.seek(0)

    skip(buf)
    assert buf.tell() == end, "skip ends at wrong spot"

    buf.seek(0)
    dec = decode(buf)
    assert dec == value, "decode gives wrong value"


class TestEncoding:
    long_cases = [
        -1 << 63,
        -1,
        0,
        1,
        1 << 63,
    ]

    @pytest.mark.parametrize("value", long_cases)
    def test_long_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_long,
            encoding.decode_long,
            encoding.skip_long,
        )

    int_cases = [-1 << 31, -1, 0, 1, 1 << 31]

    @pytest.mark.parametrize("value", int_cases)
    def test_int_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_int,
            encoding.decode_int,
            encoding.skip_int,
        )

    float_cases = [float("+inf"), float("-inf"), 0, 1.0, 2.0, 2e8]

    @pytest.mark.parametrize("value", float_cases)
    def test_float_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_float,
            encoding.decode_float,
            encoding.skip_float,
        )

    double_cases = [float("+inf"), float("-inf"), 0, 1.0, 2.0, 1e60]

    @pytest.mark.parametrize("value", double_cases)
    def test_double_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_double,
            encoding.decode_double,
            encoding.skip_double,
        )

    string_cases = ["", "hello", "\t", "µ", "x" * 1000]

    @pytest.mark.parametrize("value", string_cases)
    def test_string_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_string,
            encoding.decode_string,
            encoding.skip_string,
        )

    bytes_cases = [b"", b"hello", bytes(range(255))]

    @pytest.mark.parametrize("value", bytes_cases)
    def test_bytes_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_bytes,
            encoding.decode_bytes,
            encoding.skip_bytes,
        )

    boolean_cases = [False, True]

    @pytest.mark.parametrize("value", boolean_cases)
    def test_boolean_roundtrip(self, value):
        roundtrip(
            value,
            encoding.encode_boolean,
            encoding.decode_boolean,
            encoding.skip_boolean,
        )

    def test_null_roundtrip(self):
        roundtrip(None, encoding.encode_null, encoding.decode_null, encoding.skip_null)
