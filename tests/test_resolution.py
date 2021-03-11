import pytest
import io
import avroc.codegen.resolution
import fastavro.read

import uuid
import decimal
import datetime

class testcase:
    def __init__(self, label, writer_schema, reader_schema, input_msg, output_msg):
        self.label = label
        self.writer_schema = writer_schema
        self.reader_schema = reader_schema
        self.input_msg = input_msg
        self.output_msg = output_msg

    def assert_reader(self):
        message_encoded = io.BytesIO()
        fastavro.write.schemaless_writer(message_encoded, self.writer_schema, self.input_msg)
        message_encoded.seek(0)

        c = avroc.codegen.resolution.ResolvedReaderCompiler(self.writer_schema, self.reader_schema)
        reader = c.compile()
        have = reader(message_encoded)
        assert have == self.output_msg, "reader behavior mismatch"
        assert type(have) == type(self.output_msg), "reader type mismatch"

class failcase:
    def __init__(self, label, writer_schema, reader_schema, input_msg, error_matcher):
        self.label = label
        self.writer_schema = writer_schema
        self.reader_schema = reader_schema
        self.input_msg = input_msg
        self.error_matcher = error_matcher

    def assert_reader(self):
        message_encoded = io.BytesIO()
        fastavro.write.schemaless_writer(message_encoded, self.writer_schema, self.input_msg)
        message_encoded.seek(0)

        failed = False
        try:
            c = avroc.codegen.resolution.ResolvedReaderCompiler(self.writer_schema, self.reader_schema)
            reader = c.compile()
        except Exception as e:
            failed = True
            assert self.error_matcher in repr(e)
            return

        try:
            have = reader(message_encoded)
        except Exception as e:
            failed = True
            assert self.error_matcher in repr(e)
        assert failed, "expected a failure"

type_promotion_testcases = [
    testcase(
        label="int to long",
        writer_schema="int",
        reader_schema="long",
        input_msg=1,
        output_msg=1,
    ),
    testcase(
        label="int to float",
        writer_schema="int",
        reader_schema="float",
        input_msg=1,
        output_msg=1.0,
    ),
    testcase(
        label="int to double",
        writer_schema="int",
        reader_schema="double",
        input_msg=1,
        output_msg=1.0,
    ),
    testcase(
        label="long to float",
        writer_schema="long",
        reader_schema="float",
        input_msg=1,
        output_msg=1.0,
    ),
    testcase(
        label="long to double",
        writer_schema="long",
        reader_schema="double",
        input_msg=1,
        output_msg=1.0,
    ),
    testcase(
        label="string to bytes",
        writer_schema="string",
        reader_schema="bytes",
        input_msg="Hello! 😀",
        output_msg=b'Hello! \xf0\x9f\x98\x80',
    ),
    testcase(
        label="bytes to string",
        writer_schema="bytes",
        reader_schema="string",
        input_msg=b'Hello! \xf0\x9f\x98\x80',
        output_msg="Hello! 😀",
    )
]

@pytest.mark.parametrize("case", type_promotion_testcases, ids=[tc.label for tc in type_promotion_testcases])
def test_resolving_reader_type_promotion(case):
    case.assert_reader()

enum_testcases = [
    testcase(
        label="unchanged",
        writer_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        reader_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        input_msg="red",
        output_msg="red",
    ),
    testcase(
        label="reordered",
        writer_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        reader_schema={"type": "enum", "name": "color", "symbols": ["yellow", "red", "blue"]},
        input_msg="red",
        output_msg="red",
    ),
    testcase(
        label="removed",
        writer_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        reader_schema={"type": "enum", "name": "color", "symbols": ["red", "blue"]},
        input_msg="red",
        output_msg="red",
    ),
    testcase(
        label="added",
        writer_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        reader_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow", "green"]},
        input_msg="red",
        output_msg="red",
    ),
    failcase(
        label="removed encoded value",
        writer_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        reader_schema={"type": "enum", "name": "color", "symbols": ["blue", "yellow"]},
        input_msg="red",
        error_matcher="KeyError",
    ),
    testcase(
        label="removed encoded value uses default",
        writer_schema={"type": "enum", "name": "color", "symbols": ["red", "blue", "yellow"]},
        reader_schema={"type": "enum", "name": "color", "symbols": ["blue", "yellow"], "default": "blue"},
        input_msg="red",
        output_msg="blue",
    )
]

@pytest.mark.parametrize("case", enum_testcases, ids=[tc.label for tc in enum_testcases])
def test_resolving_reader_enums(case):
    case.assert_reader()

union_testcases = [
    testcase(
        label="read from union",
        writer_schema=["null", "int", "long", "boolean"],
        reader_schema="int",
        input_msg=1,
        output_msg=1,
    ),
    testcase(
        label="read from union with type promotion",
        writer_schema=["null", "int", "long", "boolean"],
        reader_schema="float",
        input_msg=1,
        output_msg=1.0,
    ),
    failcase(
        label="read from union with incompatible type stored",
        writer_schema=["null", "int"],
        reader_schema="int",
        input_msg=None,
        error_matcher="data written with type null is incompatible"
    )
]

@pytest.mark.parametrize("case", union_testcases, ids=[tc.label for tc in union_testcases])
def test_resolving_reader_unions(case):
    case.assert_reader()

record_testcases = [
    testcase(
        label="unchanged",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "int_field"},
                {"type": "float", "name": "float_field"},
                {"type": "string", "name": "string_field"},
            ],
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "int_field"},
                {"type": "float", "name": "float_field"},
                {"type": "string", "name": "string_field"},                               ]
        },
        input_msg={"int_field": 1, "float_field": 0.5, "string_field": "hello"},
        output_msg={"int_field": 1, "float_field": 0.5, "string_field": "hello"},
    ),
    testcase(
        label="reordered",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "int_field"},
                {"type": "float", "name": "float_field"},
                {"type": "string", "name": "string_field"},
            ],
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "float", "name": "float_field"},
                {"type": "string", "name": "string_field"},                                    {"type": "int", "name": "int_field"},
           ]
        },
        input_msg={"int_field": 1, "float_field": 0.5, "string_field": "hello"},
        output_msg={"int_field": 1, "float_field": 0.5, "string_field": "hello"},
    ),
    failcase(
        label="reader has field that writer lacks",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [],
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "int_field"},
            ]
        },
        input_msg={},
        error_matcher="missing field",
    ),
    testcase(
        label="primitive default values",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [],
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "int_field", "default": 1},
                {"type": "long", "name": "long_field", "default": 2},
                {"type": "float", "name": "float_field", "default": 3.0},
                {"type": "double", "name": "double_field", "default": 4.0},
                {"type": "boolean", "name": "boolean_field", "default": True},
                {"type": "bytes", "name": "bytes_field", "default": "\u00ff"},
                {"type": "string", "name": "string_field", "default": "abc"},
                {"type": "null", "name": "null_field", "default": None},
            ]
        },
        input_msg={},
        output_msg={
            "int_field": 1,
            "long_field": 2,
            "float_field": 3.0,
            "double_field": 4.0,
            "boolean_field": True,
            "bytes_field": b"\xc3\xbf",
            "string_field": "abc",
            "null_field": None,
        },
    ),
]

@pytest.mark.parametrize("case", record_testcases, ids=[tc.label for tc in record_testcases])
def test_resolving_reader_records(case):
    case.assert_reader()


record_skip_testcases = [
    testcase(
        label="skip int fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "int_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "int_field": 2, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip null fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "null", "name": "null_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "null_field": None, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip bytes fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "bytes", "name": "bytes_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "bytes_field": b'123', "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip float fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "float", "name": "float_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "float_field": 2.0, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip verbose primitive fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {"type": "string"}, "name": "string_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "string_field": "hello", "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip union fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": ["float", "boolean", "int"], "name": "union_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "union_field": False, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip union fields with nulls missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": ["float", "null"], "name": "union_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "union_field": None, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip record fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {
                    "type": "record",
                    "name": "SubRecord",
                    "fields": [{"type": "string", "name": "subfield"}],
                }, "name": "record_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "record_field": {"subfield": "abc"}, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip record fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {
                    "type": "record",
                    "name": "SubRecord",
                    "fields": [{"type": "string", "name": "subfield"}],
                }, "name": "record_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "record_field": {"subfield": "abc"}, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip array fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {
                    "type": "array",
                    "items": "int",
                }, "name": "array_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "array_field": [2, 4, 6, 8], "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip map fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {
                    "type": "map",
                    "values": "int",
                }, "name": "map_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "map_field": {"a": 2, "b": 3}, "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip enum fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {
                    "type": "enum",
                    "symbols": ["RED", "YELLOW", "BLUE"],
                    "name": "Color",
                }, "name": "enum_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "enum_field": "RED", "end": 3},
        output_msg={"start": 1, "end": 3},
    ),
    testcase(
        label="skip fixed fields missing from reader",
        writer_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": {
                    "type": "fixed",
                    "size": 8,
                    "name": "eightbytes",
                }, "name": "fixed_field"},
                {"type": "int", "name": "end"},
            ]
        },
        reader_schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": "int", "name": "start"},
                {"type": "int", "name": "end"},
            ],
        },
        input_msg={"start": 1, "fixed_field": b'12345678', "end": 3},
        output_msg={"start": 1, "end": 3},
    )
]

@pytest.mark.parametrize("case", record_skip_testcases, ids=[tc.label for tc in record_skip_testcases])
def test_resolving_reader_record_skipping(case):
    case.assert_reader()

array_testcases = [
    testcase(
        label="array of primitives with promotion",
        writer_schema={
            "type": "array",
            "items": "int",
        },
        reader_schema={
            "type": "array",
            "items": "float",
        },
        input_msg=[1, 2, 3, 4, 5],
        output_msg=[1.0, 2.0, 3.0, 4.0, 5.0],
    ),
    testcase(
        label="array of records with skipping",
        writer_schema={
            "type": "array",
            "items": {
                "type": "record",
                "name": "Item",
                "fields": [
                    {"name": "s", "type": "string"},
                    {"name": "i", "type": "int"},
                ]
            },
        },
        reader_schema={
            "type": "array",
            "items": {
                "type": "record",
                "name": "Item",
                "fields": [
                    {"name": "s", "type": "string"},
                ]
            },
        },
        input_msg=[{"s": "a", "i": 1}, {"s": "b", "i": 2}],
        output_msg=[{"s": "a"}, {"s": "b"}],
    ),
]

@pytest.mark.parametrize("case", array_testcases, ids=[tc.label for tc in array_testcases])
def test_resolving_reader_arrays(case):
    case.assert_reader()

map_testcases = [
    testcase(
        label="map of primitives with promotion",
        writer_schema={
            "type": "map",
            "values": "int",
        },
        reader_schema={
            "type": "map",
            "values": "float",
        },
        input_msg={"k1": 1, "k2": 2},
        output_msg={"k1": 1.0, "k2": 2.0},
    ),
    testcase(
        label="map of records with skipping",
        writer_schema={
            "type": "map",
            "values": {
                "type": "record",
                "name": "Item",
                "fields": [
                    {"name": "s", "type": "string"},
                    {"name": "i", "type": "int"},
                ]
            },
        },
        reader_schema={
            "type": "map",
            "values": {
                "type": "record",
                "name": "Item",
                "fields": [
                    {"name": "s", "type": "string"},
                ]
            },
        },
        input_msg={"k1": {"s": "a", "i": 1},
                   "k2": {"s": "b", "i": 2}},
        output_msg={"k1": {"s": "a"}, "k2": {"s": "b"}},
    ),
]

@pytest.mark.parametrize("case", map_testcases, ids=[tc.label for tc in map_testcases])
def test_resolving_reader_maps(case):
    case.assert_reader()


fixed_testcases = [
    testcase(
        label="fixed",
        writer_schema={"name": "F", "type": "fixed", "size": 8},
        reader_schema={"name": "F", "type": "fixed", "size": 8, "extraInfo": True},
        input_msg=b'12345678',
        output_msg=b'12345678',
    ),
    failcase(
        label="changed size",
        writer_schema={"name": "F", "type": "fixed", "size": 8},
        reader_schema={"name": "F", "type": "fixed", "size": 10},
        input_msg=b'12345678',
        error_matcher="schemas do not match"
    ),
]

@pytest.mark.parametrize("case", fixed_testcases, ids=[tc.label for tc in fixed_testcases])
def test_resolving_reader_fixeds(case):
    case.assert_reader()


logical_testcases = [
    testcase(
        label="string to uuid",
        writer_schema="string",
        reader_schema={"type": "string", "logicalType": "uuid"},
        input_msg="f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
        output_msg=uuid.UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
    ),
    testcase(
        label="uuid to string",
        writer_schema={"type": "string", "logicalType": "uuid"},
        reader_schema="string",
        input_msg=uuid.UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
        output_msg="f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    ),
    testcase(
        label="bytes to uuid",
        writer_schema="bytes",
        reader_schema={"type": "string", "logicalType": "uuid"},
        input_msg=b"f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
        output_msg=uuid.UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
    ),
    testcase(
        label="int to time-micros",
        writer_schema="int",
        reader_schema={"type": "long", "logicalType": "time-micros"},
        input_msg=1,
        output_msg=datetime.time(0, 0, 0, 1),
    ),
    testcase(
        label="int to timestamp-millis",
        writer_schema="int",
        reader_schema={"type": "long", "logicalType": "timestamp-millis"},
        input_msg=1,
        output_msg=datetime.datetime(
            1970, 1, 1, 0, 0, 0, 1000, tzinfo=datetime.timezone.utc
        )
    ),
    testcase(
        label="int to timestamp-micros",
        writer_schema="int",
        reader_schema={"type": "long", "logicalType": "timestamp-micros"},
        input_msg=1,
        output_msg=datetime.datetime(
            1970, 1, 1, 0, 0, 0, 1, tzinfo=datetime.timezone.utc
        )
    ),
]
@pytest.mark.parametrize("case", logical_testcases, ids=[tc.label for tc in logical_testcases])
def test_resolving_reader_logicals(case):
    case.assert_reader()


incompatible_testcases = [
    failcase(
        label="mismatched types",
        writer_schema={"type": "array", "items": "int"},
        reader_schema={"type": "map", "items": "int"},
        input_msg=[1, 2, 3],
        error_matcher="schemas do not match"
    ),
    failcase(
        label="not promotable",
        writer_schema="int",
        reader_schema="string",
        input_msg=1,
        error_matcher="schemas do not match"
    ),
    failcase(
        label="union not resolvable",
        writer_schema=["int", "long"],
        reader_schema="string",
        input_msg=1,
        error_matcher="none of the options for the writer union can be resolved"
    )
]

@pytest.mark.parametrize("case", incompatible_testcases, ids=[tc.label for tc in incompatible_testcases])
def test_resolving_reader_schema_incompatibilities(case):
    case.assert_reader()
