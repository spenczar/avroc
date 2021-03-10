import pytest
import io
import avroc.codegen.resolution
import fastavro.read

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
        c = avroc.codegen.resolution.ResolvedReaderCompiler(self.writer_schema, self.reader_schema)
        try:
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
