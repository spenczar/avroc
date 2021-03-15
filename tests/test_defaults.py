import pytest
import io
import avroc.messages

class default_testcase:
    def __init__(self, label, field_schema, field_default, field_explicit):
        self.label = label

        self.schema_with_default = {
            "type": "record",
            "name": "Record",
            "fields": [
                {"name": "F", "type": field_schema, "default": field_default},
            ],
        }
        self.schema_without_default = {
            "type": "record",
            "name": "Record",
            "fields": [
                {"name": "F", "type": field_schema},
            ],
        }
        self.schema_without_field = {
            "type": "record",
            "name": "Record",
            "fields": []
        }
        self.message_with_default = {"F": field_default}
        self.message_with_explicit_value = {"F": field_explicit}
        self.message_without_default = {}

    def assert_writer_sets_defaults(self):
        """
        Use a schema with a default set.
        Write a message which is missing the defaulted field.
        Read with the writer's schema.

        The reader see the field set to the default value.
        """
        enc = avroc.messages.message_encoder(self.schema_with_default)
        encoded = enc(self.message_without_default)

        dec = avroc.messages.message_decoder(self.schema_with_default)
        output = dec(io.BytesIO(encoded))

        assert output == self.message_with_default, "writer should set default value"

    def assert_reader_adds_defaults(self):
        """
        Use a schema without a field.
        Write a message which is missing the defaulted field.
        Read with an updated schema with a new field with a default added.

        The reader see the field set to the default value.
        """
        enc = avroc.messages.message_encoder(self.schema_without_field)
        encoded = enc(self.message_without_default)

        dec = avroc.messages.message_decoder(self.schema_without_field, self.schema_with_default)
        output = dec(io.BytesIO(encoded))

        assert output == self.message_with_default, "reader should set default value"

    def assert_read_nondefault_value(self):
        """
        Use a schema without a default set.
        Write a message which holds a non-default value.
        Read with an updated schema with a default added.

        The reader should see the field set as set to the non-default value.
        """
        enc = avroc.messages.message_encoder(self.schema_without_default)
        encoded = enc(self.message_with_explicit_value)

        dec = avroc.messages.message_decoder(self.schema_without_default, self.schema_with_default)
        output = dec(io.BytesIO(encoded))

        assert output == self.message_with_explicit_value, "writer shouldnt use default when value is set"

    def assert_write_nondefault_value(self):
        """
        Use a schema with a default set.
        Write a message which holds a non-default value.
        Read with the writer's schema.

        The reader should see the field set as set to the non-default value.
        """
        enc = avroc.messages.message_encoder(self.schema_with_default)
        encoded = enc(self.message_with_explicit_value)

        dec = avroc.messages.message_decoder(self.schema_with_default)
        output = dec(io.BytesIO(encoded))

        assert output == self.message_with_explicit_value, "reader shouldnt use default when value is set"

testcases = [
    default_testcase("int", "int", 3, 4),
    default_testcase("boolean", "boolean", False, True),
    default_testcase("string", "string", "a", "b"),
    default_testcase("verbose primitive", {"type": "string"}, "a", "b"),
    default_testcase("map", {"type": "map", "values": "int"}, {"a": 1}, {"b": 2}),
    default_testcase("array", {"type": "array", "items": "int"}, [1, 2, 3], [4, 5, 6]),
    default_testcase("enum", {"type": "enum", "symbols": ["A", "B", "C"], "name": "E"}, "A", "C"),
    default_testcase("empty record", {"type": "record", "name": "Rec", "fields": []}, {}, {}),
    default_testcase(
        "record",
        {"type": "record", "name": "Rec", "fields": [
            {"name": "F1", "type": "string"},
            {"name": "F2", "type": "string", "default": "V2"}
        ]},
        {"F1": "D1", "F2": "D2"},
        {"F1": "E1", "F2": "E2"}
    ),
    default_testcase(
        "nested record",
        {"type": "record", "name": "Outer", "fields": [
            {"name": "child", "type": {
                "type": "record",
                "name": "Inner",
                "fields": [
                    {"name": "int_field", "type": "int"},
                ]
            }},
        ]},
        {"child": {"int_field": 3}},
        {"child": {"int_field": 4}},
    ),
]

# bytes and fixed fields need special separate construction for testing
# because their representation as a default is different from their representation
# as a decoded message.

# As a default, they are represented in JSON, so they're an encoded string.
# But once decoded, they should be a bytes-typed value.
bytes_case = default_testcase("bytes", "bytes", "1111", "2222",)
bytes_case.message_with_default = {"F": b'1111'}
bytes_case.message_with_explicit_value = {"F": b'2222'}
testcases.append(bytes_case)

fixed_case = default_testcase("fixed", {"type": "fixed", "size": 4, "name": "Fix"}, '1111', '2222')
fixed_case.message_with_default = {"F": b'1111'}
fixed_case.message_with_explicit_value = {"F": b'2222'}
testcases.append(fixed_case)

@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_writer_sets_defaults(case):
    case.assert_writer_sets_defaults()

@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_reader_adds_defaults(case):
    case.assert_reader_adds_defaults()

@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_read_nondefault_value(case):
    case.assert_read_nondefault_value()

@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_write_nondefault_value(case):
    case.assert_write_nondefault_value()
