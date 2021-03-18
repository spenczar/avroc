import pytest

import avroc.schema


class ok:
    def __init__(self, label, schema):
        self.label = label
        self.schema = schema

    def run(self):
        avroc.schema.load_schema(self.schema)


class should_warn:
    def __init__(self, label, schema, warning_matcher):
        self.label = label
        self.schema = schema
        self.warning_matcher = warning_matcher

    def run(self):
        with pytest.warns(
            avroc.schema.SchemaValidationWarning, match=self.warning_matcher
        ):
            avroc.schema.load_schema(self.schema)


class should_error:
    def __init__(
        self,
        label,
        schema,
        error_matcher=None,
        error_class=avroc.schema.SchemaValidationError,
    ):
        self.label = label
        self.schema = schema
        self.error_matcher = error_matcher
        self.error_class = error_class

    def run(self):
        if self.error_matcher is not None:
            with pytest.raises(self.error_class, match=self.error_matcher):
                avroc.schema.load_schema(self.schema)
        with pytest.raises(self.error_class):
            avroc.schema.load_schema(self.schema)


testcases = [
    ok("primitive int", "int"),
    ok("wrapped int", {"type": "int"}),
    ok("union", ["int", "long", "float", "null"]),
    ok("record", {"type": "record", "name": "abc", "fields": []}),
    ok(
        "record with fields",
        {
            "type": "record",
            "name": "abc",
            "fields": [
                {"name": "f1", "type": "string"},
                {"name": "f2", "type": {"type": "record", "name": "def", "fields": []}},
            ],
        },
    ),
    ok(
        "record with fields with defaults",
        {
            "type": "record",
            "name": "abc",
            "fields": [
                {"name": "f1", "type": "string", "default": "abc"},
                {
                    "name": "f2",
                    "default": {"int_field": 2},
                    "type": {
                        "type": "record",
                        "name": "def",
                        "fields": [{"type": "int", "name": "int_field"}],
                    },
                },
            ],
        },
    ),
    ok(
        "logical types",
        {
            "type": "record",
            "name": "abc",
            "fields": [
                {
                    "name": "l1",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                    },
                },
                {
                    "name": "l2",
                    "type": {
                        "type": "fixed",
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "size": 4,
                        "name": "x",
                    },
                },
                {
                    "name": "uuid",
                    "type": {"type": "string", "logicalType": "uuid"},
                },
                {
                    "name": "date",
                    "type": {"type": "int", "logicalType": "date"},
                    "default": 0,
                },
                {
                    "name": "time-millis",
                    "type": {"type": "int", "logicalType": "time-millis"},
                    "default": 1,
                },
                {
                    "name": "time-micros",
                    "type": {"type": "long", "logicalType": "time-micros"},
                    "default": 2,
                },
                {
                    "name": "timestamp-millis",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                    "default": 4,
                },
                {
                    "name": "timestamp-micros",
                    "type": {"type": "long", "logicalType": "timestamp-micros"},
                    "default": 5,
                },
                {
                    "name": "duration",
                    "type": {
                        "type": "fixed",
                        "name": "d",
                        "size": 12,
                        "logicalType": "duration",
                    },
                },
            ],
        },
    ),
    # Basic failures
    should_error("undefined type", "Int", "unknown name Int"),
    should_error(
        "invalid type",
        set(),
        "must be a list, str, or dict",
        ValueError,
    ),
    should_error("empty union", [], "unions must have at least two elements"),
    should_error(
        "single-element union", ["int"], "unions must have at least two elements"
    ),
    should_error("dict without type", {"size": 12}, "missing 'type' field"),
    should_error("dict with undefined type", {"type": "Named"}),
    should_error("non-string type", {"type": False}, "unknown schema type False"),
    # Stuff around name validation
    should_error(
        "record without name", {"type": "record", "fields": []}, error_class=KeyError
    ),
    should_error(
        "enum without name", {"type": "enum", "fields": []}, error_class=KeyError
    ),
    should_error(
        "fixed without name", {"type": "fixed", "fields": []}, error_class=KeyError
    ),
    should_error(
        "name reused",
        [
            {"name": "A", "type": "record", "fields": []},
            {"name": "A", "type": "record", "fields": []},
        ],
        "name A is used twice",
    ),
    should_error(
        "field name reused",
        {
            "name": "Record",
            "type": "record",
            "fields": [
                {"name": "A", "type": "int"},
                {"name": "A", "type": "int"},
            ],
        },
        "field name A is used twice",
    ),
    # Record types
    should_error(
        "record missing fields",
        {"name": "A", "type": "record"},
        error_class=KeyError,
    ),
    # Record field defaults
    ok(
        "record with fields with defaults",
        {
            "type": "record",
            "name": "abc",
            "fields": [
                {"name": "f1", "type": "string", "default": "abc"},
                {
                    "name": "f2",
                    "default": {"int_field": 2},
                    "type": {
                        "type": "record",
                        "name": "def",
                        "fields": [{"type": "int", "name": "int_field"}],
                    },
                },
            ],
        },
    ),
    should_error(
        "default is wrong type",
        {
            "name": "A",
            "type": "record",
            "fields": [{"type": "int", "name": "intfield", "default": "aaa"}],
        },
        "field intfield has an invalid default",
    ),
    should_error(
        "fixed default is wrong size",
        {
            "name": "A",
            "type": "record",
            "fields": [
                {
                    "type": {
                        "type": "fixed",
                        "name": "fixedfield",
                        "default": "\u00ff\ueeff",
                        "size": 1,
                    },
                    "name": "fixedfield",
                }
            ],
        },
        "default value is invalid",
    ),
]


@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_schema_validation(case):
    case.run()
