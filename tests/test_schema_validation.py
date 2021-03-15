import pytest

import avroc.schema


class ok:
    def __init__(self, label, schema):
        self.label = label
        self.schema = schema

    def run(self):
        avroc.schema.validate(self.schema)


class should_warn:
    def __init__(self, label, schema, warning_matcher):
        self.label = label
        self.schema = schema
        self.warning_matcher = warning_matcher

    def run(self):
        with pytest.warns(
            avroc.schema.SchemaValidationWarning, match=self.warning_matcher
        ):
            avroc.schema.validate(self.schema)


class should_error:
    def __init__(self, label, schema, error_matcher):
        self.label = label
        self.schema = schema
        self.error_matcher = error_matcher

    def run(self):
        with pytest.raises(
            avroc.schema.SchemaValidationError, match=self.error_matcher
        ):
            avroc.schema.validate(self.schema)


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
    # Basic failures
    should_error("undefined type", "Int", "'Int' is not defined"),
    should_error(
        "invalid type",
        set(),
        "schema types should only be str, list, or dict; this is a <class 'set'>",
    ),
    should_error("empty union", [], "unions must have at least two elements"),
    should_error(
        "single-element union", ["int"], "unions must have at least two elements"
    ),
    should_error("dict without type", {"size": 12}, "missing required field 'type'"),
    should_error(
        "dict with undefined type", {"type": "Named"}, "'Named' is not defined"
    ),
    should_error("non-string type", {"type": False}, "must have a string value"),
    # Stuff around name validation
    should_error(
        "record without name",
        {"type": "record", "fields": []},
        "name field is required",
    ),
    should_error(
        "enum without name", {"type": "enum", "fields": []}, "name field is required"
    ),
    should_error(
        "fixed without name", {"type": "fixed", "fields": []}, "name field is required"
    ),
    should_error(
        "record reuses name",
        [
            {"name": "A", "type": "record", "fields": []},
            {"name": "A", "type": "record", "fields": []},
        ],
        "names cannot be defined twice",
    ),
    # Record types
    should_error(
        "record missing fields",
        {"name": "A", "type": "record"},
        "records must have fields",
    ),
    should_error(
        "record fields is dict",
        {"name": "A", "type": "record", "fields": {"type": "int", "name": "x"}},
        "record fields should be a list",
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
        {"name": "A", "type": "record", "fields": [{"type": "int", "name": "intfield", "default": False}]},
         "schema invalid at intfield: field default's type doesn't match"
    ),
    should_error(
        "fixed default is wrong size",
        {"name": "A", "type": "record", "fields": [{"type": "fixed", "name": "fixedfield", "default": "\u00ff", "size": 1}]},
         "schema invalid at intfield: field default's type doesn't match"
    ),
]


@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_schema_validation(case):
    case.run()
