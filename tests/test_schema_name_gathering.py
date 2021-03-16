from typing import Dict

from avroc.schema import gather_named_types, load_schema
import pytest
import copy


class testcase:
    def __init__(self, label, schema, expected):
        self.label = label
        self.schema = load_schema(schema)
        self.expected = {k: load_schema(v) for k, v in expected.items()}

    def run(self):
        assert gather_named_types(self.schema) == self.expected


testcases = [
    testcase("unnamed", "int", {}),
    testcase(
        label="record",
        schema={"type": "record", "name": "foo.bar.Record", "fields": []},
        expected={
            "foo.bar.Record": {
                "type": "record",
                "name": "foo.bar.Record",
                "fields": [],
            },
        },
    ),
    testcase(
        label="array of records",
        schema={
            "type": "array",
            "items": {"type": "record", "name": "foo.bar.Record", "fields": []},
        },
        expected={
            "foo.bar.Record": {
                "type": "record",
                "name": "foo.bar.Record",
                "fields": [],
            },
        },
    ),
    testcase(
        label="map of records",
        schema={
            "type": "map",
            "values": {"type": "record", "name": "foo.bar.Record", "fields": []},
        },
        expected={
            "foo.bar.Record": {
                "type": "record",
                "name": "foo.bar.Record",
                "fields": [],
            },
        },
    ),
    testcase(
        label="map of records",
        schema={
            "type": "map",
            "values": {"type": "record", "name": "foo.bar.Record", "fields": []},
        },
        expected={
            "foo.bar.Record": {
                "type": "record",
                "name": "foo.bar.Record",
                "fields": [],
            },
        },
    ),
    testcase(
        label="record with childen",
        schema={
            "type": "record",
            "name": "qux.baz.Record",
            "fields": [
                {
                    "name": "record_field",
                    "type": {
                        "type": "record",
                        "name": "qux.baz.ChildRecord",
                        "fields": [],
                    },
                },
                {
                    "name": "fixed_field",
                    "type": {"type": "fixed", "name": "qux.baz.ChildFixed", "size": 4},
                },
            ],
        },
        expected={
            "qux.baz.Record": {
                "type": "record",
                "name": "qux.baz.Record",
                "fields": [
                    {
                        "name": "record_field",
                        "type": {
                            "type": "record",
                            "name": "qux.baz.ChildRecord",
                            "fields": [],
                        },
                    },
                    {
                        "name": "fixed_field",
                        "type": {
                            "type": "fixed",
                            "name": "qux.baz.ChildFixed",
                            "size": 4,
                        },
                    },
                ],
            },
            "qux.baz.ChildRecord": {
                "type": "record",
                "name": "qux.baz.ChildRecord",
                "fields": [],
            },
            "qux.baz.ChildFixed": {
                "type": "fixed",
                "name": "qux.baz.ChildFixed",
                "size": 4,
            },
        },
    ),
    testcase(
        label="record with childen with aliases",
        schema={
            "type": "record",
            "name": "qux.baz.Record",
            "fields": [
                {
                    "name": "record_field",
                    "type": {
                        "type": "record",
                        "name": "qux.baz.ChildRecord",
                        "fields": [],
                    },
                },
                {
                    "name": "fixed_field",
                    "type": {
                        "type": "fixed",
                        "name": "qux.baz.ChildFixed",
                        "aliases": ["foo.bar.Alias1", "foo.bar.Alias2"],
                        "size": 4,
                    },
                },
            ],
        },
        expected={
            "qux.baz.Record": {
                "type": "record",
                "name": "qux.baz.Record",
                "fields": [
                    {
                        "name": "record_field",
                        "type": {
                            "type": "record",
                            "name": "qux.baz.ChildRecord",
                            "fields": [],
                        },
                    },
                    {
                        "name": "fixed_field",
                        "type": {
                            "type": "fixed",
                            "aliases": ["foo.bar.Alias1", "foo.bar.Alias2"],
                            "name": "qux.baz.ChildFixed",
                            "size": 4,
                        },
                    },
                ],
            },
            "qux.baz.ChildRecord": {
                "type": "record",
                "name": "qux.baz.ChildRecord",
                "fields": [],
            },
            "qux.baz.ChildFixed": {
                "type": "fixed",
                "name": "qux.baz.ChildFixed",
                "aliases": ["foo.bar.Alias1", "foo.bar.Alias2"],
                "size": 4,
            },
            "foo.bar.Alias1": {
                "type": "fixed",
                "name": "qux.baz.ChildFixed",
                "aliases": ["foo.bar.Alias1", "foo.bar.Alias2"],
                "size": 4,
            },
            "foo.bar.Alias2": {
                "type": "fixed",
                "name": "qux.baz.ChildFixed",
                "aliases": ["foo.bar.Alias1", "foo.bar.Alias2"],
                "size": 4,
            },
        },
    ),
]


@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_schema_name_expansion(case):
    case.run()
