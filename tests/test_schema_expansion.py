from avroc.schema import expand_names
from avroc.util import SchemaType
import pytest
import copy


class testcase:
    def __init__(self, label: str, before: SchemaType, after: SchemaType):
        self.label = label
        self.before = before
        self.after = after

    def run(self):
        untouched_before = copy.deepcopy(self.before)
        assert expand_names(self.before, "") == self.after
        assert self.before == untouched_before, "input schema should not be mutated"


testcases = [
    testcase("unnamed", "int", "int"),
    testcase(
        label="record with namespace",
        before={
            "type": "record",
            "name": "Record",
            "namespace": "foo.bar",
            "fields": [],
        },
        after={
            "type": "record",
            "name": "foo.bar.Record",
            "namespace": "foo.bar",
            "fields": [],
        },
    ),
    testcase(
        label="record with fullname and namespace",
        before={
            "type": "record",
            "name": "qux.baz.Record",
            "namespace": "foo.bar",
            "fields": [],
        },
        after={
            "type": "record",
            "name": "qux.baz.Record",
            "namespace": "foo.bar",
            "fields": [],
        },
    ),
    testcase(
        label="child of record with namespace",
        before={
            "type": "record",
            "name": "Record",
            "namespace": "foo.bar",
            "fields": [
                {
                    "name": "record_field",
                    "type": {"type": "record", "name": "ChildRecord", "fields": []},
                },
                {
                    "name": "fixed_field",
                    "type": {"type": "fixed", "name": "ChildFixed", "size": 4},
                },
            ],
        },
        after={
            "type": "record",
            "name": "foo.bar.Record",
            "namespace": "foo.bar",
            "fields": [
                {
                    "name": "record_field",
                    "type": {
                        "type": "record",
                        "name": "foo.bar.ChildRecord",
                        "fields": [],
                    },
                },
                {
                    "name": "fixed_field",
                    "type": {"type": "fixed", "name": "foo.bar.ChildFixed", "size": 4},
                },
            ],
        },
    ),
    testcase(
        label="child of record with fullname",
        before={
            "type": "record",
            "name": "qux.baz.Record",
            "fields": [
                {
                    "name": "record_field",
                    "type": {"type": "record", "name": "ChildRecord", "fields": []},
                },
                {
                    "name": "fixed_field",
                    "type": {"type": "fixed", "name": "ChildFixed", "size": 4},
                },
            ],
        },
        after={
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
    ),
    testcase(
        label="alias in child",
        before={
            "type": "record",
            "name": "Record",
            "namespace": "foo.bar",
            "fields": [
                {
                    "name": "record_field",
                    "type": {
                        "type": "record",
                        "name": "ChildRecord",
                        "fields": [],
                        "aliases": ["ChildRecordAlias", "absolute.ChildRecordAlias"],
                    },
                },
                {
                    "name": "fixed_field",
                    "type": {
                        "type": "fixed",
                        "name": "ChildFixed",
                        "size": 4,
                        "aliases": ["ChildFixedAlias", "absolute.ChildFixedAlias"],
                    },
                },
            ],
        },
        after={
            "type": "record",
            "name": "foo.bar.Record",
            "namespace": "foo.bar",
            "fields": [
                {
                    "name": "record_field",
                    "type": {
                        "type": "record",
                        "name": "foo.bar.ChildRecord",
                        "fields": [],
                        "aliases": [
                            "foo.bar.ChildRecordAlias",
                            "absolute.ChildRecordAlias",
                        ],
                    },
                },
                {
                    "name": "fixed_field",
                    "type": {
                        "type": "fixed",
                        "name": "foo.bar.ChildFixed",
                        "size": 4,
                        "aliases": [
                            "foo.bar.ChildFixedAlias",
                            "absolute.ChildFixedAlias",
                        ],
                    },
                },
            ],
        },
    ),
    testcase(
        label="array using namespaced reference",
        before={
            "type": "record",
            "name": "foo",
            "namespace": "outer_ns",
            "fields": [
                {
                    "name": "field1",
                    "type": {
                        "name": "referenced_by_name",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "array_field",
                    "type": {
                        "type": "array",
                        "items": "referenced_by_name",
                    }
                }
            ]
        },
        after={
            "type": "record",
            "name": "outer_ns.foo",
            "namespace": "outer_ns",
            "fields": [
                {
                    "name": "field1",
                    "type": {
                        "name": "outer_ns.referenced_by_name",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "array_field",
                    "type": {
                        "type": "array",
                        "items": "outer_ns.referenced_by_name",
                    }
                }
            ]
        }
    ),
    testcase(
        label="map using namespaced reference",
        before={
            "type": "record",
            "name": "foo",
            "namespace": "outer_ns",
            "fields": [
                {
                    "name": "field1",
                    "type": {
                        "name": "referenced_by_name",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "map_field",
                    "type": {
                        "type": "map",
                        "values": "referenced_by_name",
                    }
                }
            ]
        },
        after={
            "type": "record",
            "namespace": "outer_ns",
            "name": "outer_ns.foo",
            "fields": [
                {
                    "name": "field1",
                    "type": {
                        "name": "outer_ns.referenced_by_name",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "map_field",
                    "type": {
                        "type": "map",
                        "values": "outer_ns.referenced_by_name",
                    }
                }
            ]
        }
    ),
    testcase(
        label="enum using namespaced reference",
        before={
            "type": "record",
            "name": "foo",
            "namespace": "outer_ns",
            "fields": [
                {
                    "name": "field1",
                    "type": {
                        "name": "referenced_by_name",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "enum_field",
                    "type": ["referenced_by_name", "null"],
                }
            ]
        },
        after={
            "type": "record",
            "name": "outer_ns.foo",
            "namespace": "outer_ns",
            "fields": [
                {
                    "name": "field1",
                    "type": {
                        "name": "outer_ns.referenced_by_name",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "enum_field",
                    "type": ["outer_ns.referenced_by_name", "null"],
                }
            ]
        }
    ),
]


@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_schema_name_expansion(case):
    case.run()
