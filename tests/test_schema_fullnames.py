from avroc.schema import load_schema
from avroc.util import SchemaType
import pytest
import copy

def test_fullname_without_namespace():
    schema = load_schema({
        "type": "record",
        "name": "Record",
        "fields": [],
    })
    assert schema.fullname() == "Record"

def test_fullname_with_namespace():
    schema = load_schema({
        "type": "record",
        "name": "Record",
        "namespace": "foo.bar",
        "fields": [],
    })
    assert schema.fullname() == "foo.bar.Record"

def test_fullname_with_qualified_name():
    # When a name contains dots, it is always authoritative, overruling any
    # namespace specified.
    schema = load_schema({
        "type": "record",
        "name": "qux.baz.Record",
        "namespace": "foo.bar",
        "fields": [],
    })
    assert schema.fullname() == "qux.baz.Record"

def test_fullname_within_explicit_namespace():
    # When a named type lacks a namespace, the most-tightly enclosing namespace
    # is used.
    schema = load_schema({
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
    })
    assert schema.fullname() == "foo.bar.Record"
    child_one = schema.fields[0].type
    assert child_one.fullname() == "foo.bar.ChildRecord"
    child_two = schema.fields[1].type
    assert child_two.fullname() == "foo.bar.ChildFixed"

def test_fullname_within_implicit_namespace():
    # When a named type lacks a namespace, the most-tightly enclosing namespace
    # is used. A fully-qualified name on an enclosing record is treated as
    # implicitly creating a namespace.
    schema = load_schema({
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
    })
    assert schema.fullname() == "qux.baz.Record"
    child_one = schema.fields[0].type
    assert child_one.fullname() == "qux.baz.ChildRecord"
    child_two = schema.fields[1].type
    assert child_two.fullname() == "qux.baz.ChildFixed"

def test_full_alias_names():
    schema = load_schema({
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
        ],
    })
    assert schema.fullname() == "foo.bar.Record"
    child_record_schema = schema.fields[0].type
    assert child_record_schema.fullname() == "foo.bar.ChildRecord"
    child_record_fullaliases = child_record_schema.fullaliases()
    assert child_record_fullaliases[0] == "foo.bar.ChildRecordAlias"
    assert child_record_fullaliases[1] == "absolute.ChildRecordAlias"
