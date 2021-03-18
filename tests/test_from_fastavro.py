"""
A lot of this code is adapted from the tests in the fastavro project,
https://github.com/fastavro/fastavro. They are copied and modified here under the MIT license for fastavro:

Copyright (c) 2011 Miki Tebeka

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import pytest
import io
import avroc.files
import avroc.messages
import avroc.schema
import avroc.codegen.read
import avroc.codegen.write
import avroc.codegen.resolution
import os.path
import zipfile

from collections import OrderedDict


def roundtrip(schema, records):
    buf = io.BytesIO()
    schema = avroc.schema.load_schema(schema)
    writer = avroc.files.AvroFileWriter(buf, schema)
    for r in records:
        writer.write(r)
    writer.flush()

    buf.seek(0)
    reader = avroc.files.AvroFileReader(buf)
    return list(reader)


def roundtrip_schema_migration(old_schema, new_schema, records):
    buf = io.BytesIO()
    old_schema = avroc.schema.load_schema(old_schema)
    new_schema = avroc.schema.load_schema(new_schema)
    w = avroc.files.AvroFileWriter(buf, old_schema)
    for r in records:
        w.write(r)
    w.flush()
    buf.seek(0)
    reader = avroc.files.AvroFileReader(buf, new_schema)
    new_records = list(reader)
    return new_records


def test_default_values():
    schema = {
        "type": "record",
        "name": "test_default_values",
        "fields": [
            {"name": "default_field", "type": "string", "default": "default_value"}
        ],
    }
    records = [{}]

    new_records = roundtrip(schema, records)
    assert new_records == [{"default_field": "default_value"}]


def test_nullable_values():
    schema = {
        "type": "record",
        "name": "test_nullable_values",
        "fields": [
            {"name": "nullable_field", "type": ["string", "null"]},
            {"name": "field", "type": "string"},
        ],
    }
    records = [{"field": "val"}, {"field": "val", "nullable_field": "no_null"}]

    new_records = roundtrip(schema, records)
    assert new_records == [
        {"nullable_field": None, "field": "val"},
        {"nullable_field": "no_null", "field": "val"},
    ]


def test_repo_caching_issue():
    schema = {
        "type": "record",
        "name": "B",
        "fields": [
            {
                "name": "b",
                "type": {
                    "type": "record",
                    "name": "C",
                    "fields": [{"name": "c", "type": "string"}],
                },
            }
        ],
    }

    records = [{"b": {"c": "test"}}]

    assert records == roundtrip(schema, records)

    other_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "a",
                "type": {
                    "type": "record",
                    "name": "B",
                    "fields": [
                        {
                            "name": "b",
                            "type": {
                                "type": "record",
                                "name": "C",
                                "fields": [{"name": "c", "type": "int"}],
                            },
                        }
                    ],
                },
            },
            {"name": "aa", "type": "B"},
        ],
    }

    records = [{"a": {"b": {"c": 1}}, "aa": {"b": {"c": 2}}}]

    assert records == roundtrip(other_schema, records)


def test_schema_migration_remove_field():
    schema = {
        "type": "record",
        "name": "test_schema_migration_remove_field",
        "fields": [
            {
                "name": "test",
                "type": "string",
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_remove_field",
        "fields": [],
    }

    records = [{"test": "test"}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == [{}]


def test_schema_migration_add_default_field():
    schema = {
        "type": "record",
        "name": "test_schema_migration_add_default_field",
        "fields": [],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_add_default_field",
        "fields": [
            {
                "name": "test",
                "type": "string",
                "default": "default",
            }
        ],
    }

    records = [{}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == [{"test": "default"}]


def test_schema_migration_type_promotion():
    schema = {
        "type": "record",
        "name": "test_schema_migration_type_promotion",
        "fields": [
            {
                "name": "test",
                "type": ["string", "int"],
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_type_promotion",
        "fields": [
            {
                "name": "test",
                "type": ["float", "string"],
            }
        ],
    }

    records = [{"test": 1}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == records


def test_schema_migration_maps_with_union_promotion():
    schema = {
        "type": "record",
        "name": "test_schema_migration_maps_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": ["string", "int"]},
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_maps_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": ["string", "long"]},
            }
        ],
    }

    records = [{"test": {"foo": 1}}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == records


def test_schema_migration_array_with_union_promotion():
    schema = {
        "type": "record",
        "name": "test_schema_migration_array_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "array", "items": ["boolean", "long"]},
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_array_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "array", "items": ["string", "float"]},
            }
        ],
    }

    records = [{"test": [1, 2, 3]}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == records


def test_schema_migration_writer_union():
    schema = {
        "type": "record",
        "name": "test_schema_migration_writer_union",
        "fields": [{"name": "test", "type": ["string", "int"]}],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_writer_union",
        "fields": [{"name": "test", "type": "int"}],
    }

    records = [{"test": 1}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == records


def test_schema_migration_reader_union():
    schema = {
        "type": "record",
        "name": "test_schema_migration_reader_union",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_reader_union",
        "fields": [{"name": "test", "type": ["string", "int"]}],
    }

    records = [{"test": 1}]
    new_records = roundtrip_schema_migration(schema, new_schema, records)
    assert new_records == records


def test_union_records():
    #
    schema = {
        "name": "test_name",
        "namespace": "test",
        "type": "record",
        "fields": [
            {
                "name": "val",
                "type": [
                    {
                        "name": "a",
                        "namespace": "common",
                        "type": "record",
                        "fields": [
                            {"name": "x", "type": "int"},
                            {"name": "y", "type": "int"},
                        ],
                    },
                    {
                        "name": "b",
                        "namespace": "common",
                        "type": "record",
                        "fields": [
                            {"name": "x", "type": "int"},
                            {"name": "y", "type": "int"},
                            {"name": "z", "type": ["null", "int"]},
                        ],
                    },
                ],
            }
        ],
    }

    data = [
        {
            "val": {
                "x": 3,
                "y": 4,
                "z": 5,
            }
        }
    ]

    assert data == roundtrip(schema, data)


def test_ordered_dict_record():
    """
    Write an Avro record using an OrderedDict and read it back. This tests for
    a bug where dict was supported but not dict-like types.
    """
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field", "type": {"type": "string"}}],
    }

    record = OrderedDict()
    record["field"] = "foobar"
    records = [record]

    assert records == roundtrip(schema, records)


def test_ordered_dict_map():
    """
    Write an Avro record containing a map field stored in an OrderedDict, then
    read it back. This tests for a bug where dict was supported but not
    dict-like types.
    """
    schema = {
        "type": "record",
        "name": "test_ordered_dict_map",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": ["string", "int"]},
            }
        ],
    }

    map_ = OrderedDict()
    map_["foo"] = 1
    records = [{"test": map_}]

    assert records == roundtrip(schema, records)


def test_doubles_set_to_zero_on_windows():
    """https://github.com/fastavro/fastavro/issues/154"""

    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
            {"name": "test_float", "type": "double"},
        ],
    }

    records = [
        {
            "station": "011990-99999",
            "temp": 0,
            "test_float": 0.21334215134123513,
            "time": -714214260,
        },
        {
            "station": "011990-99999",
            "temp": 22,
            "test_float": 0.21334215134123513,
            "time": -714213259,
        },
        {
            "station": "011990-99999",
            "temp": -11,
            "test_float": 0.21334215134123513,
            "time": -714210269,
        },
        {
            "station": "012650-99999",
            "temp": 111,
            "test_float": 0.21334215134123513,
            "time": -714208170,
        },
    ]

    assert records == roundtrip(schema, records)


def test_string_not_treated_as_array():
    """https://github.com/fastavro/fastavro/issues/166"""

    schema = {
        "type": "record",
        "fields": [
            {
                "name": "description",
                "type": ["null", {"type": "array", "items": "string"}, "string"],
            }
        ],
        "name": "description",
        "doc": "A description of the thing.",
    }

    records = [
        {
            "description": "value",
        },
        {"description": ["an", "array"]},
    ]

    assert records == roundtrip(schema, records)


def test_schema_is_custom_dict_type():
    """https://github.com/fastavro/fastavro/issues/168"""

    class CustomDict(dict):
        pass

    schema = {
        "type": "record",
        "fields": [
            {
                "name": "description",
                "type": ["null", {"type": "array", "items": "string"}, "string"],
            }
        ],
        "name": "description",
        "doc": "A description of the thing.",
    }
    other_type_schema = CustomDict(schema)

    records = [
        {
            "description": "value",
        }
    ]

    new_records = roundtrip_schema_migration(schema, other_type_schema, records)

    assert records == new_records


def test_long_bounds():
    schema = {
        "name": "test_long_bounds",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "time", "type": "long"},
        ],
    }

    records = [
        {"time": (1 << 63) - 1},
        {"time": -(1 << 63)},
    ]

    assert records == roundtrip(schema, records)


data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "testdata")


def test_py37_runtime_error():
    """On Python 3.7 this test would cause the StopIteration to get raised as
    a RuntimeError.

    See https://www.python.org/dev/peps/pep-0479/
    """
    weather_file = os.path.join(data_dir, "weather.avro")

    zip_io = io.BytesIO()
    with zipfile.ZipFile(zip_io, mode="w") as zio:
        zio.write(weather_file, arcname="weather")

    with zipfile.ZipFile(zip_io) as zio:
        with zio.open("weather") as fo:
            # Need to read fo into a bytes buffer for python versions less
            # than 3.7
            reader = avroc.files.AvroFileReader(io.BytesIO(fo.read()))
            list(reader)


# Currently fails: we raise struct.error, not EOFError.
@pytest.mark.xfail(strict=True)
def test_eof_error():
    schema = {
        "type": "record",
        "name": "test_eof_error",
        "fields": [
            {
                "name": "test",
                "type": "float",
            }
        ],
    }
    record = {"test": 1.234}

    encode = avroc.messages.message_encoder(schema)
    encoded = encode(record)

    # Back up one byte and truncate
    encoded = encoded[:-1]
    buf = io.BytesIO(encoded)
    with pytest.raises(EOFError):
        decode = avroc.messages.message_decoder(schema)
        decode(buf)


def test_passing_same_schema_to_reader():
    """https://github.com/fastavro/fastavro/issues/244"""
    schema = {
        "namespace": "test.avro.training",
        "name": "SomeMessage",
        "type": "record",
        "fields": [
            {
                "name": "is_error",
                "type": "boolean",
                "default": False,
            },
            {
                "name": "outcome",
                "type": [
                    "SomeMessage",
                    {
                        "type": "record",
                        "name": "ErrorRecord",
                        "fields": [
                            {
                                "name": "errors",
                                "type": {"type": "map", "values": "string"},
                                "doc": "doc",
                            }
                        ],
                    },
                ],
            },
        ],
    }

    records = [
        {
            "is_error": True,
            "outcome": {
                "errors": {"field_1": "some_message", "field_2": "some_other_message"}
            },
        }
    ]

    assert records == roundtrip_schema_migration(schema, schema, records)


def test_embedded_records_get_namespaced_correctly():
    schema = {
        "namespace": "test",
        "name": "OuterName",
        "type": "record",
        "fields": [
            {
                "name": "data",
                "type": [
                    {
                        "type": "record",
                        "name": "UUID",
                        "fields": [{"name": "uuid", "type": "string"}],
                    },
                    {
                        "type": "record",
                        "name": "Abstract",
                        "fields": [
                            {
                                "name": "uuid",
                                "type": "UUID",
                            }
                        ],
                    },
                    {
                        "type": "record",
                        "name": "Concrete",
                        "fields": [
                            {"name": "abstract", "type": "Abstract"},
                            {
                                "name": "custom",
                                "type": "string",
                            },
                        ],
                    },
                ],
            }
        ],
    }

    records = [
        {"data": {"abstract": {"uuid": {"uuid": "some_uuid"}}, "custom": "some_string"}}
    ]

    assert records == roundtrip(schema, records)


def test_null_defaults_are_not_used():
    """https://github.com/fastavro/fastavro/issues/272"""
    schema = [
        {
            "type": "record",
            "name": "A",
            "fields": [{"name": "foo", "type": ["string", "null"]}],
        },
        {
            "type": "record",
            "name": "B",
            "fields": [{"name": "bar", "type": ["string", "null"]}],
        },
        {
            "type": "record",
            "name": "AOrB",
            "fields": [{"name": "entity", "type": ["A", "B"]}],
        },
    ]

    datum_to_read = {"entity": {"foo": "this is an instance of schema A"}}

    assert [datum_to_read] == roundtrip(schema, [datum_to_read])


@pytest.mark.xfail
def test_union_schema_ignores_extra_fields():
    # Our writer is more strict, and will emit an error because it doesn't think
    # that the given message matches any of the cases in the union.
    """https://github.com/fastavro/fastavro/issues/274"""
    schema = [
        {"type": "record", "name": "A", "fields": [{"name": "name", "type": "string"}]},
        {
            "type": "record",
            "name": "B",
            "fields": [{"name": "other_name", "type": "string"}],
        },
    ]

    records = [{"name": "abc", "other": "asd"}]

    assert [{"name": "abc"}] == roundtrip(schema, records)


def test_appending_records(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = avroc.schema.load_schema({
        "type": "record",
        "name": "test_appending_records",
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
    })

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        writer = avroc.files.AvroFileWriter(new_file, schema)
        writer.write({"field": "foo"})
        writer.flush()

    with open(test_file, "a+b") as new_file:
        writer = avroc.files.AvroFileWriter(new_file, schema)
        writer.write({"field": "bar"})
        writer.flush()

    with open(test_file, "rb") as new_file:
        reader = avroc.files.AvroFileReader(new_file)
        new_records = list(reader)

    assert new_records == [{"field": "foo"}, {"field": "bar"}]


# Currently fails: we treat a union with only one value as an error.
@pytest.mark.xfail(strict=True)
def test_order_of_values_in_map():
    """https://github.com/fastavro/fastavro/issues/303"""
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {
                "name": "metadata",
                "type": {
                    "type": "map",
                    "values": [
                        {"type": "array", "items": "string"},
                        {"type": "map", "values": ["string"]},
                    ],
                },
            }
        ],
    }
    records = [{"metadata": {"map1": {"map2": "str"}}}]

    assert records == roundtrip(schema, records)


def test_writer_schema_always_read():
    """https://github.com/fastavro/fastavro/issues/312"""
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "item",
                "type": [
                    {
                        "type": "record",
                        "name": "Inner1",
                        "fields": [
                            {
                                "name": "id",
                                "type": {
                                    "type": "record",
                                    "name": "UUID",
                                    "fields": [{"name": "id", "type": "string"}],
                                },
                                "default": {"id": ""},
                            },
                            {"name": "description", "type": "string"},
                            {"name": "size", "type": "int"},
                        ],
                    },
                    {
                        "type": "record",
                        "name": "Inner2",
                        "fields": [
                            {"name": "id", "type": "UUID", "default": {"id": ""}},
                            {"name": "name", "type": "string"},
                            {"name": "age", "type": "long"},
                        ],
                    },
                ],
            }
        ],
    }

    records = [
        {"item": {"description": "test", "size": 1}},
        {"item": {"id": {"id": "#1"}, "name": "foobar", "age": 12}},
    ]

    file = io.BytesIO()

    w = avroc.files.AvroFileWriter(file, avroc.schema.load_schema(schema))
    for r in records:
        w.write(r)
    w.flush()
    file.seek(0)

    # This should not raise a KeyError
    r = avroc.files.AvroFileReader(file)
    list(r)
