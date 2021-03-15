from typing import Dict, List, Set
import re
import warnings
from avroc.avro_common import PRIMITIVES
from avroc.util import SchemaType
from avroc.runtime import typetest


def validate(schema: SchemaType):
    _validate_internal(schema, [], set())


def _validate_internal(schema: SchemaType, path: List[str], names: Set[str]) -> bool:
    if isinstance(schema, str):
        if schema in PRIMITIVES:
            return True
        else:
            # Named type reference.
            if schema not in names:
                raise SchemaValidationError(schema, path, f"'{schema}' is not defined")
            return True

    if isinstance(schema, list):
        if len(schema) < 2:
            raise SchemaValidationError(
                schema, path, "unions must have at least two elements"
            )
        for idx, subschema in enumerate(schema):
            path.append(str(idx))
            _validate_internal(subschema, path, names)
            path.pop()
        return True

    if isinstance(schema, dict):
        if "type" not in schema:
            raise SchemaValidationError(schema, path, "missing required field 'type'")
        if not isinstance(schema["type"], str):
            raise SchemaValidationError(
                schema,
                path,
                "schema['type'] must have a string value (this is {type(schema['type'])})",
            )
        if "logicalType" in schema:
            return _validate_logical(schema, path, names)
        if schema["type"] in PRIMITIVES:
            return _validate_internal(schema["type"], path, names)
        if schema["type"] in names:
            return True
        if schema["type"] == "record":
            return _validate_record(schema, path, names)
        if schema["type"] == "map":
            return _validate_map(schema, path, names)
        if schema["type"] == "array":
            return _validate_array(schema, path, names)
        if schema["type"] == "fixed":
            return _validate_fixed(schema, path, names)
        if schema["type"] == "enum":
            return _validate_enum(schema, path, names)
        raise SchemaValidationError(schema, path, f"'{schema['type']}' is not defined")

    raise SchemaValidationError(
        schema,
        path,
        f"schema types should only be str, list, or dict; this is a {type(schema)}",
    )


def _validate_record(schema: Dict, path: List[str], names: Set[str]) -> bool:
    if not _validate_named(schema, path, names):
        return False

    for k in schema.keys():
        if k not in {"name", "namespace", "doc", "aliases", "fields", "type"}:
            warnings.warn(
                SchemaValidationWarning(schema, path, f"ignoring field '{k}' on record")
            )
    if "fields" not in schema:
        raise SchemaValidationError(schema, path, "records must have fields")

    if not isinstance(schema["fields"], list):
        raise SchemaValidationError(schema, path, "record fields should be a list")
    for field in schema["fields"]:
        if "name" not in field:
            raise SchemaValidationError(schema, path, "record fields must be named")
        name = field["name"]
        path.append(name)
        if not isinstance(name, str):
            raise SchemaValidationError(schema, path, "record fields must be strings")
        if not _valid_name(name):
            raise SchemaValidationError(
                schema, path, "record fields is invalid (must be alphanumeric)"
            )
        _validate_internal(field["type"], path, names)
        if "default" in field:
            _validate_field_default(field, path, names)
        path.pop()


def _validate_field_default(field: Dict, path: List[str], names: Mapping[str, Dict]) -> bool:
    field_type = field["type"]
    default = field["default"]
    primitive_typetesters = {
        "null": typetest.is_null,
        "boolean": typetest.is_boolean,
        "int": typetest.is_int,
        "long": typetest.is_long,
        "float": typetest.is_float,
        "double": typetest.is_float,
        "string": typetest.is_string,
        "bytes": typetest.is_string,
    }
    if isinstance(field_type, str):
        if field_type in PRIMITIVES:
            if not typetesters[field_type](default):
                raise SchemaValidationError(
                    field["type"],
                    path,
                    "field default's type doesn't match the field's type",
                )
        else:
            pass
    if isinstance(field_type, list):
        # Union
        pass
    if isinstance(field_type, dict):
        if field["type"]["type"] == "fixed":
            if not typetest.is_string(default):
                raise SchemaValidationError(
                    field_type["type"],
                    path,
                    "field default's type doesn't match the field's type",
                )
            if len(default.encode()) != field["size"]:
                raise SchemaValidationError(
                    field["type"], path, "field default is the wrong encoded size"
                )
            return True


def _validate_named(schema: Dict, path: List[str], names: Set[str]) -> bool:
    if "name" not in schema:
        raise SchemaValidationError(schema, path, "name field is required")

    if not isinstance(schema["name"], str):
        raise SchemaValidationError(schema, path, "name field must be a string")

    for part in schema["name"].split("."):
        if not _valid_name(part):
            raise SchemaValidationError(
                schema, path, "invalid name (must be alphanumeric)"
            )

        if part in PRIMITIVES:
            raise SchemaValidationError(
                schema, path, "primitive type names cannot be used as parts of names"
            )

    if not isinstance(schema.get("namespace", ""), str):
        raise SchemaValidationError(schema, path, "namespace field must be a string")

    if schema["name"] in names:
        raise SchemaValidationError(schema, path, "names cannot be defined twice")

    if "aliases" in schema:
        aliases = schema["aliases"]
        if not isinstance(aliases, list):
            raise SchemaValidationError(
                schema, path, "aliases must be a list of strings"
            )

        for a in schema["aliases"]:
            if not isinstance(a, str):
                raise SchemaValidationError(
                    schema, path, "aliases must be a list of strings"
                )

            for part in a.split("."):
                if not _valid_name(part):
                    raise SchemaValidationError(
                        schema, path, "invalid alias name (must be alphanumeric)"
                    )

    names.add(schema["name"])
    return True


def _validate_enum(schema: Dict, path: List[str], names: Set[str]) -> bool:
    if not _validate_named(schema, path, names):
        return False
    raise NotImplementedError("enums not validated")


def _validate_array(schema: Dict, path: List[str], names: Set[str]) -> bool:
    raise NotImplementedError("arrays not validated")


def _validate_map(schema: Dict, path: List[str], names: Set[str]) -> bool:
    raise NotImplementedError("maps not validated")


def _validate_fixed(schema: Dict, path: List[str], names: Set[str]) -> bool:
    if not _validate_named(schema, path, names):
        return False
    raise NotImplementedError("fixeds not validated")


def _valid_name(name: str) -> bool:
    return re.match("[A-Za-z_][A-Za-z0-9_]*", name) is not None


def _validate_schema_fields(schema: Dict, path: List[str], required: Dict[str, type], optional: Dict[str, type]) -> bool:
    """
    Check that a schema has all its required fields, and check that they have
    the right typed values. Check that optional fields have the right typed
    values. Warn for any other fields.
    """
    for field_name, field_type in required.items():
        if field_name not in schema:
            raise SchemaValidationError(schema, path, f'missing required field "{field_name}"')
        if not isinstance(schema[field_name], field_type):
            have = type(schema[field_name])
            msg = f'field "{field_name}" has the wrong type (have {have}, want {field_type})'
            raise SchemaValidationError(schema, path, msg)

    for field_name, field_type in optional.items():
        if field_name in schema:
            if not isinstance(schema[field_name], field_type):
                have = type(schema[field_name])
                msg = f'field "{field_name}" has the wrong type (have {have}, want {field_type})'
                raise SchemaValidationError(schema, path, msg)

    expected = set(required.keys()).union(set(optional.keys()))
    for field_name in schema.keys():
        if field_name not in expected:
            msg = f'field "{field_name}" is not expected and will be ignored'
            warnings.Warn(SchemaValidationWarning(schema, path, msg))


class SchemaValidationError(Exception):
    def __init__(self, schema, path, msg, *args, **kwargs):
        self.schema = schema
        self.path = path

        if len(self.path) > 0:
            msg = f"schema invalid at {'.'.join(self.path)}: {msg}"
        else:
            msg = f"schema invalid: {msg}"
        self.msg = msg
        super(SchemaValidationError, self).__init__(msg)


class SchemaValidationWarning(Warning):
    def __init__(self, schema, path, msg, *args, **kwargs):
        self.schema = schema
        self.path = path

        if len(self.path) > 0:
            msg = f"at {'.'.join(self.path)}: {msg}"
        self.msg = msg
        super(SchemaValidationWarning, self).__init__(msg)


def expand_names(schema: SchemaType, enclosing_namespace: str = "") -> SchemaType:
    """
    Convert all names, aliases, and named type references into fully-qualified
    names.
    """

    if isinstance(schema, str):
        if schema in PRIMITIVES:
            # Can't be named.
            return schema
        else:
            # Named type reference.
            if "." in schema:
                # Already fully-qualified.
                return schema
            else:
                if enclosing_namespace != "":
                    return enclosing_namespace + "." + schema
                return schema

    if isinstance(schema, list):
        # Union.
        return [expand_names(option, enclosing_namespace) for option in schema]

    assert isinstance(schema, dict)
    schema = schema.copy()

    # Non-named types.
    if schema["type"] == "array":
        schema["items"] = expand_names(schema["items"])
        return schema
    if schema["type"] == "map":
        schema["values"] = expand_names(schema["values"])
        return schema
    if schema["type"] in PRIMITIVES:
        return schema
    if schema["type"] not in {"enum", "fixed", "record", "error"}:
        # Named type reference.
        return expand_names(schema["type"])

    # Named types (enum, fixed, and record)
    local_namespace = namespace(schema, enclosing_namespace)
    schema["name"] = fullname(schema, enclosing_namespace)
    if "aliases" in schema:
        expanded_aliases = []
        for alias in schema["aliases"]:
            if "." in alias:
                # Already expanded.
                expanded_alias = alias
            elif local_namespace != "":
                # Expand relative to the local namespace.
                expanded_alias = f"{local_namespace}.{alias}"
            else:
                # Alias is relative to the null namespace, so leave it
                # unchanged.
                expanded_alias = alias
            expanded_aliases.append(expanded_alias)
        schema["aliases"] = expanded_aliases

    # Enum and fixed are named, but have no children, so we're done.
    if schema["type"] == "enum" or schema["type"] == "fixed":
        return schema

    # Record types set the namespace for any definitions they contain.
    assert schema["type"] in {"record", "error"}
    expanded_fields = []
    for field in schema["fields"]:
        field = field.copy()
        field["type"] = expand_names(field["type"], local_namespace)
        expanded_fields.append(field)

    schema["fields"] = expanded_fields
    return schema


def gather_named_types(schema: SchemaType) -> Dict[str, SchemaType]:
    """
    Traverse the schema to find the definitions of all named types. Returns a
    dictionary, mapping fully-qualified type names to their definitions.
    """
    if isinstance(schema, str):
        return {}

    if isinstance(schema, list):
        result = {}
        for option in schema:
            result.update(gather_named_types(option))
        return result

    if isinstance(schema, dict):
        if schema["type"] == "array":
            return gather_named_types(schema["items"])
        if schema["type"] == "map":
            return gather_named_types(schema["values"])
        result = {}
        if "name" in schema:
            result[schema["name"]] = schema
        if "aliases" in schema:
            for alias in schema["aliases"]:
                result[alias] = schema

        if schema["type"] == "record":
            for field in schema["fields"]:
                result.update(gather_named_types(field["type"]))
        return result


def fullname(schema: SchemaType, enclosing_namespace: str) -> str:
    name = schema["name"]
    if "." in name:
        return name
    ns = namespace(schema, enclosing_namespace)
    return name if ns == "" else f"{ns}.{name}"


def namespace(schema: dict, enclosing_namespace: str) -> str:
    if "." in schema["name"]:
        namespace_part, _ = schema["name"].rsplit(".", 1)
        return namespace_part
    if "namespace" in schema:
        return schema["namespace"]
    return enclosing_namespace
