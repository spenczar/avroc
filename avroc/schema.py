from typing import Dict

from avroc.avro_common import PRIMITIVES
from avroc.util import SchemaType


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
        schema["items"] = expand_names(schema["items"], enclosing_namespace)
        return schema
    if schema["type"] == "map":
        schema["values"] = expand_names(schema["values"], enclosing_namespace)
        return schema
    if schema["type"] in PRIMITIVES:
        return schema
    if schema["type"] not in {"enum", "fixed", "record", "error"}:
        # Named type reference.
        return expand_names(schema["type"], enclosing_namespace)

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
