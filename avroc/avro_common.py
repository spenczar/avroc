from avroc.util import SchemaType

PRIMITIVES = {"int", "long", "float", "double", "bytes", "null", "string", "boolean"}


def is_primitive_schema(schema: SchemaType) -> bool:
    if isinstance(schema, str) and schema in PRIMITIVES:
        return True
    if isinstance(schema, dict) and schema["type"] in PRIMITIVES:
        return True
    return False


def schema_type(schema: SchemaType) -> str:
    if isinstance(schema, list):
        return "union"
    if isinstance(schema, dict):
        return schema["type"]
    if schema in PRIMITIVES:
        return schema
    raise ValueError("unknown schema type")
