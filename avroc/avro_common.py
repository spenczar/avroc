from avroc.util import SchemaType

PRIMITIVES = {"int", "long", "float", "double", "bytes", "null", "string", "boolean"}
COMPLEXES = {"enum", "fixed", "array", "map", "record", "union", "error"}
AVRO_TYPES = PRIMITIVES.union(COMPLEXES)


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
    return schema
