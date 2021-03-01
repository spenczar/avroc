from typing import Dict, Union, List, Any
import random
import re


# SchemaType is a type which describes Avro schemas.
SchemaType = Union[
    str,  # Primitives
    List[Any],  # Unions
    Dict[str, Any],  # Complex types
]


def rand_str(length: int) -> str:
    """Generate a random string of given length."""
    alphabet = "0123456789abcdef"
    return "".join(random.choices(alphabet, k=length))


def clean_name(name: str) -> str:
    """
    Clean a name so it can be used as a python identifier.
    """
    if not re.match("[a-zA-Z_]", name[0]):
        name = "_" + name
    name = re.sub("[^0-9a-zA-Z_]+", "_", name)
    if all(c == "_" for c in name):
        name = "v"
    return name


class LogicalTypeError(Exception):
    pass
