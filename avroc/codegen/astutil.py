"""
astutil contains utility functions for generating an AST.
"""
from avroc.avro_common import SchemaType, PRIMITIVES
from typing import Union, List, Optional, Dict, Any
from ast import (
    Name,
    expr,
    stmt,
    AugAssign,
    Add,
    List as ListLiteral,
    Dict as DictLiteral,
    Load,
    Call,
    Constant,
    Mod,
    FloorDiv,
    BinOp,
    Mult,
    Store,
    Attribute,
    keyword,
)


def extend_buffer(buf: Name, extend_with: expr) -> stmt:
    """
    Generate a statement equivaluent to 'buf += extend_with'.
    """
    return AugAssign(
        target=Name(id=buf.id, ctx=Store()),
        op=Add(),
        value=extend_with,
    )


def call_decoder(primitive_type: str) -> expr:
    return Call(
        func=Name(id="decode_" + primitive_type, ctx=Load()),
        args=[Name(id="src", ctx=Load())],
        keywords=[],
    )


def call_encoder(primitive_type: str, msg: Union[expr, int]) -> expr:
    call = Call(
        func=Name(id="encode_" + primitive_type, ctx=Load()),
        keywords=[],
    )
    if isinstance(msg, expr):
        call.args = [msg]
    else:
        call.args = [Constant(value=msg)]
    return call


def func_call(name: str, args: List[expr]) -> Call:
    for idx, a in enumerate(args):
        if not isinstance(a, expr):
            args[idx] = Constant(value=a)

    return Call(
        func=Name(id=name, ctx=Load()),
        args=args,
        keywords=[],
    )


def method_call(
    chain: str, args: List[expr], kwargs: Optional[Dict[str, expr]] = None
) -> Call:
    parts = chain.split(".")
    attrib = Attribute(
        value=Name(id=parts[0], ctx=Load()),
        attr=parts[1],
        ctx=Load(),
    )
    if len(parts) > 2:
        for p in parts[2:]:
            attrib = Attribute(
                value=attrib,
                attr=p,
                ctx=Load(),
            )
    call = Call(func=attrib, args=args, keywords=[])
    if kwargs is not None:
        for name, value in kwargs.items():
            call.keywords.append(keyword(arg=name, value=value))
    return call


def literal_from_default(v: Any, schema: SchemaType) -> expr:
    """
    Schemas can contain default values. When generating code, we need to have a
    way to inject a literal which constructs that default. This function does
    that - it converts a Python value (which should be sourced from json-parsing
    an Avro schema's "default" field) and the schema describing the default
    value's type into a literal expression.
    """
    # Default for a primitive is just a literal constant
    if isinstance(schema, str):
        assert schema in PRIMITIVES
        if schema == "null":
            assert v is None
            return Constant(value=None)
        elif schema == "boolean":
            assert v is True or v is False
            return Constant(value=v)
        elif schema == "int" or schema == "long":
            assert isinstance(v, int) and not isinstance(v, bool)
            return Constant(value=int(v))
        elif schema == "float" or schema == "double":
            assert isinstance(v, float)
            return Constant(value=v)
        elif schema == "bytes":
            assert isinstance(v, str)
            return Constant(value=v.encode("utf8"))
        elif schema == "string":
            assert isinstance(v, str)
            return Constant(value=v)
        else:
            raise ValueError(f"unexpected schema type {schema}")
    elif isinstance(schema, list):
        # Default for a union has the schema of the first type in the union, but
        # lots of code violates this, so fall back to accepting later options.
        for option in schema:
            try:
                return literal_from_default(v, option)
            except:
                pass
        raise ValueError("no matching schema for default value")
    # Default for a complex type is... complex
    else:
        assert isinstance(schema, dict)
        if schema["type"] in PRIMITIVES:
            return literal_from_default(v, schema["type"])
        if schema["type"] == "enum":
            # Default for an enum is a string for one of the symbols
            assert isinstance(v, str)
            assert v in schema["symbols"]
            return Constant(value=v)
        if schema["type"] in {"record", "error"}:
            # Default for a record is a dictionary; keys are field names and
            # values are according to the schema of the field.
            assert isinstance(v, dict)
            record_literal = DictLiteral(keys=[], values=[])
            for field in schema["fields"]:
                default_field_value = v[field["name"]]
                record_literal.keys.append(Constant(value=field["name"]))
                record_literal.values.append(
                    literal_from_default(default_field_value, field["type"])
                )
            return record_literal
        if schema["type"] == "array":
            # Default for an array is a list; values are according to the items
            # schema for the array.
            assert isinstance(v, list)
            array_literal = ListLiteral(elts=[], ctx=Load())
            for array_item in v:
                array_literal.elts.append(
                    literal_from_default(array_item, schema["items"])
                )
            return array_literal
        if schema["type"] == "map":
            # Default for a map is a dictionary; values are according to the values
            # schema for the map.
            assert isinstance(v, dict)
            map_literal = DictLiteral(keys=[], values=[])
            for key, val in v.items():
                map_literal.keys.append(Constant(value=key))
                map_literal.values.append(literal_from_default(val, schema["values"]))
            return map_literal
        if schema["type"] == "fixed":
            # Default for a fixed is a string
            assert isinstance(v, str)
            return Constant(value=v.encode("utf8"))
    raise NotImplementedError(
        f"unable to generate literal from default; missing implementation for {schema}"
    )


def floor_div(dividend: expr, divisor: int) -> expr:
    return BinOp(
        op=FloorDiv(),
        left=dividend,
        right=Constant(value=divisor),
    )


def mod(quantity: expr, modulo: int) -> expr:
    return BinOp(
        op=Mod(),
        left=quantity,
        right=Constant(value=modulo),
    )


def mult(quantity: expr, multiple: int) -> expr:
    return BinOp(
        op=Mult(),
        left=quantity,
        right=Constant(value=multiple),
    )


def add(quantity: expr, add: int) -> expr:
    return BinOp(op=Add(), left=quantity, right=Constant(value=add))


def utc() -> Attribute:
    return Attribute(
        value=Attribute(
            value=Name(id="datetime", ctx=Load()),
            attr="timezone",
            ctx=Load(),
        ),
        attr="utc",
        ctx=Load(),
    )
