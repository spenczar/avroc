"""
astutil contains utility functions for generating an AST.
"""
from typing import Union, List, Optional, Dict
from ast import Name, expr, stmt, AugAssign, Add, Load, Call, Constant, Mod, FloorDiv, BinOp, Mult, Store, Attribute, Add, keyword

def extend_buffer(buf: Name, extend_with: expr) -> stmt:
    """
    Generate a statement equivaluent to 'buf += extend_with'.
    """
    return AugAssign(
        target=Name(id=buf.id, ctx=Store()),
        op=Add(),
        value=extend_with,
    )

def call_decoder(primitive_type: str, src: Name) -> expr:
    return Call(
        func=Name(id="decode_" + primitive_type, ctx=Load()),
        args=[Name(id=src.id, ctx=Load())],
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

def method_call(chain: str, args: List[expr], kwargs: Optional[Dict[str, expr]]=None) -> Call:
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
