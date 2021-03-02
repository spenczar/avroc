from typing import List

from fastavro._schema_common import PRIMITIVES
from avroc.util import clean_name, SchemaType
from avroc.codegen.compiler import Compiler
from avroc.codegen.astutil import extend_buffer, call_encoder

INT_MAX_VALUE = (1 << 31) - 1
INT_MIN_VALUE = -INT_MAX_VALUE
LONG_MAX_VALUE = (1 << 63) - 1
LONG_MIN_VALUE = -LONG_MAX_VALUE

from ast import (
    Add,
    And,
    AST,
    Assign,
    Attribute,
    AugAssign,
    BoolOp,
    Call,
    Compare,
    Constant,
    Dict as DictLiteral,
    Eq,
    Expr,
    For,
    FunctionDef,
    If,
    IfExp,
    Import,
    ImportFrom,
    Index,
    List as ListLiteral,
    Load,
    Lt,
    LtE,
    Module,
    Name,
    Not,
    NotEq,
    Return,
    Store,
    Subscript,
    Tuple,
    USub,
    UnaryOp,
    While,
    alias,
    arg,
    arguments,
    expr,
    fix_missing_locations,
    keyword,
    stmt,
)

class WriterCompiler(Compiler):
    def __init__(self, schema: SchemaType):
        super(WriterCompiler, self).__init__(schema, "writer")

    def generate_module(self) -> Module:
        body: List[stmt] = []

        body.append(Import(names=[alias(name="numbers")]))
        # Add import statements of low-level writer functions
        import_from_encoding = []
        for primitive_type in PRIMITIVES:
            name = "encode_" + primitive_type
            import_from_encoding.append(alias(name=name))

        body.append(
            ImportFrom(
                module="avroc.runtime.encoding",
                names=import_from_encoding,
                level=0,
            )
        )

        body.append(self.generate_encoder_func(self.schema, self.entrypoint_name))
        for recursive_type in self.recursive_types:
            body.append(
                self.generate_encoder_func(
                    name=self._named_type_encoder_name(recursive_type["name"]),
                    schema=recursive_type,
                )
            )

        module = Module(
            body=body,
            type_ignores=[],
        )
        module = fix_missing_locations(module)
        return module

    @staticmethod
    def _named_type_reader_name(name: str) -> str:
        return "_write_" + clean_name(name)

    def generate_encoder_func(self, schema: SchemaType, name: str) -> FunctionDef:
        msg_var = Name(id="msg", ctx=Load())
        func = FunctionDef(
            name=name,
            args=arguments(
                args=[arg(arg="msg")],
                posonlyargs=[],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=[],
            decorator_list=[],
        )
        # Create an empty byte buffer for all data
        buf_var = Name(id="buf", ctx=Load())
        func.body.append(
            Assign(
                targets=[Name(id="buf", ctx=Store())],
                value=Call(
                    func=Name(id="bytes", ctx=Load()),
                    args=[],
                    keywords=[],
                ),
            )
        )
        func.body.extend(self._gen_encoder(schema, buf_var, msg_var))
        func.body.append(Return(value=buf_var))
        return func

    def _gen_encoder(self, schema: SchemaType, buf: Name, msg: Name) -> List[stmt]:
        if isinstance(schema, str):
            if schema in PRIMITIVES:
                return self._gen_primitive_encoder(
                    primitive_type=schema,
                    buf=buf,
                    msg=msg,
                )

        if isinstance(schema, list):
            return self._gen_union_encoder(options=schema, buf=buf, msg=msg)

        raise NotImplementedError(f"Schema type not implemented: {schema}")

    def _gen_primitive_encoder(
        self, primitive_type: str, buf: Name, msg: Name
    ) -> List[stmt]:
        if primitive_type == "null":
            return []
        encoder_func_name = "encode_" + primitive_type
        value = call_encoder(primitive_type, msg)
        write = extend_buffer(buf, value)
        return [write]

    def _gen_union_encoder(
        self, options: List[SchemaType], buf: Name, msg: Name
    ) -> List[stmt]:
        statements: List[stmt] = []

        idx = 0
        case = options[0]
        prev_if = None

        def call_isinstance(args):
            return Call(
                func=Name("isinstance", ctx=Load()),
                args=[msg, args],
                keywords=[],
            )

        for idx, option_schema in enumerate(options):
            # For each option, generate a statement of the general form:
            # if is_<datatype>(msg):
            #    buf += write_long(idx)
            #    buf += write_<datatype>(msg)
            if_stmt = If(
                test=self._gen_union_type_test(option_schema, msg),
                body=[Expr(call_encoder("long", idx))],
                orelse=[],
            )
            if_stmt.body.extend(self._gen_encoder(option_schema, buf, msg))
            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt

        return statements

    def _gen_union_type_test(self, schema: SchemaType, msg: Name) -> expr:
        def call_isinstance(args):
            return Call(
                func=Name("isinstance", ctx=Load()),
                args=[msg, args],
                keywords=[],
            )

        if isinstance(schema, str):
            if schema == "null":
                # if msg is None:
                return Compare(left=msg, ops=[Eq()], comparators=[Constant(None)])
            elif schema == "boolean":
                # if isinstance(msg, bool):
                return call_isinstance(Name(id="bool", ctx=Load()))
            elif schema == "string":
                # if isinstance(msg, str):
                return call_isinstance(Name(id="str", ctx=Load()))
            elif schema == "bytes":
                # if isinstance(msg, (bytes, bytearray)):
                return call_isinstance(
                    Tuple(
                        elts=[
                            Name(id="bytes", ctx=Load()),
                            Name(id="bytearray", ctx=Load()),
                        ],
                        ctx=Load(),
                    )
                )
            elif schema == "int":
                # if (isinstance(msg, (int, numbers.Integral))
                #     and INT_MIN_VALUE <= msg <= INT_MAX_VALUE
                #     and not isinstance(msg, bool)):
                integral_type = Attribute(
                    value=Name(id="numbers", ctx=Load()),
                    attr="Integral",
                    ctx=Load(),
                )

                is_int_type = call_isinstance(
                    Tuple(
                        elts=[
                            Name(id="int", ctx=Load()),
                            integral_type,
                        ],
                        ctx=Load(),
                    ),
                )
                is_in_range = Compare(
                    left=Constant(INT_MIN_VALUE),
                    ops=[LtE(), LtE()],
                    comparators=[msg, Constant(INT_MAX_VALUE)],
                )
                is_not_bool = UnaryOp(
                    op=Not(),
                    operand=call_isinstance(Name(id="bool", ctx=Load())),
                )
                return BoolOp(op=And(), values=[is_int_type, is_in_range, is_not_bool])
            elif schema == "long":
                # if (isinstance(datum, (int, numbers.Integral))
                #  and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
                #  and not isinstance(datum, bool))
                integral_type = Attribute(
                    value=Name(id="numbers", ctx=Load()),
                    attr="Integral",
                    ctx=Load(),
                )

                is_int_type = call_isinstance(
                    Tuple(
                        elts=[
                            Name(id="int", ctx=Load()),
                            integral_type,
                        ],
                        ctx=Load(),
                    ),
                )
                is_in_range = Compare(
                    left=Constant(LONG_MIN_VALUE),
                    ops=[LtE(), LtE()],
                    comparators=[msg, Constant(LONG_MAX_VALUE)],
                )
                is_not_bool = UnaryOp(
                    op=Not(),
                    operand=call_isinstance(Name(id="bool", ctx=Load())),
                )
                return BoolOp(op=And(), values=[is_int_type, is_in_range, is_not_bool])
            elif schema == "float":
                # if (isinstance(datum, (int, float, numbers.Real))
                #     and not isinstance(datum, bool)):
                real_type = Attribute(
                    value=Name(id="numbers", ctx=Load()), attr="Real", ctx=Load()
                )
                is_float_type = call_isinstance(
                    Tuple(
                        elts=[
                            Name(id="int", ctx=Load()),
                            Name(id="float", ctx=Load()),
                            real_type,
                        ],
                        ctx=Load(),
                    ),
                )
                is_not_bool = UnaryOp(
                    op=Not(),
                    operand=call_isinstance(Name(id="bool", ctx=Load())),
                )
                returnBoolOp(op=And(), values=[is_float_type, is_not_bool])

        else:
            # Union-of-union is explicitly forbidden by the Avro spec, so all
            # thats left is dict types.
            assert isinstance(option_schema, dict)
        raise NotImplementedError(f"have not implemented union check for type {schema}")