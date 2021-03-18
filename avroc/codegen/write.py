from typing import List, Dict, Optional, Any

from fastavro._schema_common import PRIMITIVES
from avroc.schema import *
from avroc.util import clean_name, LogicalTypeError
from avroc.codegen.compiler import Compiler
from avroc.codegen.astutil import (
    add,
    call_encoder,
    extend_buffer,
    floor_div,
    func_call,
    literal_from_default,
    method_call,
    mult,
)

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
    BinOp,
    BoolOp,
    Call,
    Compare,
    Constant,
    Dict as DictLiteral,
    Eq,
    Expr,
    For,
    FunctionDef,
    Gt,
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
    Mult,
    Name,
    Not,
    NotEq,
    Pow,
    Raise,
    Return,
    Set as SetLiteral,
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
    def __init__(self, schema: Schema):
        super(WriterCompiler, self).__init__(schema, "writer")

    def generate_module(self) -> Module:
        body: List[stmt] = []

        body.append(Import(names=[alias(name="numbers")]))
        # Add import statements of low-level writer functions
        body.append(
            ImportFrom(
                module="avroc.runtime.encoding",
                names=[alias(name="*")],
                level=0,
            )
        )
        body.append(
            ImportFrom(
                module="avroc.runtime.typetest",
                names=[alias(name="*")],
                level=0,
            )
        )

        body.append(self.generate_encoder_func(self.schema, self.entrypoint_name))
        for recursive_type in self.recursive_types:
            body.append(
                self.generate_encoder_func(
                    name=self._encoder_name(recursive_type),
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
    def _encoder_name(schema: NamedSchema) -> str:
        return "_write_" + clean_name(schema.fullname())

    def generate_encoder_func(self, schema: Schema, name: str) -> FunctionDef:
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

    def _gen_encoder(self, schema: Schema, buf: Name, msg: expr) -> List[stmt]:
        if isinstance(schema, LogicalSchema):
            return self._gen_logical_encoder(schema, buf, msg)

        elif isinstance(schema, PrimitiveSchema):
            return self._gen_primitive_encoder(schema.type, buf=buf, msg=msg)

        elif isinstance(schema, NamedSchemaReference):
            # Named type reference. Could be recursion?
            if schema.referenced_schema in self.recursive_types:
                # Yep, recursion. Just generate a function call - we'll have
                # a separate function to handle this type.
                return self._gen_recursive_encode_call(schema, buf, msg)
            else:
                # Not recursion. We can inline the encode.
                return self._gen_encoder(schema.referenced_schema, buf, msg)

        elif isinstance(schema, UnionSchema):
            return self._gen_union_encoder(schema, buf=buf, msg=msg)

        elif isinstance(schema, RecordSchema):
            return self._gen_record_encoder(schema, buf, msg)

        elif isinstance(schema, ArraySchema):
            return self._gen_array_encoder(schema, buf, msg)
        elif isinstance(schema, MapSchema):
            return self._gen_map_encoder(schema, buf, msg)
        elif isinstance(schema, FixedSchema):
            return self._gen_fixed_encoder(schema.size, buf, msg)
        elif isinstance(schema, EnumSchema):
            return self._gen_enum_encoder(schema, buf, msg)

        raise NotImplementedError(f"Schema type not implemented: {schema}")

    def _gen_primitive_encoder(
        self, primitive_type: str, buf: Name, msg: expr
    ) -> List[stmt]:
        if primitive_type == "null":
            return []
        encoder_func_name = "encode_" + primitive_type
        value = call_encoder(primitive_type, msg)
        write = extend_buffer(buf, value)
        return [write]

    def _gen_record_encoder(
        self, schema: RecordSchema, buf: Name, msg: expr
    ) -> List[stmt]:
        # A record is encoded as a concatenation of its fields.
        statements: List[stmt] = []

        for field in schema.fields:
            field_value: expr
            if field.default is not None:
                # Explicit default: generate code like
                #   msg.get(field["name"], field["default"])

                # We need to know the schema of the field in order to know how
                # to construct a literal value for the field default.
                field_schema = field.type
                if isinstance(field_schema, NamedSchemaReference):
                    field_schema = field_schema.referenced_schema

                field_value = Call(
                    func=Attribute(
                        value=msg,
                        attr="get",
                        ctx=Load(),
                    ),
                    args=[
                        Constant(value=field.name),
                        literal_from_default(field.default, field_schema),
                    ],
                    keywords=[],
                )
            elif isinstance(field.type, UnionSchema) and field.type.is_nullable():
                # Nullable union: generate code like
                #   msg.get(field["name"])
                field_value = Call(
                    func=Attribute(
                        value=msg,
                        attr="get",
                        ctx=Load(),
                    ),
                    args=[
                        Constant(value=field.name),
                    ],
                    keywords=[],
                )
            else:
                # No default: generate code like
                #   msg[field["name"]]
                field_value = Subscript(
                    value=msg,
                    slice=Index(value=Constant(value=field.name)),
                    ctx=Load(),
                )
            statements.extend(self._gen_encoder(field.type, buf, field_value))

        return statements

    def _gen_array_encoder(
        self, schema: ArraySchema, buf: Name, msg: expr
    ) -> List[stmt]:
        statements: List[stmt] = []
        # An array is encoded as a series of blocks. Each block has a long
        # count, followed by that many array items. A block with zero count
        # indicates the end of the array.
        n_items = Call(func=Name(id="len", ctx=Load()), args=[msg], keywords=[])
        # if len(msg) > 0:
        if_stmt = If(
            test=Compare(
                left=n_items,
                ops=[Gt()],
                comparators=[Constant(value=0)],
            ),
            body=[],
            orelse=[],
        )
        #    buf += encode_long(len(msg))
        if_stmt.body.append(extend_buffer(buf, call_encoder("long", n_items)))
        #    for item in msg:
        #        buf += encode_<type>(item)
        item_varname = self.new_variable("item")
        write_loop = For(
            target=Name(id=item_varname, ctx=Store()),
            iter=msg,
            body=self._gen_encoder(
                schema.items, buf, Name(id=item_varname, ctx=Load())
            ),
            orelse=[],
        )
        if_stmt.body.append(write_loop)
        statements.append(if_stmt)
        # buf += encode_long(0)
        statements.append(extend_buffer(buf, call_encoder("long", 0)))
        return statements

    def _gen_map_encoder(self, schema: MapSchema, buf: Name, msg: expr) -> List[stmt]:
        statements: List[stmt] = []
        # A map is encoded as a series of blocks. Each block has a long count,
        # followed by that many map key-value pairs. A block with zero count
        # indicates the end of the map.
        n_items = Call(func=Name(id="len", ctx=Load()), args=[msg], keywords=[])
        # if len(msg) > 0:
        if_stmt = If(
            test=Compare(
                left=n_items,
                ops=[Gt()],
                comparators=[Constant(value=0)],
            ),
            body=[],
            orelse=[],
        )
        #    buf += encode_long(len(msg))
        if_stmt.body.append(extend_buffer(buf, call_encoder("long", n_items)))
        #    for key, val in msg.items():
        #        buf += encode_string(key)
        #        buf += encode_<item>(val)
        key_varname = self.new_variable("key")
        val_varname = self.new_variable("val")
        items_call = Call(
            func=Attribute(value=msg, attr="items", ctx=Load()),
            args=[],
            keywords=[],
        )
        write_loop = For(
            target=Tuple(
                elts=[
                    Name(id=key_varname, ctx=Store()),
                    Name(id=val_varname, ctx=Store()),
                ],
                ctx=Store(),
            ),
            iter=items_call,
            body=[],
            orelse=[],
        )
        write_loop.body.extend(
            self._gen_primitive_encoder(
                "string",
                buf,
                Name(id=key_varname, ctx=Load()),
            )
        )
        write_loop.body.extend(
            self._gen_encoder(
                schema.values,
                buf,
                Name(id=val_varname, ctx=Load()),
            )
        )
        if_stmt.body.append(write_loop)
        statements.append(if_stmt)
        # buf += encode_long(0)
        statements.append(extend_buffer(buf, call_encoder("long", 0)))
        return statements

    def _gen_fixed_encoder(self, size: int, buf: Name, msg: expr) -> List[stmt]:
        return [extend_buffer(buf, msg)]

    def _gen_enum_encoder(self, schema: EnumSchema, buf: Name, msg: expr) -> List[stmt]:
        # Construct a literal dictionary which maps symbols to integers.
        enum_map = DictLiteral(keys=[], values=[])
        for i, sym in enumerate(schema.symbols):
            enum_map.keys.append(Constant(value=sym))
            enum_map.values.append(Constant(value=i))

        # buf += encode_long(dict.get(msg, default=default))
        dict_lookup = Call(
            func=Attribute(
                value=enum_map,
                attr="get",
                ctx=Load(),
            ),
            args=[msg],
            keywords=[],
        )
        if schema.default is not None:
            dict_lookup.args.append(Constant(value=schema.default))
        long_encode = call_encoder("long", dict_lookup)
        return [extend_buffer(buf, long_encode)]

    def _gen_union_encoder(
        self, schema: UnionSchema, buf: Name, msg: expr
    ) -> List[stmt]:
        statements: List[stmt] = []

        idx = 0
        case = schema.options[0]
        prev_if = None

        for idx, option_schema in enumerate(schema.options):
            # For each option, generate a statement of the general form:
            # if is_<datatype>(msg):
            #    buf += write_long(idx)
            #    buf += write_<datatype>(msg)
            if_stmt = If(
                test=self._gen_union_type_test(option_schema, msg),
                body=[extend_buffer(buf, call_encoder("long", idx))],
                orelse=[],
            )
            if_stmt.body.extend(self._gen_encoder(option_schema, buf, msg))
            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt

        assert prev_if is not None
        # In the final else, raise an error.
        prev_if.orelse = [
            Raise(
                exc=Call(
                    func=Name(id="ValueError", ctx=Load()),
                    args=[
                        Constant(
                            value="message type doesn't match any options in the union"
                        )
                    ],
                    keywords=[],
                ),
                cause=None,
            )
        ]

        return statements

    def _gen_union_type_test(self, schema: Schema, msg: expr) -> expr:
        # Union-of-union is explicitly forbidden by the Avro spec
        assert not isinstance(
            schema, UnionSchema
        ), "Union-of-union is forbidden by Avro spec"

        if isinstance(schema, LogicalSchema):
            if isinstance(schema, (DecimalBytesSchema, DecimalFixedSchema)):
                return func_call("is_decimal", [msg])
            elif isinstance(schema, UUIDSchema):
                return func_call("is_uuid", [msg])
            elif isinstance(schema, DateSchema):
                return func_call("is_date", [msg])
            elif isinstance(schema, (TimeMillisSchema, TimeMicrosSchema)):
                return func_call("is_time", [msg])
            elif isinstance(schema, (TimestampMillisSchema, TimestampMicrosSchema)):
                return func_call("is_timestamp", [msg])

        elif isinstance(schema, PrimitiveSchema):
            return func_call(f"is_{schema.type}", [msg])

        elif isinstance(schema, ArraySchema):
            return func_call("is_array", [msg])

        elif isinstance(schema, NamedSchemaReference):
            return self._gen_union_type_test(schema.referenced_schema, msg)

        elif isinstance(schema, FixedSchema):
            return func_call("is_fixed", [msg, schema.size])

        elif isinstance(schema, EnumSchema):
            symbols = SetLiteral(elts=[Constant(value=x) for x in schema.symbols])
            return func_call("is_enum", [msg, symbols])

        elif isinstance(schema, MapSchema):
            return func_call("is_map", [msg])

        elif isinstance(schema, RecordSchema):
            field_names = SetLiteral(elts=[Constant(value=f.name) for f in schema.fields])
            return func_call("is_record", [msg, field_names])

        raise NotImplementedError(f"have not implemented union check for type {schema}")

    def _gen_logical_encoder(
        self, schema: LogicalSchema, buf: Name, msg: expr
    ) -> List[stmt]:

        if isinstance(schema, DecimalBytesSchema):
            scale_val = 0 if schema.scale is None else schema.scale
            call = func_call(
                "encode_decimal_bytes",
                [msg, schema.precision, scale_val],
            )
        elif isinstance(schema, DecimalFixedSchema):
            scale_val = 0 if schema.scale is None else schema.scale
            call = func_call(
                "encode_decimal_fixed",
                [
                    msg,
                    schema.size,
                    schema.precision,
                    scale_val,
                ],
            )
        elif isinstance(schema, UUIDSchema):
            call = func_call("encode_uuid", [msg])
        elif isinstance(schema, DateSchema):
            call = func_call("encode_date", [msg])
        elif isinstance(schema, TimeMillisSchema):
            call = func_call("encode_time_millis", [msg])
        elif isinstance(schema, TimeMicrosSchema):
            call = func_call("encode_time_micros", [msg])
        elif isinstance(schema, TimestampMillisSchema):
            call = func_call("encode_timestamp_millis", [msg])
        elif isinstance(schema, TimestampMicrosSchema):
            call = func_call("encode_timestamp_micros", [msg])
        else:
            raise LogicalTypeError("unknown logical type")
        return [extend_buffer(buf, call)]

    def _gen_recursive_encode_call(
        self, schema_reference: NamedSchemaReference, buf: Name, msg: expr
    ) -> List[stmt]:
        funcname = self._encoder_name(schema_reference.referenced_schema)
        c = [
            extend_buffer(
                buf,
                Call(
                    func=Name(id=funcname, ctx=Load()),
                    args=[msg],
                    keywords=[],
                ),
            )
        ]
        return c
