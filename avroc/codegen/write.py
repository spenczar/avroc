from typing import List, Dict, Optional, Any

from avroc.avro_common import PRIMITIVES
from avroc.util import clean_name, SchemaType, LogicalTypeError
from avroc.codegen.compiler import Compiler
from avroc.codegen.astutil import (
    call_encoder,
    extend_buffer,
    func_call,
    literal_from_default,
)

from ast import (
    Assign,
    Attribute,
    Call,
    Compare,
    Constant,
    Dict as DictLiteral,
    For,
    FunctionDef,
    Gt,
    If,
    Import,
    ImportFrom,
    Index,
    Load,
    Module,
    Name,
    Raise,
    Return,
    Set,
    Store,
    Subscript,
    Tuple,
    alias,
    arg,
    arguments,
    expr,
    fix_missing_locations,
    stmt,
)

INT_MAX_VALUE = (1 << 31) - 1
INT_MIN_VALUE = -INT_MAX_VALUE
LONG_MAX_VALUE = (1 << 63) - 1
LONG_MIN_VALUE = -LONG_MAX_VALUE


class WriterCompiler(Compiler):
    def __init__(self, schema: SchemaType):
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
                    name=self._encoder_name(recursive_type["name"]),
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
    def _encoder_name(name: str) -> str:
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

    def _gen_encoder(self, schema: SchemaType, buf: Name, msg: expr) -> List[stmt]:
        if isinstance(schema, str):
            if schema in PRIMITIVES:
                return self._gen_primitive_encoder(
                    primitive_type=schema,
                    buf=buf,
                    msg=msg,
                )
            else:
                # Named type reference. Could be recursion?
                if schema in set(t["name"] for t in self.recursive_types):
                    # Yep, recursion. Just generate a function call - we'll have
                    # a separate function to handle this type.
                    return self._gen_recursive_encode_call(schema, buf, msg)
                else:
                    # Not recursion. We can inline the encode, assuming the
                    # schema was already present.
                    referenced_schema = self.named_types.get(schema)
                    if referenced_schema is None:
                        raise ValueError(
                            f"schema {schema} was used before it is defined"
                        )
                    return self._gen_encoder(referenced_schema, buf, msg)

        if isinstance(schema, list):
            return self._gen_union_encoder(options=schema, buf=buf, msg=msg)

        if isinstance(schema, dict):
            if "logicalType" in schema:
                return self._gen_logical_encoder(
                    schema,
                    buf,
                    msg,
                )
            if schema["type"] in PRIMITIVES:
                return self._gen_primitive_encoder(
                    primitive_type=schema["type"],
                    buf=buf,
                    msg=msg,
                )
            if schema["type"] == "record" or schema["type"] == "error":
                return self._gen_record_encoder(schema, buf, msg)
            if schema["type"] == "array":
                return self._gen_array_encoder(schema["items"], buf, msg)
            if schema["type"] == "map":
                return self._gen_map_encoder(schema["values"], buf, msg)
            if schema["type"] == "fixed":
                return self._gen_fixed_encoder(schema["size"], buf, msg)
            if schema["type"] == "enum":
                return self._gen_enum_encoder(
                    schema["symbols"], schema.get("default"), buf, msg
                )

        raise NotImplementedError(f"Schema type not implemented: {schema}")

    def _gen_primitive_encoder(
        self, primitive_type: str, buf: Name, msg: expr
    ) -> List[stmt]:
        if primitive_type == "null":
            return []
        value = call_encoder(primitive_type, msg)
        write = extend_buffer(buf, value)
        return [write]

    def _gen_record_encoder(self, schema: Dict, buf: Name, msg: expr) -> List[stmt]:
        # A record is encoded as a concatenation of its fields.
        statements: List[stmt] = []

        for field in schema["fields"]:
            if "default" in field:
                # Explicit default: generate code like
                #   msg.get(field["name"], field["default"])

                # We need to know the schema of the field in order to know how
                # to construct a literal value for the field default.
                field_schema = field["type"]
                if isinstance(field_schema, str) and field_schema in self.named_types:
                    field_schema = self.named_types[field_schema]

                field_value = Call(
                    func=Attribute(
                        value=msg,
                        attr="get",
                        ctx=Load(),
                    ),
                    args=[
                        Constant(value=field["name"]),
                        literal_from_default(field["default"], field_schema),
                    ],
                    keywords=[],
                )
            elif isinstance(field["type"], list) and "null" in field["type"]:
                # Nullable union: generate code like
                #   msg.get(field["name"])
                field_value = Call(
                    func=Attribute(
                        value=msg,
                        attr="get",
                        ctx=Load(),
                    ),
                    args=[
                        Constant(value=field["name"]),
                    ],
                    keywords=[],
                )
            else:
                # No default: generate code like
                #   msg[field["name"]]
                field_value = Subscript(
                    value=msg,
                    slice=Index(value=Constant(value=field["name"])),
                    ctx=Load(),
                )
            statements.extend(self._gen_encoder(field["type"], buf, field_value))

        return statements

    def _gen_array_encoder(
        self, item_schema: SchemaType, buf: Name, msg: expr
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
            body=self._gen_encoder(item_schema, buf, Name(id=item_varname, ctx=Load())),
            orelse=[],
        )
        if_stmt.body.append(write_loop)
        statements.append(if_stmt)
        # buf += encode_long(0)
        statements.append(extend_buffer(buf, call_encoder("long", 0)))
        return statements

    def _gen_map_encoder(
        self, value_schema: SchemaType, buf: Name, msg: expr
    ) -> List[stmt]:
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
                value_schema,
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

    def _gen_enum_encoder(
        self, symbols: List[str], default: Optional[str], buf: Name, msg: expr
    ) -> List[stmt]:
        # Construct a literal dictionary which maps symbols to integers.
        enum_map = DictLiteral(keys=[], values=[])
        for i, sym in enumerate(symbols):
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
        if default is not None:
            dict_lookup.args.append(Constant(value=default))
        long_encode = call_encoder("long", dict_lookup)
        return [extend_buffer(buf, long_encode)]

    def _gen_union_encoder(
        self, options: List[SchemaType], buf: Name, msg: expr
    ) -> List[stmt]:
        statements: List[stmt] = []

        idx = 0
        prev_if = None

        for idx, option_schema in enumerate(options):
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

    def _gen_union_type_test(self, schema: SchemaType, msg: expr) -> expr:
        if isinstance(schema, str):
            if schema == "null":
                return func_call("is_null", [msg])
            elif schema == "boolean":
                return func_call("is_boolean", [msg])
            elif schema == "string":
                return func_call("is_string", [msg])
            elif schema == "bytes":
                return func_call("is_bytes", [msg])
            elif schema == "int":
                return func_call("is_int", [msg])
            elif schema == "long":
                return func_call("is_long", [msg])
            elif schema == "float":
                return func_call("is_float", [msg])
            elif schema == "double":
                return func_call("is_double", [msg])
            else:
                # Named type reference. Look it up.
                referenced_schema = self.named_types[schema]
                return self._gen_union_type_test(referenced_schema, msg)

        # Union-of-union is explicitly forbidden by the Avro spec, so all
        # thats left is dict types.
        assert not isinstance(schema, list), "Union-of-union is forbidden by Avro spec"
        assert isinstance(schema, dict)

        if "logicalType" in schema:
            lt = schema["logicalType"]
            if lt == "decimal":
                return func_call("is_decimal", [msg])
            elif lt == "uuid":
                return func_call("is_uuid", [msg])
            elif lt == "date":
                return func_call("is_date", [msg])
            elif lt == "time_millis" or lt == "time_micros":
                return func_call("is_time", [msg])
            elif lt == "timestamp_millis" or lt == "timestamp_micros":
                return func_call("is_timestamp", [msg])

        if schema["type"] in PRIMITIVES:
            return self._gen_union_type_test(schema["type"], msg)

        if schema["type"] == "array":
            return func_call("is_array", [msg])

        if schema["type"] == "fixed":
            return func_call("is_fixed", [msg, schema["size"]])

        if schema["type"] == "enum":
            symbols = Set(elts=[Constant(value=x) for x in schema["symbols"]])
            return func_call("is_enum", [msg, symbols])

        if schema["type"] == "map":
            return func_call("is_map", [msg])

        if schema["type"] == "record" or schema["type"] == "error":
            field_names = Set(
                elts=[Constant(value=f["name"]) for f in schema["fields"]]
            )
            c = func_call("is_record", [msg, field_names])
            return c

        raise NotImplementedError(f"have not implemented union check for type {schema}")

    def _gen_logical_encoder(
        self, schema: Dict[str, Any], buf: Name, msg: expr
    ) -> List[stmt]:
        try:
            lt = schema["logicalType"]
            t = schema["type"]
            call = None
            if lt == "decimal":
                if t == "bytes":
                    call = func_call(
                        "encode_decimal_bytes",
                        [msg, schema["precision"], schema.get("scale", 0)],
                    )
                elif t == "fixed":
                    call = func_call(
                        "encode_decimal_fixed",
                        [
                            msg,
                            schema["size"],
                            schema["precision"],
                            schema.get("scale", 0),
                        ],
                    )
            elif lt == "uuid" and t == "string":
                call = func_call("encode_uuid", [msg])
            elif lt == "date" and t == "int":
                call = func_call("encode_date", [msg])
            elif lt == "time-millis" and t == "int":
                call = func_call("encode_time_millis", [msg])
            elif lt == "time-micros" and t == "long":
                call = func_call("encode_time_micros", [msg])
            elif lt == "timestamp-millis" and t == "long":
                call = func_call("encode_timestamp_millis", [msg])
            elif lt == "timestamp-micros" and t == "long":
                call = func_call("encode_timestamp_micros", [msg])

            if call is None:
                raise LogicalTypeError("unknown logical type")

            return [extend_buffer(buf, call)]

        except LogicalTypeError:
            # If a logical type is unknown, or invalid, then we should fall back
            # and use the underlying Avro type. We do this by clearing the
            # logicalType field of the schema and calling self._gen_encoder.
            schema = schema.copy()
            del schema["logicalType"]
            return self._gen_encoder(schema, buf, msg)

    def _gen_recursive_encode_call(
        self, recursive_type_name: str, buf: Name, msg: expr
    ) -> List[stmt]:
        funcname = self._encoder_name(recursive_type_name)
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
