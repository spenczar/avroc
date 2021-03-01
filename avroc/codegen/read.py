from typing import Dict, Any, List, IO, Optional, Iterator

import datetime

from fastavro.read import block_reader
from avroc.avro_common import PRIMITIVES, LOGICALS
from avroc.util import SchemaType, clean_name, LogicalTypeError
from avroc.codegen.compiler import Compiler

from ast import (
    Add,
    AST,
    Assign,
    Attribute,
    BinOp,
    Call,
    Compare,
    Constant,
    Dict as DictLiteral,
    Eq,
    Expr,
    For,
    FloorDiv,
    FunctionDef,
    If,
    IfExp,
    Import,
    ImportFrom,
    Index,
    List as ListLiteral,
    Load,
    Lt,
    Mod,
    Module,
    Mult,
    Name,
    NotEq,
    Return,
    Store,
    Subscript,
    USub,
    UnaryOp,
    While,
    alias,
    arg,
    arguments,
    fix_missing_locations,
    keyword,
    stmt,
)


def read_file(fo: IO[bytes]) -> Iterator[Any]:
    """
    Open an Avro Container Format file. Read its header to find the schema,
    compile the schema, and use it to deserialize records, yielding them out.
    """
    blocks = block_reader(fo, reader_schema=None, return_record_name=False)
    if blocks.writer_schema is None:
        raise ValueError("missing write schema")
    compiler = ReaderCompiler(blocks.writer_schema)
    reader = compiler.compile()

    for block in blocks:
        for _ in range(block.num_records):
            yield reader(block.bytes_)


class ReaderCompiler(Compiler):
    def __init__(self, schema: SchemaType):
        super(ReaderCompiler, self).__init__(schema, "decoder")

    def generate_module(self) -> Module:
        body: List[stmt] = [
            Import(names=[alias(name="datetime")]),
            Import(names=[alias(name="decimal")]),
            Import(names=[alias(name="uuid")]),
        ]

        # Add import statements of low-level decode functions
        import_from_encoding = []
        for primitive_type in PRIMITIVES:
            name = "decode_" + primitive_type
            import_from_encoding.append(alias(name=name))

        body.append(
            ImportFrom(
                module="avroc.runtime.encoding",
                names=import_from_encoding,
                level=0,
            )
        )
        body.append(self.generate_decoder_func(self.schema, self.entrypoint_name))

        # Identify recursively-defined schemas. For each one, create a named
        # decoder function.
        for recursive_type in self.recursive_types:
            body.append(
                self.generate_decoder_func(
                    name=self._decoder_name(recursive_type["name"]),
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
    def _decoder_name(name: str) -> str:
        return "_decode_" + clean_name(name)

    def generate_decoder_func(self, schema: SchemaType, name: str) -> FunctionDef:
        """
        Returns an AST describing a function which can decode an Avro message from a
        IO[bytes] source. The message is parsed according to the given schema.
        """
        src_var = Name(id="src", ctx=Load())
        result_var = Name(id=self.new_variable("result"), ctx=Store())
        func = FunctionDef(
            name=name,
            args=arguments(
                args=[arg(arg="src")],
                posonlyargs=[],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=[],
            decorator_list=[],
        )

        func.body.extend(self._gen_decode(schema, src_var, result_var))
        func.body.append(Return(value=Name(id=result_var.id, ctx=Load())))
        return func

    def _gen_decode(self, schema: SchemaType, src: Name, dest: AST) -> List[stmt]:
        """
        Returns a sequence of statements which will read data from src and write
        the deserialized value into dest.
        """
        if isinstance(schema, str):
            if schema in PRIMITIVES:
                return self._gen_primitive_decode(
                    primitive_type=schema, src=src, dest=dest
                )
            else:
                # Named type reference. Could be recursion?
                if schema in set(t["name"] for t in self.recursive_types):
                    # Yep, recursion. Just generate a function call - we'll have
                    # a separate function to handle this type.
                    return self._gen_recursive_decode_call(schema, src, dest)
        if isinstance(schema, list):
            return self._gen_union_decode(
                options=schema,
                src=src,
                dest=dest,
            )
        if isinstance(schema, dict):
            if "logicalType" in schema:
                return self._gen_logical_decode(
                    schema=schema,
                    src=src,
                    dest=dest,
                )
            schema_type = schema["type"]
            if schema_type in PRIMITIVES:
                return self._gen_primitive_decode(
                    primitive_type=schema_type,
                    src=src,
                    dest=dest,
                )
            if schema_type == "record":
                return self._gen_record_decode(
                    schema=schema,
                    src=src,
                    dest=dest,
                )
            if schema_type == "array":
                return self._gen_array_decode(
                    item_schema=schema["items"],
                    src=src,
                    dest=dest,
                )
            if schema_type == "map":
                return self._gen_map_decode(
                    value_schema=schema["values"],
                    src=src,
                    dest=dest,
                )
            if schema_type == "fixed":
                return self._gen_fixed_decode(
                    size=schema["size"],
                    src=src,
                    dest=dest,
                )
            if schema_type == "enum":
                return self._gen_enum_decode(
                    symbols=schema["symbols"],
                    default=schema.get("default"),
                    src=src,
                    dest=dest,
                )

        raise NotImplementedError(f"Schema type not implemented: {schema}")

    def _gen_union_decode(
        self, options: List[SchemaType], src: Name, dest: AST
    ) -> List[stmt]:

        # Special case: fields like '["null", "long"] which represent an
        # optional field.
        if len(options) == 2:
            if options[0] == "null":
                return self._gen_optional_decode(1, options[1], src, dest)
            if options[1] == "null":
                return self._gen_optional_decode(0, options[0], src, dest)

        statements: List[stmt] = []
        # Read a long to figure out which option in the union is chosen.
        idx_var = self.new_variable("union_choice")
        idx_var_dest = Name(id=idx_var, ctx=Store())
        statements.extend(self._gen_primitive_decode("long", src, idx_var_dest))

        idx_var_ref = Name(id=idx_var, ctx=Load())
        prev_if = None
        for idx, option in enumerate(options):
            if_idx_matches = Compare(
                left=idx_var_ref, ops=[Eq()], comparators=[Constant(idx)]
            )
            if_stmt = If(
                test=if_idx_matches,
                body=self._gen_decode(option, src, dest),
                orelse=[],
            )

            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt
        return statements

    def _gen_optional_decode(
        self, idx: int, schema: SchemaType, src: Name, dest: AST
    ) -> List[stmt]:
        statements: List[stmt] = []
        is_populated = Compare(
            left=Call(func=Name(id="decode_long", ctx=Load()), args=[src], keywords=[]),
            ops=[Eq()],
            comparators=[Constant(idx)],
        )

        if isinstance(schema, str) and schema in PRIMITIVES:
            # We can read the value in one line, so we can do something like:
            #  v1["optional_long"] = decode_long(src) if idx == 1 else None

            if_expr = IfExp(
                test=is_populated,
                body=Call(
                    func=Name(id="decode_" + schema, ctx=Load()),
                    args=[src],
                    keywords=[],
                ),
                orelse=Constant(None),
            )
            assignment = Assign(
                targets=[dest],
                value=if_expr,
            )
            statements.append(assignment)
        else:
            # It takes more than one line to read the value, so we need a real if block.
            if_stmt = If(
                test=is_populated,
                body=self._gen_decode(schema, src, dest),
                orelse=[Assign(targets=[dest], value=Constant(None))],
            )
            statements.append(if_stmt)
        return statements

    def _gen_record_decode(self, schema: Dict, src: Name, dest: AST) -> List[stmt]:
        statements: List[stmt] = []

        # Construct a new empty dictionary to hold the record contents.
        value_name = self.new_variable(clean_name(schema["name"]))
        empty_dict = DictLiteral(keys=[], values=[])
        statements.append(
            Assign(
                targets=[Name(id=value_name, ctx=Store())],
                value=empty_dict,
                lineno=0,
            ),
        )
        value_reference = Name(id=value_name, ctx=Load())

        # Write statements to populate all the fields of the record.
        for field in schema["fields"]:
            # Make an AST node that references an entry in the record dict,
            # using the field name as a key.
            field_dest = Subscript(
                value=value_reference,
                slice=Index(value=Constant(value=field["name"])),
                ctx=Store(),
            )

            # Generate the statements required to read that field's type, and to
            # store it into field_dest.
            read_statements = self._gen_decode(field["type"], src, field_dest)
            statements.extend(read_statements)

        # Now that we have a fully constructed record, write it into the
        # destination provided.
        statements.append(
            Assign(
                targets=[dest],
                value=value_reference,
                lineno=0,
            )
        )
        return statements

    def _gen_array_decode(
        self, item_schema: SchemaType, src: Name, dest: AST
    ) -> List[stmt]:
        """
        Returns a sequence of statements which will deserialize an array of given
        type from src into dest.
        """
        statements: List[stmt] = []

        # Create a new list to hold the values we'll read.
        name = "array_"
        if isinstance(item_schema, dict):
            if "name" in item_schema:
                name += item_schema["name"]
            elif "type" in item_schema and isinstance(item_schema["type"], str):
                name += item_schema["type"]
        elif isinstance(item_schema, str):
            name += item_schema
        name = clean_name(name)

        list_varname = self.new_variable(name)

        assign_stmt = Assign(
            targets=[Name(id=list_varname, ctx=Store())],
            value=ListLiteral(elts=[], ctx=Load()),
        )
        statements.append(assign_stmt)

        # For each message in the array...
        for_each_message: List[stmt] = []

        # ... read a value...
        value_varname = self.new_variable("array_val")
        value_dest = Name(id=value_varname, ctx=Store())
        read_statements = self._gen_decode(item_schema, src, value_dest)
        for_each_message.extend(read_statements)

        # ... and append it to the list.
        list_append_method = Attribute(
            value=Name(id=list_varname, ctx=Load()),
            attr="append",
            ctx=Load(),
        )
        list_append_method_call = Expr(
            Call(
                func=list_append_method,
                args=[Name(id=value_varname, ctx=Load())],
                keywords=[],
            )
        )
        for_each_message.append(list_append_method_call)

        statements.extend(self._gen_block_decode(for_each_message, src))

        # Finally, assign the list we have constructed into the destination AST node.
        assign_result = Assign(
            targets=[dest],
            value=Name(id=list_varname, ctx=Load()),
        )
        statements.append(assign_result)
        return statements

    def _gen_map_decode(
        self, value_schema: SchemaType, src: Name, dest: AST
    ) -> List[stmt]:
        """
        Returns a sequence of statements which will deserialize a map with given
        value type from src into dest.
        """
        statements: List[stmt] = []

        name = "map_"
        if isinstance(value_schema, dict):
            if "name" in value_schema:
                name += value_schema["name"]
            elif "type" in value_schema and isinstance(value_schema["type"], str):
                name += value_schema["type"]
        elif isinstance(value_schema, str):
            name += value_schema
        name = clean_name(name)

        map_varname = self.new_variable(name)
        assign_stmt = Assign(
            targets=[Name(id=map_varname, ctx=Store())],
            value=DictLiteral(keys=[], values=[]),
        )
        statements.append(assign_stmt)

        # For each message in a block...
        for_each_message = []

        # ... read a string key...
        key_varname = self.new_variable("key")
        key_dest = Name(id=key_varname, ctx=Store())
        for_each_message.extend(self._gen_primitive_decode("string", src, key_dest))
        # ... and read the corresponding value.
        value_dest = Subscript(
            value=Name(id=map_varname, ctx=Load()),
            slice=Index(Name(id=key_varname, ctx=Load())),
            ctx=Store(),
        )
        for_each_message.extend(self._gen_decode(value_schema, src, value_dest))

        statements.extend(self._gen_block_decode(for_each_message, src))

        # Finally, assign our resulting map to the destination target.
        statements.append(
            Assign(
                targets=[dest],
                value=Name(id=map_varname, ctx=Load()),
            )
        )
        return statements

    def _gen_block_decode(self, for_each_message: List[stmt], src: Name) -> List[stmt]:
        """
        Returns a series of statements which represent iteration over an Avro record
        block, like are used for arrays and maps.

        Blocks are a series of records. The block is prefixed with a long that
        indicates the number of records in the block. A zero-length block
        indicates the end of the array or map.

        If a block's count is negative, its absolute value is used, and the
        count is followed immediately by a long block size indicating the number
        of bytes in the block

        for_each_message is a series of statements that will be injected and
        called for every message in the block.
        """
        statements: List[stmt] = []

        # Read the blocksize to figure out how many messages to read.
        blocksize_varname = self.new_variable("blocksize")
        blocksize_dest = Name(id=blocksize_varname, ctx=Store())
        statements.extend(self._gen_primitive_decode("long", src, blocksize_dest))

        # For each nonzero-sized block...
        while_loop = While(
            test=Compare(
                left=Name(id=blocksize_varname, ctx=Load()),
                ops=[NotEq()],
                comparators=[Constant(value=0)],
            ),
            body=[],
            orelse=[],
        )

        # ... handle negative block sizes...
        if_negative_blocksize = If(
            test=Compare(
                left=Name(id=blocksize_varname, ctx=Load()),
                ops=[Lt()],
                comparators=[Constant(value=0)],
            ),
            body=[],
            orelse=[],
        )
        flip_blocksize_sign = Assign(
            targets=[Name(id=blocksize_varname, ctx=Store())],
            value=UnaryOp(op=USub(), operand=Name(id=blocksize_varname, ctx=Load())),
        )
        if_negative_blocksize.body.append(flip_blocksize_sign)
        # Just discard the byte size of the block.
        read_a_long = Expr(
            Call(func=Name(id="decode_long", ctx=Load()), args=[src], keywords=[])
        )
        if_negative_blocksize.body.append(read_a_long)
        while_loop.body.append(if_negative_blocksize)

        # Do a 'for _ in range(blocksize)' loop
        read_loop = For(
            target=Name(id="_", ctx=Store()),
            iter=Call(
                func=Name(id="range", ctx=Load()),
                args=[Name(id=blocksize_varname, ctx=Load())],
                keywords=[],
            ),
            body=for_each_message,
            orelse=[],
        )

        while_loop.body.append(read_loop)

        # If we've finished the block, read another long into blocksize.
        #
        # If it's zero, then we're done reading the array, and the loop test
        # will exit.
        #
        # If it's nonzero, then there are more messages to go.
        while_loop.body.extend(self._gen_primitive_decode("long", src, blocksize_dest))

        statements.append(while_loop)
        return statements

    def _gen_enum_decode(
        self, symbols: List[str], default: Optional[str], src: Name, dest: AST
    ) -> List[stmt]:
        statements: List[stmt] = []

        # Construct a literal dictionary which maps integers to symbols.
        enum_map = DictLiteral(keys=[], values=[])
        for i, sym in enumerate(symbols):
            enum_map.keys.append(Constant(value=i))
            enum_map.values.append(Constant(value=sym))

        # Call dict.get(decode_long(src), default=default)
        call = Call(
            func=Attribute(
                value=enum_map,
                attr="get",
                ctx=Load(),
            ),
            args=[
                Call(
                    func=Name(id="decode_long", ctx=Load()),
                    args=[src],
                    keywords=[],
                )
            ],
            keywords=[],
        )

        if default is not None:
            call.args.append(Constant(value=default))

        statements.append(
            Assign(
                targets=[dest],
                value=call,
            )
        )
        return statements

    def _gen_fixed_decode(self, size: int, src: Name, dest: AST) -> List[stmt]:
        # Call dest = src.read(size).
        read = Call(
            func=Attribute(value=src, attr="read", ctx=Load()),
            args=[Constant(value=size)],
            keywords=[],
        )
        return [
            Assign(
                targets=[dest],
                value=read,
            )
        ]

    def _gen_primitive_decode(
        self, primitive_type: str, src: Name, dest: AST
    ) -> List[stmt]:
        """
        Returns a sequence of statements which will deserialize a given primitive
        type from src into dest.
        """
        if primitive_type == "null":
            statement = Assign(
                targets=[dest],
                value=Constant(value=None),
            )
            return [statement]

        decode_func_name = "decode_" + primitive_type
        value = Call(
            func=Name(id=decode_func_name, ctx=Load()),
            args=[src],
            keywords=[],
        )
        statement = Assign(
            targets=[dest],
            value=value,
        )
        return [statement]

    def _gen_logical_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        try:
            lt = schema["logicalType"]
            if lt == "decimal":
                return self._gen_decimal_decode(schema, src, dest)
            if lt == "uuid":
                return self._gen_uuid_decode(schema, src, dest)
            if lt == "date":
                return self._gen_date_decode(schema, src, dest)
            if lt == "time-millis":
                return self._gen_time_millis_decode(schema, src, dest)
            if lt == "time-micros":
                return self._gen_time_micros_decode(schema, src, dest)
            if lt == "timestamp-millis":
                return self._gen_timestamp_millis_decode(schema, src, dest)
            if lt == "timestamp-micros":
                return self._gen_timestamp_micros_decode(schema, src, dest)
            raise LogicalTypeError("unknown logical type")
        except LogicalTypeError:
            # If a logical type is unknown, or invalid, then we should fall back
            # and use the underlying Avro type. We do this by clearing the
            # logicalType field of the schema and calling self._gen_decode.
            schema = schema.copy()
            del schema["logicalType"]
            return self._gen_decode(schema, src, dest)

    def _gen_decimal_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        scale = schema.get("scale", 0)
        precision = schema.get("precision", 0)
        if precision <= 0 or scale < 0 or scale > precision:
            raise LogicalTypeError("invalid decimal")

        statements: List[stmt] = []

        # Read the raw bytes. They can be either 'fixed' or 'bytes'
        raw_bytes_varname = self.new_variable("raw_decimal")
        raw_bytes_dest = Name(id=raw_bytes_varname, ctx=Store())
        if schema["type"] == "bytes":
            statements.extend(self._gen_primitive_decode("bytes", src, raw_bytes_dest))
        elif schema["type"] == "fixed":
            size: int = schema["size"]
            statements.extend(self._gen_fixed_decode(size, src, raw_bytes_dest))
        else:
            raise LogicalTypeError("unexpected type for decimal")

        # Interpret the bytes as an unscaled integer
        raw_int_varname = self.new_variable("raw_int")
        raw_int_dest = Name(id=raw_int_varname, ctx=Store())
        statements.append(
            Assign(
                targets=[raw_int_dest],
                value=Call(
                    func=Attribute(
                        value=Name(id="int", ctx=Load()),
                        attr="from_bytes",
                        ctx=Load(),
                    ),
                    args=[Name(id=raw_bytes_varname, ctx=Load())],
                    keywords=[keyword(arg="byteorder", value=Constant(value="big"))],
                ),
            )
        )

        # Scale the integer up based on the schema's scale and precision.
        #
        # First, create a new decimal context, like
        #   decimal_context = decimal.Context(prec={precision})
        decimal_ctx_varname = self.new_variable("decimal_context")
        statements.append(
            Assign(
                targets=[Name(id=decimal_ctx_varname, ctx=Store())],
                value=Call(
                    func=Attribute(
                        value=Name(id="decimal", ctx=Load()), attr="Context", ctx=Load()
                    ),
                    args=[],
                    keywords=[keyword(arg="prec", value=Constant(value=precision))],
                ),
            )
        )
        # Then, use the context to interpret the unscaled integer and scale it
        # up, like decimal_context.create_decimal(raw_int).scaleb(-{scale}, decimal_context)
        create_decimal_call = Call(
            func=Attribute(
                value=Name(id="decimal_context", ctx=Load()),
                attr="create_decimal",
                ctx=Load(),
            ),
            args=[Name(id=raw_int_varname, ctx=Load())],
            keywords=[],
        )
        scaleb_call = Call(
            func=Attribute(value=create_decimal_call, attr="scaleb", ctx=Load()),
            args=[
                UnaryOp(
                    op=USub(),
                    operand=Constant(value=scale),
                ),
            ],
            keywords=[],
        )
        statements.append(Assign(targets=[dest], value=scaleb_call))
        return statements

    def _gen_uuid_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        if schema["type"] != "string":
            raise LogicalTypeError("unexpected type for uuid")
        # Call uuid.UUID(decode_string(src)).
        decode_call = Call(
            func=Name(id="decode_string", ctx=Load()),
            args=[src],
            keywords=[],
        )
        uuid_constructor = Attribute(
            value=Name(id="uuid", ctx=Load()),
            attr="UUID",
            ctx=Load(),
        )
        uuid_constructor_call = Call(
            func=uuid_constructor,
            args=[decode_call],
            keywords=[],
        )
        assignment = Assign(
            targets=[dest],
            value=uuid_constructor_call,
        )
        return [assignment]

    def _gen_date_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        if schema["type"] != "int":
            raise LogicalTypeError("unexpected type for date")
        unix_epoch_day_zero = datetime.date(1970, 1, 1).toordinal()
        # Call datetime.date.fromordinal(read_int(src) + {unix_epoch_day_zero})
        decode_call = Call(
            func=Name(id="decode_int", ctx=Load()),
            args=[src],
            keywords=[],
        )
        sum_call = BinOp(
            left=decode_call,
            right=Constant(value=unix_epoch_day_zero),
            op=Add(),
        )
        date_constructor = Attribute(
            value=Attribute(
                value=Name(id="datetime", ctx=Load()),
                attr="date",
                ctx=Load(),
            ),
            attr="fromordinal",
            ctx=Load(),
        )
        date_constructor_call = Call(
            func=date_constructor,
            args=[sum_call],
            keywords=[],
        )
        assignment = Assign(
            targets=[dest],
            value=date_constructor_call,
        )
        return [assignment]

    def _gen_time_millis_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        if schema["type"] != "int":
            raise LogicalTypeError("unexpected type for time-millis")
        # Decode an integer, then call
        # datetime.time(
        #    hour=int_val // 3600000,
        #    minute=(int_val // 60000) % 60,
        #    second=(int_val // 1000) % 60,
        #    microsecond=(int_val % 1000) * 1000,
        # )
        statements: List[stmt] = []
        raw_int_varname = self.new_variable("raw_time_millis")
        raw_int_dest = Name(id=raw_int_varname, ctx=Store())
        statements.extend(self._gen_primitive_decode("int", src, raw_int_dest))
        raw_int = Name(id=raw_int_varname, ctx=Load())
        hours = BinOp(
            op=FloorDiv(),
            left=raw_int,
            right=Constant(value=1000 * 60 * 60),
        )
        minutes = BinOp(
            op=Mod(),
            left=BinOp(
                op=FloorDiv(),
                left=raw_int,
                right=Constant(value=1000 * 60),
            ),
            right=Constant(value=60),
        )
        seconds = BinOp(
            op=Mod(),
            left=BinOp(
                op=FloorDiv(),
                left=raw_int,
                right=Constant(value=1000),
            ),
            right=Constant(value=60),
        )
        microseconds = BinOp(
            op=Mult(),
            left=BinOp(
                op=Mod(),
                left=raw_int,
                right=Constant(value=1000),
            ),
            right=Constant(value=1000),
        )
        time_constructor = Attribute(
            value=Name(id="datetime"),
            attr="time",
            ctx=Load(),
        )
        time_constructor_call = Call(
            func=time_constructor,
            args=[hours, minutes, seconds, microseconds],
            keywords=[],
        )
        statements.append(
            Assign(
                targets=[dest],
                value=time_constructor_call,
            )
        )
        return statements

    def _gen_time_micros_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        if schema["type"] != "long":
            raise LogicalTypeError("unexpected type for time-micros")
        # Decode an integer, then call
        # datetime.time(
        #    hour=int_val // 3600000000,
        #    minute=(int_val // 60000000) % 60,
        #    second=(int_val // 1000000) % 60,
        #    microsecond=(int_val % 1000000),
        # )
        statements: List[stmt] = []
        raw_int_varname = self.new_variable("raw_time_micros")
        raw_int_dest = Name(id=raw_int_varname, ctx=Store())
        statements.extend(self._gen_primitive_decode("int", src, raw_int_dest))
        raw_int = Name(id=raw_int_varname, ctx=Load())
        hours = BinOp(
            op=FloorDiv(),
            left=raw_int,
            right=Constant(value=1000000 * 60 * 60),
        )
        minutes = BinOp(
            op=Mod(),
            left=BinOp(
                op=FloorDiv(),
                left=raw_int,
                right=Constant(value=1000000 * 60),
            ),
            right=Constant(value=60),
        )
        seconds = BinOp(
            op=Mod(),
            left=BinOp(
                op=FloorDiv(),
                left=raw_int,
                right=Constant(value=1000000),
            ),
            right=Constant(value=60),
        )
        microseconds = BinOp(
            op=Mod(),
            left=raw_int,
            right=Constant(value=1000000),
        )
        time_constructor = Attribute(
            value=Name(id="datetime"),
            attr="time",
            ctx=Load(),
        )
        time_constructor_call = Call(
            func=time_constructor,
            args=[hours, minutes, seconds, microseconds],
            keywords=[],
        )
        statements.append(
            Assign(
                targets=[dest],
                value=time_constructor_call,
            )
        )
        return statements

    def _gen_timestamp_millis_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        if schema["type"] != "long":
            raise LogicalTypeError("unexpected type for timestamp-millis")
        # Return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=decode_long(src) * 1000)
        decode_call = Call(
            func=Name(id="decode_long", ctx=Load()), args=[src], keywords=[]
        )
        scaled_up = BinOp(
            op=Mult(),
            left=decode_call,
            right=Constant(value=1000),
        )
        timedelta_constructor = Attribute(
            value=Name(id="datetime", ctx=Load()), attr="timedelta", ctx=Load()
        )
        timedelta_constructor_call = Call(
            func=timedelta_constructor,
            args=[],
            keywords=[keyword(arg="microseconds", value=scaled_up)],
        )

        epoch_start = Call(
            func=Attribute(
                value=Name(id="datetime", ctx=Load()),
                attr="datetime",
                ctx=Load(),
            ),
            args=[
                Constant(value=1970),
                Constant(value=1),
                Constant(value=1),
            ],
            keywords=[
                keyword(
                    arg="tzinfo",
                    value=Attribute(
                        value=Attribute(
                            value=Name(id="datetime", ctx=Load()),
                            attr="timezone",
                            ctx=Load(),
                        ),
                        attr="utc",
                        ctx=Load(),
                    ),
                ),
            ],
        )

        sum_op = BinOp(
            op=Add(),
            left=epoch_start,
            right=timedelta_constructor_call,
        )
        return [
            Assign(
                targets=[dest],
                value=sum_op,
            )
        ]

    def _gen_timestamp_micros_decode(
        self, schema: Dict[str, Any], src: Name, dest: AST
    ) -> List[stmt]:
        if schema["type"] != "long":
            raise LogicalTypeError("unexpected type for timestamp-micros")
        # Return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=decode_long(src))
        decode_call = Call(
            func=Name(id="decode_long", ctx=Load()), args=[src], keywords=[]
        )
        timedelta_constructor = Attribute(
            value=Name(id="datetime", ctx=Load()), attr="timedelta", ctx=Load()
        )
        timedelta_constructor_call = Call(
            func=timedelta_constructor,
            args=[],
            keywords=[keyword(arg="microseconds", value=decode_call)],
        )

        epoch_start = Call(
            func=Attribute(
                value=Name(id="datetime", ctx=Load()),
                attr="datetime",
                ctx=Load(),
            ),
            args=[
                Constant(value=1970),
                Constant(value=1),
                Constant(value=1),
            ],
            keywords=[
                keyword(
                    arg="tzinfo",
                    value=Attribute(
                        value=Attribute(
                            value=Name(id="datetime", ctx=Load()),
                            attr="timezone",
                            ctx=Load(),
                        ),
                        attr="utc",
                        ctx=Load(),
                    ),
                ),
            ],
        )

        sum_op = BinOp(
            op=Add(),
            left=epoch_start,
            right=timedelta_constructor_call,
        )
        return [
            Assign(
                targets=[dest],
                value=sum_op,
            )
        ]

    def _gen_recursive_decode_call(
        self, recursive_type_name: str, src: Name, dest: AST
    ) -> List[stmt]:
        funcname = self._decoder_name(recursive_type_name)
        return [
            Assign(
                targets=[dest],
                value=Call(
                    func=Name(id=funcname, ctx=Load()),
                    args=[src],
                    keywords=[],
                ),
            )
        ]

    def _call_logical_decode(
        self, primitive_type: str, parser: str, src: Name, dest: AST
    ) -> List[stmt]:
        """
        Read a value of primitive type from src, and then call parser on it,
        assigning into dest.
        """
        statements: List[stmt] = []
        # Read the raw value.
        raw_varname = self.new_variable("raw_" + primitive_type)
        raw_dest = Name(id=raw_varname, ctx=Store())
        statements.extend(self._gen_primitive_decode(primitive_type, src, raw_dest))

        # Call the fastavro parser for the logical type.
        parse = Call(
            func=Name(id=parser, ctx=Load()),
            args=[Name(id=raw_varname, ctx=Load())],
            keywords=[],
        )
        statements.append(Assign(targets=[dest], value=parse))
        return statements
